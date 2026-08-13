// Package k8s is DART's Kubernetes-native discovery plugin: a cluster.Seeder
// backed by an EndpointSlice watch, registered with node.Run as the "k8s"
// discovery scheme:
//
//	dart-k8s -discover=k8s:<namespace>/<service>[/<port>] ...
//
// Why this exists alongside the DNS scheme: a headless Service's DNS answer
// lags endpoint changes by the resolver TTL and truncates past a response size
// limit, while an EndpointSlice watch delivers endpoint readiness changes as
// they happen and reads the endpoints' ready condition directly. Everything
// else about membership — turning addresses into stable identities, liveness,
// the forget-after grace — is unchanged and still lives in the cluster package:
// a Seeder answers only "who might be out there".
//
// The package lives in its own module so that the client-go dependency tree it
// pulls in never touches the main DART module (see go.mod).
package k8s

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/node"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// DefaultPortName selects which port of an EndpointSlice is the DART peer port
// when the spec does not name one. Deployments should name their peer Service
// port accordingly (see deploy/k8s/).
const DefaultPortName = "peer"

// Scheme is the discovery scheme binaries register to accept
// -discover=k8s:<namespace>/<service>[/<port>]. The optional port segment is a
// port name ("peer") or a number ("9000"); absent, the port named
// DefaultPortName is used.
var Scheme = node.DiscoveryScheme{
	Name:  "k8s",
	Usage: "k8s:<namespace>/<service>[/<port>] (an EndpointSlice watch)",
	New:   NewSeeder,
}

// NewSeeder builds a Seeder from the scheme spec, using the in-cluster config
// and falling back to the default kubeconfig loading rules (so the same binary
// runs against a cluster from a developer machine).
func NewSeeder(spec string) (cluster.Seeder, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("k8s seeder: no in-cluster config and no kubeconfig: %w", err)
		}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s seeder: %w", err)
	}
	return NewSeederWithClient(client, spec)
}

// portSelector matches an EndpointSlice port either by name or by number.
type portSelector struct {
	name   string // matched against EndpointPort.Name when non-empty
	number int32  // matched against EndpointPort.Port when name is empty
}

// parseSpec splits "<namespace>/<service>[/<port>]"; the port segment is a name
// unless it parses as a port number.
func parseSpec(spec string) (namespace, service string, port portSelector, err error) {
	parts := strings.Split(strings.Trim(spec, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
		return "", "", portSelector{}, fmt.Errorf("k8s seeder spec %q: want <namespace>/<service>[/<port>]", spec)
	}
	port = portSelector{name: DefaultPortName}
	if len(parts) == 3 {
		if n, convErr := strconv.ParseInt(parts[2], 10, 32); convErr == nil {
			if n <= 0 || n > 65535 {
				return "", "", portSelector{}, fmt.Errorf("k8s seeder spec %q: bad port number %q", spec, parts[2])
			}
			port = portSelector{number: int32(n)}
		} else {
			port = portSelector{name: parts[2]}
		}
	}
	return parts[0], parts[1], port, nil
}

// Seeder is a cluster.Seeder fed by an EndpointSlice informer. It reports the
// addresses of the ready endpoints of one Service's slices, on the selected
// port. Run drives the informer; Seeds only reads the cached result, so the
// cluster package's refresh loop never blocks on the API server.
type Seeder struct {
	namespace string
	service   string
	port      portSelector
	factory   informers.SharedInformerFactory
	informer  cache.SharedIndexInformer

	mu     sync.RWMutex // guards addrs
	addrs  []string     // sorted; the last recomputed snapshot
	synced atomic.Bool  // set once the informer cache has synced
}

// NewSeederWithClient builds a Seeder from the scheme spec on an existing
// client, which is also how tests inject the fake clientset.
func NewSeederWithClient(client kubernetes.Interface, spec string) (cluster.Seeder, error) {
	namespace, service, port, err := parseSpec(spec)
	if err != nil {
		return nil, err
	}
	return newSeeder(client, namespace, service, port), nil
}

func newSeeder(client kubernetes.Interface, namespace, service string, port portSelector) *Seeder {
	s := &Seeder{namespace: namespace, service: service, port: port}
	// The slice set of a Service is selected by the well-known label rather than
	// by listing everything in the namespace and filtering client-side.
	s.factory = informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = discoveryv1.LabelServiceName + "=" + service
		}))
	s.informer = s.factory.Discovery().V1().EndpointSlices().Informer()
	// Any change to the slice set — endpoints appearing, going unready, being
	// deleted — recomputes the whole snapshot. EndpointSlice sets are small (one
	// slice per ~100 endpoints), so a full recompute per event is the simple and
	// obviously-correct option.
	if _, err := s.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { s.recompute() },
		UpdateFunc: func(_, _ any) { s.recompute() },
		DeleteFunc: func(any) { s.recompute() },
	}); err != nil {
		// AddEventHandler only errors when the informer is already stopped; that
		// cannot happen here, so this is a programming error.
		panic(fmt.Sprintf("k8s seeder: AddEventHandler: %v", err))
	}
	return s
}

// Run starts the informer and blocks until ctx is cancelled. node.Run invokes
// it via the DiscoveryScheme lifecycle convention.
func (s *Seeder) Run(ctx context.Context) {
	s.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		return // ctx cancelled before the first list completed
	}
	s.recompute()
	s.synced.Store(true)
	<-ctx.Done()
}

// Seeds returns the latest snapshot. Before the first sync — or if the API
// server is briefly unreachable at startup — it reports "nothing found right
// now", which membership handles the same as a DNS miss: keep the known peers.
func (s *Seeder) Seeds(context.Context) ([]string, error) {
	if !s.synced.Load() {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.addrs...), nil
}

// recompute rebuilds the address snapshot from the informer store.
func (s *Seeder) recompute() {
	var addrs []string
	for _, obj := range s.informer.GetStore().List() {
		es, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			continue
		}
		port, ok := s.selectPort(es)
		if !ok {
			continue // this slice does not expose the peer port
		}
		for _, ep := range es.Endpoints {
			// A nil Ready condition means the endpoint is ready (the API omits
			// the default); an explicit false is a not-yet-ready or terminating
			// endpoint that must not be dialed.
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			if len(ep.Addresses) == 0 {
				continue
			}
			addrs = append(addrs, net.JoinHostPort(ep.Addresses[0], strconv.Itoa(int(port))))
		}
	}
	// The snapshot feeds logs and tests; keep it deterministic regardless of
	// informer store order.
	sort.Strings(addrs)
	s.mu.Lock()
	s.addrs = addrs
	s.mu.Unlock()
}

// selectPort picks the DART peer port from a slice's port list.
func (s *Seeder) selectPort(es *discoveryv1.EndpointSlice) (int32, bool) {
	for _, p := range es.Ports {
		if p.Port == nil {
			continue
		}
		if s.port.name != "" {
			if p.Name != nil && *p.Name == s.port.name {
				return *p.Port, true
			}
			continue
		}
		if *p.Port == s.port.number {
			return *p.Port, true
		}
	}
	return 0, false
}
