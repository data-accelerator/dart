package k8s

import (
	"context"
	"strings"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParseSpec(t *testing.T) {
	cases := []struct {
		spec     string
		wantNS   string
		wantSvc  string
		wantName string
		wantNum  int32
		wantErr  bool
	}{
		{spec: "dart-system/dart", wantNS: "dart-system", wantSvc: "dart", wantName: DefaultPortName},
		{spec: "ns/svc/p2p", wantNS: "ns", wantSvc: "svc", wantName: "p2p"},
		{spec: "ns/svc/9000", wantNS: "ns", wantSvc: "svc", wantNum: 9000},
		{spec: "ns/svc/", wantNS: "ns", wantSvc: "svc", wantName: DefaultPortName}, // trailing slash trimmed
		{spec: "", wantErr: true},
		{spec: "onlyns", wantErr: true},
		{spec: "/svc", wantErr: true},
		{spec: "ns/", wantErr: true},
		{spec: "a/b/c/d", wantErr: true},
		{spec: "ns/svc/0", wantErr: true},
		{spec: "ns/svc/70000", wantErr: true},
	}
	for _, c := range cases {
		ns, svc, port, err := parseSpec(c.spec)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseSpec(%q) succeeded, want error", c.spec)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSpec(%q): %v", c.spec, err)
			continue
		}
		if ns != c.wantNS || svc != c.wantSvc || port.name != c.wantName || port.number != c.wantNum {
			t.Errorf("parseSpec(%q) = (%q, %q, %+v)", c.spec, ns, svc, port)
		}
	}
}

// slice builds an EndpointSlice for svc with one port and the given endpoints,
// labeled the way the API server labels a Service's slices (which is what the
// informer's selector matches).
func slice(name, svc string, portName string, port int32, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	pn, p := portName, port
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "dart-system",
			Labels:    map[string]string{discoveryv1.LabelServiceName: svc},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: &pn, Port: &p}},
		Endpoints:   endpoints,
	}
}

func endpoint(ip string, ready *bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses:  []string{ip},
		Conditions: discoveryv1.EndpointConditions{Ready: ready},
	}
}

func boolp(b bool) *bool { return &b }

// startSeeder builds a Seeder on the fake clientset and runs its informer until
// the test ends, waiting for the first sync.
func startSeeder(t *testing.T, client *fake.Clientset, spec string) *Seeder {
	t.Helper()
	sd, err := NewSeederWithClient(client, spec)
	if err != nil {
		t.Fatalf("NewSeederWithClient(%q): %v", spec, err)
	}
	s := sd.(*Seeder)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Run(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for !s.synced.Load() {
		if time.Now().After(deadline) {
			t.Fatal("informer did not sync in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s
}

// waitSeeds polls until Seeds returns exactly want, or fails after a generous
// deadline: informer events are delivered asynchronously.
func waitSeeds(t *testing.T, s *Seeder, want ...string) {
	t.Helper()
	wantStr := strings.Join(want, ",")
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := s.Seeds(context.Background())
		if err != nil {
			t.Fatalf("Seeds: %v", err)
		}
		if strings.Join(got, ",") == wantStr {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Seeds = %v, want %v", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSeedsBeforeSyncIsEmpty(t *testing.T) {
	client := fake.NewSimpleClientset()
	s, err := NewSeederWithClient(client, "dart-system/dart")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Seeds(context.Background())
	if err != nil || len(got) != 0 {
		t.Errorf("Seeds before Run = (%v, %v), want empty", got, err)
	}
}

func TestReadyEndpointsOnly(t *testing.T) {
	client := fake.NewSimpleClientset(slice("dart-abc", "dart", "peer", 9000,
		endpoint("10.0.0.2", boolp(true)),
		endpoint("10.0.0.3", boolp(false)), // not ready: excluded
		endpoint("10.0.0.1", nil),          // nil means ready
		discoveryv1.Endpoint{Conditions: discoveryv1.EndpointConditions{Ready: boolp(true)}}, // no address: excluded
	))
	s := startSeeder(t, client, "dart-system/dart")
	waitSeeds(t, s, "10.0.0.1:9000", "10.0.0.2:9000") // sorted, not-ready excluded
}

func TestEndpointChurn(t *testing.T) {
	client := fake.NewSimpleClientset(slice("dart-abc", "dart", "peer", 9000,
		endpoint("10.0.0.1", boolp(true))))
	s := startSeeder(t, client, "dart-system/dart")
	waitSeeds(t, s, "10.0.0.1:9000")

	// An endpoint appears (scale-out) and goes away again (scale-in).
	updated := slice("dart-abc", "dart", "peer", 9000,
		endpoint("10.0.0.1", boolp(true)), endpoint("10.0.0.2", boolp(true)))
	if _, err := client.DiscoveryV1().EndpointSlices("dart-system").Update(
		context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitSeeds(t, s, "10.0.0.1:9000", "10.0.0.2:9000")

	if err := client.DiscoveryV1().EndpointSlices("dart-system").Delete(
		context.Background(), "dart-abc", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitSeeds(t, s)
}

func TestPortSelection(t *testing.T) {
	// A slice with several ports: only the one named in the spec is reported.
	multi := slice("dart-abc", "dart", "peer", 9000, endpoint("10.0.0.1", boolp(true)))
	httpName, httpPort := "http", int32(8080)
	multi.Ports = append(multi.Ports, discoveryv1.EndpointPort{Name: &httpName, Port: &httpPort})

	t.Run("by default name", func(t *testing.T) {
		client := fake.NewSimpleClientset(multi.DeepCopy())
		s := startSeeder(t, client, "dart-system/dart")
		waitSeeds(t, s, "10.0.0.1:9000")
	})
	t.Run("by explicit name", func(t *testing.T) {
		client := fake.NewSimpleClientset(multi.DeepCopy())
		s := startSeeder(t, client, "dart-system/dart/http")
		waitSeeds(t, s, "10.0.0.1:8080")
	})
	t.Run("by number", func(t *testing.T) {
		client := fake.NewSimpleClientset(multi.DeepCopy())
		s := startSeeder(t, client, "dart-system/dart/8080")
		waitSeeds(t, s, "10.0.0.1:8080")
	})
	t.Run("missing port skips the slice", func(t *testing.T) {
		client := fake.NewSimpleClientset(multi.DeepCopy())
		s := startSeeder(t, client, "dart-system/dart/metrics")
		time.Sleep(200 * time.Millisecond) // no event is coming; assert stability
		waitSeeds(t, s)
	})
}

func TestSchemeShape(t *testing.T) {
	if Scheme.Name != "k8s" || Scheme.New == nil {
		t.Errorf("Scheme = %+v, want name k8s with a constructor", Scheme)
	}
	if !strings.Contains(Scheme.Usage, "namespace") {
		t.Errorf("Scheme.Usage = %q, want it to document the spec", Scheme.Usage)
	}
}
