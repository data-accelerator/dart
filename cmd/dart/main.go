// Command dart runs a DART caching-proxy node: an HTTP server that serves
// object byte ranges through the read-through engine (cache, peers, origin).
//
// Origin-resolution modes:
//
//   - fixed origin (-origin URL): every request is served from that one origin;
//   - prefix passthrough (default): the request path after -prefix is the full
//     upstream URL, including scheme, e.g.
//     GET /dart/https://registry.example.com/v2/lib/nginx/blobs/sha256:...
//     (this matches overlaybd's p2pConfig address form).
//
// Peer-to-peer (optional): with -peers set (id@host:port,... including self,
// selected by -self-id) the node starts a peer block server on -peer-listen and,
// on a cache miss, pulls the block from the owning peer before origin. Without
// -peers the node is single-node (every miss goes to origin). This realizes the
// M2 star topology; the distribution tree is future work.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/data-accelerator/dart/internal/admin"
	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/engine"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/registry"
	"github.com/data-accelerator/dart/internal/store"
	"github.com/data-accelerator/dart/internal/tracker"
)

type config struct {
	listen           string
	origin           string
	prefix           string
	cacheDir         string
	cacheSize        int64
	chunkSize        int64
	blockSize        int64
	namespace        string
	selfID           string
	peerListen       string
	peers            string
	fanout           int
	adminAddr        string
	readerTree       bool
	trackerTick      time.Duration
	replicas         int
	ownedFraction    float64
	memSize          int64
	hedge            bool
	hedgeRatio       float64
	peerTimeout      time.Duration
	breakerFails     int
	breakerCool      time.Duration
	registry         string
	registryAuth     string
	ociDigestOnly    bool
	showVersion      bool
	discover         string
	peerAdvertise    string
	discoverInterval time.Duration
	forgetAfter      time.Duration
}

// version is stamped at build time via -ldflags "-X main.version=...". It is
// reported by -version and on startup so a running container can be identified.
var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dart:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	cfg, err := parseFlags(args, out)
	if err != nil {
		return err
	}
	if cfg.showVersion {
		fmt.Fprintf(out, "dart %s\n", version)
		return nil
	}

	n, err := build(cfg)
	if err != nil {
		return err
	}
	defer n.closer.Close()

	servers := []*http.Server{{
		Addr: cfg.listen, Handler: n.client, ReadHeaderTimeout: 15 * time.Second,
	}}
	if n.peer != nil {
		servers = append(servers, &http.Server{
			Addr: cfg.peerListen, Handler: n.peer, ReadHeaderTimeout: 15 * time.Second,
		})
	}
	if n.admin != nil {
		servers = append(servers, &http.Server{
			Addr: cfg.adminAddr, Handler: n.admin, ReadHeaderTimeout: 15 * time.Second,
		})
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Membership refresh runs alongside the servers. It is started before them so
	// that the first view is in place by the time requests can arrive.
	if n.discover != nil {
		go n.discover.Run(ctx)
	}

	errCh := make(chan error, len(servers))
	for _, s := range servers {
		go func(s *http.Server) {
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}(s)
	}

	// The registry mirror does not replace the other front end: both are dispatched
	// on the same listener by request path and share one cache. Reporting only one
	// of them would tell an operator that the other is disabled.
	mode := "prefix passthrough (/" + strings.Trim(cfg.prefix, "/") + "/<upstream-url>)"
	if cfg.origin != "" {
		mode = "fixed origin " + cfg.origin
	}
	if cfg.registry != "" {
		mode += " + registry mirror " + cfg.registry
	}
	p2p := "off"
	if n.peer != nil {
		p2p = "on (self=" + cfg.selfID + ", peer-listen=" + cfg.peerListen + ")"
		if cfg.discover != "" {
			p2p += " discover=" + cfg.discover
		} else {
			p2p += " peers=static"
		}
	}
	adminMode := "off"
	if n.admin != nil {
		adminMode = cfg.adminAddr
	}
	fmt.Fprintf(out, "dart client=%s | mode: %s | p2p: %s | admin: %s | cache: %s (%d MiB)\n",
		cfg.listen, mode, p2p, adminMode, cfg.cacheDir, cfg.cacheSize/chunk.MiB)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		fmt.Fprintln(out, "dart shutting down...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var firstErr error
		for _, s := range servers {
			if err := s.Shutdown(shutCtx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}

func parseFlags(args []string, out io.Writer) (config, error) {
	fs := flag.NewFlagSet("dart", flag.ContinueOnError)
	fs.SetOutput(out)
	var cfg config
	fs.BoolVar(&cfg.showVersion, "version", false, "print the version and exit")
	fs.StringVar(&cfg.listen, "listen", ":8080", "client listen address")
	fs.StringVar(&cfg.origin, "origin", "", "fixed upstream origin URL; empty enables prefix passthrough")
	fs.StringVar(&cfg.prefix, "prefix", "dart", "path prefix for passthrough mode")
	fs.StringVar(&cfg.cacheDir, "cache-dir", "./dart-cache", "directory for the block cache file")
	// Sizes accept a unit suffix (8GiB, 512MiB) as well as a plain byte count.
	sizeVar(fs, &cfg.cacheSize, "cache-size", 1*chunk.GiB, "block cache capacity, e.g. 8GiB")
	sizeVar(fs, &cfg.chunkSize, "chunk-size", 256*chunk.MiB, "chunk size (placement/tree unit), e.g. 256MiB")
	sizeVar(fs, &cfg.blockSize, "block-size", 4*chunk.MiB, "block size (transfer/cache unit), e.g. 4MiB")
	fs.StringVar(&cfg.namespace, "namespace", "dart", "chunk-key namespace")
	fs.StringVar(&cfg.selfID, "self-id", "", "this node's cluster ID (required with -peers)")
	fs.StringVar(&cfg.peerListen, "peer-listen", ":9000", "peer block-server listen address (P2P mode)")
	fs.StringVar(&cfg.discover, "discover", "",
		"maintain membership by discovery instead of a fixed -peers list: "+
			"dns:<name>:<port> (a headless Service) or static:<a>,<b>,...")
	fs.StringVar(&cfg.peerAdvertise, "peer-advertise", "",
		"host:port peers should use to reach this node; defaults to -peer-listen with the "+
			"host taken from POD_IP or the hostname when it is a wildcard")
	fs.DurationVar(&cfg.discoverInterval, "discover-interval", cluster.DefaultRefreshInterval,
		"how often to re-resolve seeds and re-exchange rosters")
	fs.DurationVar(&cfg.forgetAfter, "forget-after", cluster.DefaultForgetAfter,
		"how long a member must be both unseen and unreachable before it is dropped from "+
			"membership; deliberately much larger than -discover-interval because removal "+
			"re-runs placement while routing around a dead peer is already immediate")
	fs.StringVar(&cfg.peers, "peers", "", "comma-separated members id@host:port (incl. self) to enable P2P")
	fs.IntVar(&cfg.fanout, "fanout", 2, "distribution-tree branching factor (children per node)")
	fs.StringVar(&cfg.adminAddr, "admin", ":9100", "admin/metrics listen address; empty disables")
	fs.BoolVar(&cfg.readerTree, "reader-tree", true, "build the distribution tree over the active reader set (per-file tracker)")
	fs.DurationVar(&cfg.trackerTick, "tracker-tick", tracker.DefaultTick, "how often a tracker republishes a reader set")
	fs.IntVar(&cfg.replicas, "replicas", 1, "HRW candidates that authoritatively hold a chunk (the owned budget)")
	fs.Float64Var(&cfg.ownedFraction, "owned-fraction", 0.8, "share of the cache reserved for owned blocks (0<f<1)")
	sizeVar(fs, &cfg.memSize, "mem-size", 256*chunk.MiB, "in-memory hot-set size; below one block disables the memory tier")
	fs.BoolVar(&cfg.hedge, "hedge", true, "hedge a slow peer fetch to the grandparent/root once it exceeds the estimated p99")
	fs.Float64Var(&cfg.hedgeRatio, "hedge-ratio", 0.05, "max share of peer fetches allowed to hedge")
	fs.DurationVar(&cfg.peerTimeout, "peer-timeout", peer.DefaultRequestTimeout, "per-request timeout for peer block fetches")
	fs.IntVar(&cfg.breakerFails, "breaker-failures", peer.DefaultFailureThreshold, "consecutive peer failures that open its circuit; 0 disables breaking")
	fs.DurationVar(&cfg.breakerCool, "breaker-cooldown", peer.DefaultBreakerCooldown, "how long a peer circuit stays open before a probe")
	fs.StringVar(&cfg.registry, "registry", "", "also serve as a pull-through mirror for this registry on /v2/ (e.g. https://registry-1.docker.io); coexists with -origin/-prefix on the same listener")
	fs.StringVar(&cfg.registryAuth, "registry-auth", "", "JSON file mapping registry host to {username,password} for private upstreams")
	fs.BoolVar(&cfg.ociDigestOnly, "oci-digest-only", false,
		"only treat \"<algo>:<hex>\" paths as content-addressed; disables recognizing Docker Distribution's object-storage layout")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	// The mirror and the prefix API share a listener, so the prefix must not
	// shadow the registry API path.
	if cfg.registry != "" && "/"+strings.Trim(cfg.prefix, "/")+"/" == registry.Path {
		return config{}, fmt.Errorf("-prefix %q collides with the registry API path %q; pick another prefix",
			cfg.prefix, registry.Path)
	}
	return cfg, nil
}

// parsePeers parses "id1@host:port,id2@host:port,..." into Ready members.
func parsePeers(s string) ([]cluster.Member, error) {
	var ms []cluster.Member
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		id, addr, ok := strings.Cut(tok, "@")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q (want id@host:port)", tok)
		}
		ms = append(ms, cluster.Member{ID: id, Addr: addr, Weight: 1, State: cluster.Ready})
	}
	if len(ms) == 0 {
		return nil, fmt.Errorf("no peers parsed from %q", s)
	}
	return ms, nil
}

// node holds a built DART node's handlers and the resource closer.
type node struct {
	client   http.Handler // client data plane (required)
	peer     http.Handler // peer block server; nil when P2P is off
	admin    http.Handler // metrics/admin; nil when -admin is empty
	closer   io.Closer    // releases the store and the cache-dir lock
	discover *cluster.DynamicProvider
}

// closers releases several resources in order, returning the first error.
type closers []io.Closer

func (cs closers) Close() error {
	var first error
	for _, c := range cs {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// build constructs the store, engine, and handlers from cfg.
func build(cfg config) (*node, error) {
	cc := chunk.Config{ChunkSize: cfg.chunkSize, BlockSize: cfg.blockSize}
	if err := cc.Validate(); err != nil {
		return nil, err
	}
	if cfg.cacheSize < cfg.blockSize {
		return nil, fmt.Errorf("cache-size %d must be >= block-size %d", cfg.cacheSize, cfg.blockSize)
	}
	if err := os.MkdirAll(cfg.cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	// Take the directory lock BEFORE opening any store: the arena open truncates,
	// so a second instance must be refused here rather than after it has already
	// wiped the running instance's cache.
	lock, err := store.LockDir(cfg.cacheDir)
	if err != nil {
		return nil, err
	}
	disk, err := store.OpenTiered(store.TieredOptions{
		Path:          filepath.Join(cfg.cacheDir, "blocks"),
		SlotSize:      cfg.blockSize,
		Slots:         int(cfg.cacheSize / cfg.blockSize),
		OwnedFraction: cfg.ownedFraction,
	})
	if err != nil {
		lock.Close()
		return nil, err
	}
	// A hybrid store keeps a small hot set in RAM in front of the disk tiers;
	// both are caches, so memory eviction only costs a disk read.
	var st store.ClassStore = disk
	if cfg.memSize >= cfg.blockSize {
		mem, err := store.OpenMem(store.MemOptions{
			SlotSize: cfg.blockSize,
			Slots:    int(cfg.memSize / cfg.blockSize),
		})
		if err != nil {
			disk.Close()
			lock.Close()
			return nil, err
		}
		st = store.NewHybrid(mem, disk)
	}
	// Release in reverse order: stores first, then the directory lock.
	closer := closers{st, lock}

	// Registry credentials, when configured, must cover the whole data path: the
	// mirror's pass-through proxy AND the engine's blob fetches. The credential is
	// a property of the upstream rather than of a request, because a coalesced
	// fetch is shared between callers (see internal/registry/auth.go).
	var upstreamRT http.RoundTripper
	if cfg.registryAuth != "" {
		creds, err := registry.LoadCredentials(cfg.registryAuth)
		if err != nil {
			closer.Close()
			return nil, err
		}
		upstreamRT = registry.NewAuthTransport(nil, creds)
	}

	// Object identity: how a digest is recovered from an upstream URL. It feeds
	// both the cache key and the fetch-coalescing key, which must agree.
	layout := chunk.LayoutDistribution
	if cfg.ociDigestOnly {
		layout = chunk.LayoutOCIOnly
	}
	objectID := func(u string) string {
		id, _ := chunk.ObjectIDLayout(u, layout)
		return id
	}

	reg := metrics.NewRegistry()
	engine.RegisterStoreMetrics(reg, st)
	var trackerReg *tracker.Registry
	eopt := engine.Options{
		Chunk: cc, Store: st,
		// Coalesce by content identity, not by URL: a presigned upstream is signed
		// afresh per request, so URL-keyed dedup would let every client open its own
		// origin fetch for the same block.
		Fetcher: &fetch.Coalescing{
			F:   &fetch.HTTPFetcher{Client: &http.Client{Transport: upstreamRT}},
			Key: objectID,
		},
		Namespace: cfg.namespace, Fanout: cfg.fanout, Replicas: cfg.replicas,
		Hedge: cfg.hedge, HedgeRatio: cfg.hedgeRatio,
		Metrics: engine.NewMetrics(reg),
	}
	// P2P is enabled by either a static peer list or a discovery spec.
	p2p := cfg.peers != "" || cfg.discover != ""
	var dyn *cluster.DynamicProvider
	if p2p {
		if cfg.selfID == "" {
			closer.Close()
			return nil, fmt.Errorf("-self-id is required with -peers or -discover")
		}
		if cfg.peers != "" && cfg.discover != "" {
			closer.Close()
			return nil, fmt.Errorf("-peers and -discover are alternatives: -peers fixes membership, -discover maintains it")
		}

		pc := peer.NewClient()
		pc.Timeout = cfg.peerTimeout
		if cfg.breakerFails > 0 {
			brk := peer.NewBreaker(peer.BreakerOptions{
				FailureThreshold: cfg.breakerFails,
				Cooldown:         cfg.breakerCool,
			})
			pc.Breaker = brk
			engine.RegisterPeerMetrics(reg, brk)
		}

		switch {
		case cfg.discover != "":
			seeder, err := cluster.ParseSeeder(cfg.discover)
			if err != nil {
				closer.Close()
				return nil, err
			}
			selfAddr, err := advertisedAddr(cfg.peerAdvertise, cfg.peerListen)
			if err != nil {
				closer.Close()
				return nil, err
			}
			dyn = cluster.NewDynamicProvider(cluster.DynamicConfig{
				Self:            cluster.Member{ID: cfg.selfID, Addr: selfAddr, Weight: 1},
				Seeder:          seeder,
				Fetcher:         &rosterFetcher{c: pc, selfID: cfg.selfID, selfAddr: selfAddr},
				RefreshInterval: cfg.discoverInterval,
				ForgetAfter:     cfg.forgetAfter,
				OnError: func(err error) {
					// Discovery errors are expected and transient (a peer restarting, DNS
					// briefly unavailable). Membership is unaffected, so this is a log line
					// rather than a failure.
					fmt.Fprintln(os.Stderr, "dart: discover:", err)
				},
			})
			eopt.Cluster = dyn
		default:
			members, err := parsePeers(cfg.peers)
			if err != nil {
				closer.Close()
				return nil, err
			}
			eopt.Cluster = cluster.NewStaticProvider(members...)
		}

		eopt.Peer = pc
		eopt.SelfID = cfg.selfID
		if cfg.readerTree {
			// Active-reader-set tree: build the distribution tree over the nodes
			// currently reading a file, so a parent is always another reader.
			trackerReg = tracker.NewRegistry(tracker.Options{Tick: cfg.trackerTick})
			eopt.TrackerRegistry = trackerReg
			eopt.TrackerClient = tracker.NewClient()
		}
	}

	e, err := engine.New(eopt)
	if err != nil {
		closer.Close()
		return nil, err
	}

	// Front-ends share one engine and therefore one cache. They serve different
	// clients and can run together on the same listener:
	//
	//   /v2/...          containerd's registry mirror (-registry)
	//   /<prefix>/<url>  OverlayBD's p2pConfig API, or a fixed -origin
	//
	// A blob pulled either way is stored once, because both key on its digest.
	prefixH := &engine.Handler{E: e, Resolve: resolver(cfg.origin, cfg.prefix)}
	var clientH http.Handler = prefixH
	if cfg.registry != "" {
		mirror, err := registry.New(registry.Options{
			Upstream: cfg.registry, Engine: e, Transport: upstreamRT,
		})
		if err != nil {
			closer.Close()
			return nil, err
		}
		clientH = dispatch(mirror, prefixH)
	}

	n := &node{
		client: clientH,
		closer: closer,
	}
	if p2p {
		// Cut-through relay: a peer streams a block while receiving it, so a
		// multi-hop chain pipelines instead of storing-and-forwarding per hop.
		// The tracker (if enabled) shares the peer listener so readers can JOIN.
		blocks := &peer.StreamServer{NodeID: cfg.selfID, Src: e.PeerStreamSource()}
		var mux *http.ServeMux
		if trackerReg != nil {
			mux = (&tracker.Server{R: trackerReg}).Handler()
		} else {
			mux = http.NewServeMux()
		}
		mux.Handle("/peer/", blocks)
		if dyn != nil {
			// Membership exchange rides the peer listener. Learn makes it
			// bidirectional: whoever asks us also tells us who they are, so a node
			// with nothing to seed from still joins.
			mux.Handle(RosterRoute, &peer.RosterServer{
				NodeID: cfg.selfID,
				Src:    func() peer.Roster { return rosterOf(dyn) },
				Learn:  dyn.LearnPeer,
			})
		}
		n.peer = mux
		n.discover = dyn
	}
	if cfg.adminAddr != "" {
		n.admin = admin.Handler(admin.Options{
			Registry: reg, Store: st, Cluster: eopt.Cluster, SelfID: cfg.selfID,
		})
	}
	return n, nil
}

// dispatch routes Registry v2 traffic to the mirror and everything else to the
// prefix/origin handler.
//
// It matches on RequestURI rather than using an http.ServeMux because the prefix
// handler's paths embed a full upstream URL: a ServeMux would path-clean the
// "//" in "https://" down to "/" and corrupt it (see design note in
// docs/dart.md).
func dispatch(mirror, prefixH http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uri := r.RequestURI
		if strings.HasPrefix(uri, registry.Path) || uri == strings.TrimSuffix(registry.Path, "/") {
			mirror.ServeHTTP(w, r)
			return
		}
		prefixH.ServeHTTP(w, r)
	})
}

// resolver maps a request to its upstream origin URL. With a fixed origin it
// always returns that URL; otherwise it strips "/<prefix>/" from the raw
// request URI and treats the remainder as the upstream URL (which must include
// a scheme). It uses RequestURI (not URL.Path) so an embedded "https://" is not
// collapsed by path cleaning.
func resolver(origin, prefix string) func(*http.Request) (string, error) {
	if origin != "" {
		return func(*http.Request) (string, error) { return origin, nil }
	}
	p := "/" + strings.Trim(prefix, "/") + "/"
	return func(r *http.Request) (string, error) {
		uri := r.RequestURI
		if !strings.HasPrefix(uri, p) {
			return "", fmt.Errorf("path does not start with %q", p)
		}
		up := uri[len(p):]
		if !strings.HasPrefix(up, "http://") && !strings.HasPrefix(up, "https://") {
			return "", fmt.Errorf("upstream URL must include a scheme (http:// or https://)")
		}
		return up, nil
	}
}
