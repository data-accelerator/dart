package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

func rootHTTP(t *testing.T, e *Engine) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(peer.MetadataPath, &peer.MetadataServer{Probe: e.ProbeSource()})
	mux.Handle("/peer/", &peer.Server{Borrow: e.PeerLeaseSource()})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func delegatedMetadataEngine(t *testing.T) *Engine {
	t.Helper()
	root := contentKeyedEngine(t)
	srv := rootHTTP(t, root)
	forbidden := &forbiddenOrigin{}
	leaf, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: forbidden,
		SelfID: "leaf", Role: cluster.NonRoot, Peer: peer.NewClient(),
		Cluster: cluster.NewStaticProvider(
			cluster.Member{ID: "root", Addr: strings.TrimPrefix(srv.URL, "http://"), State: cluster.Ready, Role: cluster.RootOnly, Weight: 1},
			cluster.Member{ID: "leaf", Addr: "unused:1", State: cluster.Ready, Role: cluster.NonRoot, Weight: 1})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if forbidden.calls.Load() != 0 {
			t.Error("delegated reader contacted origin")
		}
	})
	return leaf
}

func TestDelegatedSizeScopesRangeCapabilityToOrigin(t *testing.T) {
	testSizeScopesRangeCapability(t, delegatedMetadataEngine)
}
func TestConcurrentDelegatedSizeProbes(t *testing.T) {
	testConcurrentSizeProbes(t, delegatedMetadataEngine)
}

func TestDelegatedProbeRecoveryAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint("cancel=", canceled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started, stopped := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					close(started)
					if canceled {
						<-r.Context().Done()
						close(stopped)
						return
					}
					w.WriteHeader(503)
					return
				}
				w.Header().Set("Content-Range", "bytes 0-0/16")
				w.Header().Set("Content-Length", "1")
				w.WriteHeader(206)
				_, _ = w.Write([]byte{7})
			}))
			defer origin.Close()
			e := delegatedMetadataEngine(t)
			caller, stopCaller := context.WithCancel(ctx)
			defer stopCaller()
			done := make(chan error, 1)
			go func() { _, err := e.Size(caller, origin.URL); done <- err }()
			waitBlockEvent(t, ctx, started)
			if canceled {
				stopCaller()
			}
			select {
			case err := <-done:
				if err == nil || (canceled && !errors.Is(err, context.Canceled)) {
					t.Fatal("probe failure lost", err)
				}
			case <-ctx.Done():
				t.Fatal("probe failed to terminate", ctx.Err())
			}
			if canceled {
				waitBlockEvent(t, ctx, stopped)
			}
			if size, err := e.Size(ctx, origin.URL); err != nil || size != 16 {
				t.Fatal("failed probe poisoned retry", size, err)
			}
			if size, err := e.Size(ctx, origin.URL); err != nil || size != 16 || calls.Load() != 2 {
				t.Fatal("successful metadata not reused", size, err, calls.Load())
			}
		})
	}
}

func TestRoleWeightSkewDeclinesBeforeFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	root := contentKeyedEngine(t)
	var rootCalls atomic.Int32
	rootBlocks := &peer.Server{Borrow: root.PeerLeaseSource()}
	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rootCalls.Add(1)
		if r.Header.Get(peer.HeaderSupply) != "1" {
			t.Error("recovery root entered reader routing")
		}
		rootBlocks.ServeHTTP(w, r)
	}))
	defer rs.Close()
	rootAddr := strings.TrimPrefix(rs.URL, "http://")
	forbidden := &forbiddenOrigin{}
	engines := make(map[string]*Engine)
	providers := make(map[string]*cluster.StaticProvider)
	servers := make(map[string]*httptest.Server)
	var relays, maxSeen atomic.Int32
	for _, id := range []string{"A", "B"} {
		p := cluster.NewStaticProvider()
		e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: forbidden, SelfID: id,
			Role: cluster.NonRoot, Cluster: p, Peer: peer.NewClient(), Fanout: 1, PrefetchWindow: 1})
		if err != nil {
			t.Fatal(err)
		}
		e.sizes[fetch.OriginKey(origin.URL)] = sizeMeta{size: 16}
		blocks := &peer.Server{Borrow: e.PeerLeaseSource()}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			relays.Add(1)
			hop, _ := strconv.Atoi(r.Header.Get(peer.HeaderHop))
			for old := maxSeen.Load(); int32(hop) > old && !maxSeen.CompareAndSwap(old, int32(hop)); old = maxSeen.Load() {
			}
			blocks.ServeHTTP(w, r)
		}))
		defer srv.Close()
		engines[id], providers[id], servers[id] = e, p, srv
	}
	for _, self := range []string{"A", "B"} {
		members := []cluster.Member{{ID: "R", Addr: rootAddr, Weight: 1, State: cluster.Ready, Role: cluster.RootOnly}}
		for _, id := range []string{"A", "B"} {
			weight := 1.0
			if id != self {
				weight = 1e9
			}
			members = append(members, cluster.Member{ID: id, Addr: strings.TrimPrefix(servers[id].URL, "http://"), Weight: weight, State: cluster.Ready, Role: cluster.NonRoot})
		}
		providers[self].Set(members)
	}
	var output bytes.Buffer
	if err := engines["A"].Serve(ctx, &output, origin.URL, 0, 15); err != nil || !bytes.Equal(output.Bytes(), content) {
		t.Fatal("skewed relay did not recover", err)
	}
	// Opposing weights change the ordering basis, unlike reader-set churn.
	// The cold receiver declines before admission, then A uses terminal supply.
	if maxSeen.Load() != 1 || relays.Load() != 1 || rootCalls.Load() != 1 {
		t.Fatal("unexpected hop/recovery path", maxSeen.Load(), relays.Load(), rootCalls.Load())
	}
	if forbidden.calls.Load() != 0 {
		t.Fatal("non-root bypassed supply root")
	}
	for _, srv := range servers {
		srv.Close()
	}
	for _, e := range engines {
		if n, b := receiveUsage(e); n != 0 || b != 0 {
			t.Fatal("nested relay leaked receive capacity", n, b)
		}
	}
}

func TestZeroClassBudgetsDeliverOverP2P(t *testing.T) {
	content := blob(83)
	origin := countingOrigin(t, content, nil)
	open := func(fraction float64) *store.Tiered {
		s, err := store.OpenTiered(store.TieredOptions{Memory: true, SlotSize: 16, Slots: 1, OwnedFraction: fraction, OwnedFractionSet: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	rootStore, leafStore := open(0), open(1)
	root, err := New(Options{Chunk: testCfg(), Store: rootStore, Fetcher: newFetcher(), Role: cluster.RootOnly, ReceiveBytes: 16, ReceiveLoads: 1})
	if err != nil {
		t.Fatal(err)
	}
	srv := rootHTTP(t, root)
	forbidden := &forbiddenOrigin{}
	leaf, err := New(Options{Chunk: testCfg(), Store: leafStore, Fetcher: forbidden, SelfID: "leaf", Role: cluster.NonRoot,
		Peer: peer.NewClient(), Cluster: cluster.NewStaticProvider(
			cluster.Member{ID: "root", Addr: strings.TrimPrefix(srv.URL, "http://"), State: cluster.Ready, Role: cluster.RootOnly, Weight: 1},
			cluster.Member{ID: "leaf", Addr: "unused:1", State: cluster.Ready, Role: cluster.NonRoot, Weight: 1}),
		PrefetchWindow: 8, ReceiveBytes: 16, ReceiveLoads: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := leaf.Serve(ctx, &output, origin.URL, 0, 82); err != nil || !bytes.Equal(output.Bytes(), content) {
		t.Fatal("zero-budget P2P read", err)
	}
	srv.Close()
	if rootStore.Len() != 0 || leafStore.Len() != 0 || forbidden.calls.Load() != 0 {
		t.Fatal("disabled classes cached or non-root fetched origin")
	}
	for _, e := range []*Engine{root, leaf} {
		if n, b := receiveUsage(e); n != 0 || b != 0 {
			t.Fatal("P2P result retained receive capacity", n, b)
		}
	}
}

func TestRootCandidatesShareAttemptDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rootKey := blockKeyFor(prefetchURL).Chunk
	order := hashring.Rank(rootKey, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}})
	var backupCalls atomic.Int32
	started, stopped := make(chan struct{}), make(chan struct{})
	var members []cluster.Member
	for _, id := range []string{"A", "B"} {
		primary := id == order[0].ID
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if primary {
				close(started)
				<-r.Context().Done()
				close(stopped)
				return
			}
			backupCalls.Add(1)
			w.Header().Set("Content-Length", "16")
			_, _ = w.Write(blob(16))
		}))
		defer srv.Close()
		members = append(members, cluster.Member{ID: id, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly})
	}
	members = append(members, cluster.Member{ID: "leaf", Addr: "unused:1", State: cluster.Ready, Role: cluster.NonRoot})
	f := &fetch.Coalescing{F: &forbiddenOrigin{}, MaxFlight: 50 * time.Millisecond}
	pc := peer.NewClient()
	pc.Timeout = time.Second // a single peer timeout must not replace the attempt-set budget
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: f, Cluster: cluster.NewStaticProvider(members...), Peer: pc, SelfID: "leaf", Role: cluster.NonRoot})
	if err != nil {
		t.Fatal(err)
	}
	e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: 16}
	done := make(chan error, 1)
	go func() { done <- e.Serve(ctx, &bytes.Buffer{}, prefetchURL, 0, 15) }()
	waitBlockEvent(t, ctx, started)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("candidate sequence escaped deadline", err)
		}
	case <-ctx.Done():
		t.Fatal("candidate sequence did not terminate")
	}
	waitBlockEvent(t, ctx, stopped)
	if backupCalls.Load() != 0 {
		t.Fatal("new root attempt started after shared deadline")
	}
	if n, b := receiveUsage(e); n != 0 || b != 0 {
		t.Fatal("deadline leaked buffer", n, b)
	}
}

func TestRoutingEpochSkewKeepsCacheAndSupplyUsable(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	p := cluster.NewStaticProvider(cluster.Member{ID: "A", Addr: "a:1", Weight: 1, State: cluster.Ready})
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(), Cluster: p, Peer: peer.NewClient(), SelfID: "A"})
	if err != nil {
		t.Fatal(err)
	}
	key := blockKeyFor(origin.URL)
	req := peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1, HasEpoch: true, Epoch: p.Current().Epoch() ^ 1}
	l, held, err := e.PeerLeaseSource()(context.Background(), req)
	l.Release()
	if err != nil || held || len(e.borrowed.m) != 0 || len(e.sizes) != 0 {
		t.Fatal("cold epoch mismatch did not decline before probe/flight", held, err)
	}
	req.Supply = true
	l, held, err = e.PeerLeaseSource()(context.Background(), req)
	if err != nil || !held || !bytes.Equal(l.Bytes(), content) {
		t.Fatal("terminal supply rejected epoch skew", held, err)
	}
	l.Release()
	req.Supply = false
	l, held, err = e.PeerLeaseSource()(context.Background(), req)
	defer l.Release()
	if err != nil || !held || !bytes.Equal(l.Bytes(), content) {
		t.Fatal("warm immutable bytes rejected epoch skew", held, err)
	}
}
