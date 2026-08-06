package engine

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// --- latency estimator ---

func TestLatencyEstimatorNeedsSamples(t *testing.T) {
	l := newLatencyEstimator()
	if _, ok := l.quantile(0.99); ok {
		t.Error("an empty estimator should report no usable quantile")
	}
	for i := 0; i < 15; i++ {
		l.Observe(time.Millisecond)
	}
	if _, ok := l.quantile(0.99); ok {
		t.Error("fewer than 16 samples should still be untrusted")
	}
	l.Observe(time.Millisecond)
	if _, ok := l.quantile(0.99); !ok {
		t.Error("16 samples should yield a quantile")
	}
}

func TestLatencyEstimatorQuantile(t *testing.T) {
	l := newLatencyEstimator()
	// 99 fast samples and one slow: p99 must land near the slow end, p50 fast.
	for i := 0; i < 99; i++ {
		l.Observe(time.Millisecond)
	}
	l.Observe(time.Second)

	p50, ok := l.quantile(0.5)
	if !ok || p50 != time.Millisecond {
		t.Errorf("p50 = %v (ok=%v), want 1ms", p50, ok)
	}
	p99, ok := l.quantile(0.99)
	if !ok || p99 < time.Millisecond {
		t.Errorf("p99 = %v (ok=%v)", p99, ok)
	}
}

// TestLatencyEstimatorRingWraps: the window is bounded, so old samples age out.
func TestLatencyEstimatorRingWraps(t *testing.T) {
	l := newLatencyEstimator()
	for i := 0; i < latencyWindow; i++ {
		l.Observe(time.Second) // fill entirely with slow samples
	}
	for i := 0; i < latencyWindow; i++ {
		l.Observe(time.Millisecond) // overwrite entirely with fast ones
	}
	p99, ok := l.quantile(0.99)
	if !ok || p99 != time.Millisecond {
		t.Errorf("p99 = %v (ok=%v), want 1ms after the window wrapped", p99, ok)
	}
}

// --- hedge limiter ---

// TestHedgeLimiterEnforcesRatio: the long-run hedge rate must track the ratio,
// which is what stops a uniformly slow cluster from doubling its own load.
func TestHedgeLimiterEnforcesRatio(t *testing.T) {
	const attempts = 10000
	for _, ratio := range []float64{0.05, 0.25, 0.5} {
		h := newHedgeLimiter(ratio)
		allowed := 0
		for i := 0; i < attempts; i++ {
			if h.allow() {
				allowed++
			}
		}
		got := float64(allowed) / attempts
		if got < ratio*0.9 || got > ratio*1.1 {
			t.Errorf("ratio %v: allowed %.4f of attempts, want ~%v", ratio, got, ratio)
		}
	}
}

func TestHedgeLimiterDefaultsAndClamps(t *testing.T) {
	if h := newHedgeLimiter(0); h.ratio != defaultHedgeRatio {
		t.Errorf("zero ratio = %v, want default %v", h.ratio, defaultHedgeRatio)
	}
	if h := newHedgeLimiter(-1); h.ratio != defaultHedgeRatio {
		t.Errorf("negative ratio = %v, want default", h.ratio)
	}
	if h := newHedgeLimiter(5); h.ratio != 1 {
		t.Errorf("ratio > 1 = %v, want clamped to 1", h.ratio)
	}
}

// TestHedgeLimiterNoBurstAfterIdle: credit is capped at one hedge, so a long
// quiet period cannot release a burst of duplicates.
func TestHedgeLimiterNoBurstAfterIdle(t *testing.T) {
	h := newHedgeLimiter(0.5)
	for i := 0; i < 100; i++ {
		h.allow() // accumulate credit
	}
	burst := 0
	for i := 0; i < 5; i++ {
		if h.allow() {
			burst++
		}
	}
	if burst > 3 {
		t.Errorf("allowed %d hedges in a row after idling; credit is not capped", burst)
	}
}

// --- hedgeDelay ---

func TestHedgeDelayDisabledAndBounds(t *testing.T) {
	e := &Engine{latency: newLatencyEstimator(), hedges: newHedgeLimiter(0)}
	if _, ok := e.hedgeDelay(); ok {
		t.Error("hedging should be off when not enabled")
	}
	e.hedgeEnabled = true
	if _, ok := e.hedgeDelay(); ok {
		t.Error("hedging needs latency samples before it can trigger")
	}
	// Very fast cluster: the delay is floored so we do not hedge on noise.
	for i := 0; i < 32; i++ {
		e.latency.Observe(time.Microsecond)
	}
	d, ok := e.hedgeDelay()
	if !ok || d != minHedgeDelay {
		t.Errorf("delay = %v (ok=%v), want the floor %v", d, ok, minHedgeDelay)
	}
	// Pathologically slow samples: the delay is capped so hedging stays possible.
	e2 := &Engine{hedgeEnabled: true, latency: newLatencyEstimator(), hedges: newHedgeLimiter(0)}
	for i := 0; i < 32; i++ {
		e2.latency.Observe(time.Hour)
	}
	d2, ok := e2.hedgeDelay()
	if !ok || d2 != maxHedgeDelay {
		t.Errorf("delay = %v (ok=%v), want the cap %v", d2, ok, maxHedgeDelay)
	}
}

// --- end-to-end hedging ---

// slowPeer serves a block after a delay, counting requests. A delay of 0 responds
// immediately.
func slowPeer(t *testing.T, id string, delay time.Duration, data []byte, hits *int64) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			atomic.AddInt64(hits, 1)
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return // the caller cancelled us (hedge loser)
		}
		w.Header().Set(peer.HeaderNode, id)
		w.Header().Set("Content-Length", itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// newHedgeEngine builds a P2P engine with hedging forced on and a warm latency
// window, so the hedge delay is the floor rather than "not enough samples".
func newHedgeEngine(t *testing.T, prov cluster.Provider, selfID string) *Engine {
	t.Helper()
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: selfID, Fanout: 1,
		Hedge: true, HedgeRatio: 1, // always allow a hedge in tests
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 32; i++ {
		e.latency.Observe(time.Microsecond) // delay == minHedgeDelay
	}
	return e
}

// TestHedgeBeatsSlowPrimary is the point of hedging: the tree parent is alive but
// stalled, and the read still completes quickly from the backup instead of waiting
// for the parent.
func TestHedgeBeatsSlowPrimary(t *testing.T) {
	data := bytes.Repeat([]byte{0x42}, 16)
	const stall = 5 * time.Second

	var slowHits, fastHits int64
	slowAddr := slowPeer(t, "SLOW", stall, data, &slowHits)
	fastAddr := slowPeer(t, "FAST", 0, data, &fastHits)

	// Chain of three with fanout=1: rank0 <- rank1 <- rank2. Running as the tail
	// makes rank1 the parent (slow) and rank0 the grandparent (fast).
	const chunkKey = uint64(0xC0FFEE)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	members := []cluster.Member{
		{ID: ranked[0].ID, Addr: fastAddr, Weight: 1, State: cluster.Ready},
		{ID: ranked[1].ID, Addr: slowAddr, Weight: 1, State: cluster.Ready},
		{ID: ranked[2].ID, Weight: 1, State: cluster.Ready}, // self
	}
	prov := cluster.NewStaticProvider(members...)
	e := newHedgeEngine(t, prov, ranked[2].ID)

	key := store.BlockKey{Chunk: chunkKey, Block: 0}
	start := time.Now()
	got, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", "obj", chunkKey, key, 0)
	elapsed := time.Since(start)

	if !ok || !bytes.Equal(got, data) {
		t.Fatalf("fromPeer ok=%v len=%d, want the block", ok, len(got))
	}
	if elapsed > stall/2 {
		t.Errorf("took %v: the read waited on the stalled parent instead of hedging", elapsed)
	}
	if atomic.LoadInt64(&slowHits) == 0 {
		t.Error("the primary was never tried")
	}
	if atomic.LoadInt64(&fastHits) == 0 {
		t.Error("the hedge to the backup never fired")
	}
}

// TestHedgeDisabledWaitsForPrimary: with hedging off the same topology has no
// escape hatch, which is exactly the gap hedging closes. Use a short stall and a
// short client timeout so the test stays fast.
func TestHedgeDisabledWaitsForPrimary(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 16)
	var slowHits, fastHits int64
	slowAddr := slowPeer(t, "SLOW", 300*time.Millisecond, data, &slowHits)
	fastAddr := slowPeer(t, "FAST", 0, data, &fastHits)

	const chunkKey = uint64(0xBEEF01)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: fastAddr, Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[1].ID, Addr: slowAddr, Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[2].ID, Weight: 1, State: cluster.Ready},
	)
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: ranked[2].ID, Fanout: 1,
		// Hedge left false.
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	key := store.BlockKey{Chunk: chunkKey, Block: 0}
	got, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", "obj", chunkKey, key, 0)
	if !ok || !bytes.Equal(got, data) {
		t.Fatalf("fromPeer ok=%v, want the block from the (slow) parent", ok)
	}
	if atomic.LoadInt64(&fastHits) != 0 {
		t.Error("no hedge should have been sent with hedging disabled")
	}
}

// TestHedgeFallsBackWhenBothMiss: if neither contender holds the block, the
// caller still gets ok=false and falls through to origin.
func TestHedgeFallsBackWhenBothMiss(t *testing.T) {
	miss := func() string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "not held", http.StatusNotFound)
		}))
		t.Cleanup(srv.Close)
		return strings.TrimPrefix(srv.URL, "http://")
	}
	const chunkKey = uint64(0x5150)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: miss(), Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[1].ID, Addr: miss(), Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[2].ID, Weight: 1, State: cluster.Ready},
	)
	e := newHedgeEngine(t, prov, ranked[2].ID)

	key := store.BlockKey{Chunk: chunkKey, Block: 0}
	if _, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", "obj", chunkKey, key, 0); ok {
		t.Error("both peers missed, so fromPeer must report false")
	}
}

// TestHedgeTargetsPickParentAndGrandparent verifies the topology choice: primary
// is the tree parent, backup is the grandparent (or the root), and the root
// itself has no upstream.
func TestHedgeTargetsPickParentAndGrandparent(t *testing.T) {
	const chunkKey = uint64(0xABBA)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: "r0:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[1].ID, Addr: "r1:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[2].ID, Addr: "r2:1", Weight: 1, State: cluster.Ready},
	)
	view := prov.Current()

	// Tail node (rank 2): parent is rank1, grandparent rank0.
	tail := newHedgeEngine(t, prov, ranked[2].ID)
	if p, b := tail.hedgeTargets(ranked, view); p != "r1:1" || b != "r0:1" {
		t.Errorf("tail targets = (%q, %q), want (r1:1, r0:1)", p, b)
	}
	// Middle node (rank 1): its parent is the root, and there is nothing above the
	// root, so there is no distinct backup to race.
	mid := newHedgeEngine(t, prov, ranked[1].ID)
	if p, b := mid.hedgeTargets(ranked, view); p != "r0:1" || b != "" {
		t.Errorf("mid targets = (%q, %q), want (r0:1, \"\")", p, b)
	}
	// Root has no upstream.
	root := newHedgeEngine(t, prov, ranked[0].ID)
	if p, _ := root.hedgeTargets(ranked, view); p != "" {
		t.Errorf("root primary = %q, want empty", p)
	}
	// A non-member asks the owner and has no backup above it.
	outsider := newHedgeEngine(t, prov, "OUTSIDER")
	if p, b := outsider.hedgeTargets(ranked, view); p != "r0:1" || b != "" {
		t.Errorf("outsider targets = (%q, %q), want (r0:1, \"\")", p, b)
	}
}

// TestHedgeTargetsSkipsOpenCircuit: a parent whose circuit is open is routed
// around by walking further up the tree, so a dead branch does not force every
// reader beneath it back to origin.
func TestHedgeTargetsSkipsOpenCircuit(t *testing.T) {
	const chunkKey = uint64(0xD00D)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: "r0:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[1].ID, Addr: "r1:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[2].ID, Addr: "r2:1", Weight: 1, State: cluster.Ready},
	)
	view := prov.Current()

	tail := newHedgeEngine(t, prov, ranked[2].ID)
	brk := peer.NewBreaker(peer.BreakerOptions{FailureThreshold: 1, Cooldown: time.Hour})
	tail.peer.Breaker = brk

	// Healthy: parent r1, backup r0.
	if p, b := tail.hedgeTargets(ranked, view); p != "r1:1" || b != "r0:1" {
		t.Fatalf("healthy targets = (%q, %q), want (r1:1, r0:1)", p, b)
	}

	// Trip the parent's circuit: routing must skip it and promote the grandparent.
	brk.RecordFailure("r1:1")
	if p, b := tail.hedgeTargets(ranked, view); p != "r0:1" || b != "" {
		t.Errorf("with r1 open, targets = (%q, %q), want (r0:1, \"\")", p, b)
	}

	// Every ancestor open: no peer to ask, so the caller falls back to origin.
	brk.RecordFailure("r0:1")
	if p, _ := tail.hedgeTargets(ranked, view); p != "" {
		t.Errorf("with all ancestors open, primary = %q, want empty", p)
	}
}

// TestFromPeerFallsBackWhenCircuitOpen: with no usable ancestor, fromPeer reports
// false so the read goes to origin rather than hanging.
func TestFromPeerFallsBackWhenCircuitOpen(t *testing.T) {
	data := bytes.Repeat([]byte{9}, 16)
	var hits int64
	addr := slowPeer(t, "P", 0, data, &hits)

	const chunkKey = uint64(0xF00D)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: addr, Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[1].ID, Weight: 1, State: cluster.Ready},
	)
	e := newHedgeEngine(t, prov, ranked[1].ID)
	brk := peer.NewBreaker(peer.BreakerOptions{FailureThreshold: 1, Cooldown: time.Hour})
	e.peer.Breaker = brk
	brk.RecordFailure(addr) // the only ancestor is now open

	key := store.BlockKey{Chunk: chunkKey, Block: 0}
	if _, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", "obj", chunkKey, key, 0); ok {
		t.Error("fromPeer should report false when no ancestor is usable")
	}
	if n := atomic.LoadInt64(&hits); n != 0 {
		t.Errorf("peer was dialed %d times despite an open circuit", n)
	}
}

// TestFailoverNotRateLimited is the fix for a node death: when the tree parent is
// definitively gone, the read must escalate to the grandparent even though the
// hedge budget is exhausted. Rate-limiting failover would send the whole subtree
// to origin while a peer that has the block sits idle.
func TestFailoverNotRateLimited(t *testing.T) {
	data := bytes.Repeat([]byte{0x5A}, 16)
	var deadHits, aliveHits int64

	// A peer that refuses connections stands in for a departed node.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := l.Addr().String()
	l.Close()
	aliveAddr := slowPeer(t, "ALIVE", 0, data, &aliveHits)

	const chunkKey = uint64(0xFA110E)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: aliveAddr, Weight: 1, State: cluster.Ready}, // grandparent
		cluster.Member{ID: ranked[1].ID, Addr: deadAddr, Weight: 1, State: cluster.Ready},  // parent, gone
		cluster.Member{ID: ranked[2].ID, Weight: 1, State: cluster.Ready},                  // self
	)

	// HedgeRatio is deliberately tiny: no speculative hedge may be granted, so a
	// successful read proves failover happened on its own budget.
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: ranked[2].ID, Fanout: 1,
		Hedge: true, HedgeRatio: 0.0001,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 32; i++ {
		e.latency.Observe(time.Microsecond)
	}
	// Drain the hedge budget so no token is available.
	for i := 0; i < 50; i++ {
		e.hedges.allow()
	}

	key := store.BlockKey{Chunk: chunkKey, Block: 0}
	got, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", "obj", chunkKey, key, 0)
	if !ok {
		t.Fatal("fromPeer returned false: the read fell through to origin instead of failing over")
	}
	if !bytes.Equal(got, data) {
		t.Errorf("bytes mismatch")
	}
	if atomic.LoadInt64(&aliveHits) == 0 {
		t.Error("the grandparent was never contacted")
	}
	_ = deadHits
}

// TestFailoverDoesNotDoubleLaunch: a hedge already in flight must not be launched
// again when the primary then fails, or the backup would receive duplicates.
func TestFailoverDoesNotDoubleLaunch(t *testing.T) {
	data := bytes.Repeat([]byte{3}, 16)
	var backupHits int64
	// The primary is slow enough that the hedge fires first, then it 404s.
	slowMiss := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		http.Error(w, "not held", http.StatusNotFound)
	}))
	t.Cleanup(slowMiss.Close)
	backupAddr := slowPeer(t, "BACKUP", 0, data, &backupHits)

	const chunkKey = uint64(0xD00B1E)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	ranked := hashring.Rank(chunkKey, nodes)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: ranked[0].ID, Addr: backupAddr, Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[1].ID, Addr: strings.TrimPrefix(slowMiss.URL, "http://"), Weight: 1, State: cluster.Ready},
		cluster.Member{ID: ranked[2].ID, Weight: 1, State: cluster.Ready},
	)
	e := newHedgeEngine(t, prov, ranked[2].ID)

	key := store.BlockKey{Chunk: chunkKey, Block: 0}
	if _, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", "obj", chunkKey, key, 0); !ok {
		t.Fatal("expected the backup to serve the block")
	}
	if n := atomic.LoadInt64(&backupHits); n != 1 {
		t.Errorf("backup received %d requests, want exactly 1", n)
	}
}

// TestHedgeSameAddressNotRaced: when the parent and the backup resolve to the
// same address there is nothing to gain, so no duplicate is sent.
func TestHedgeSameAddressNotRaced(t *testing.T) {
	data := bytes.Repeat([]byte{1}, 16)
	var hits int64
	addr := slowPeer(t, "ONE", 0, data, &hits)

	prov := cluster.NewStaticProvider(cluster.Member{ID: "OWNER", Addr: addr, Weight: 1, State: cluster.Ready})
	e := newHedgeEngine(t, prov, "SELF") // not a member: primary = owner, backup = ""

	oid, _ := chunk.ObjectID("http://origin.invalid/x")
	ck := chunk.ChunkKey("dart", oid, 0)
	key := store.BlockKey{Chunk: ck, Block: 0}
	if _, ok := e.fromPeer(context.Background(), "http://origin.invalid/x", oid, ck, key, 0); !ok {
		t.Fatal("expected the owner to serve the block")
	}
	if n := atomic.LoadInt64(&hits); n != 1 {
		t.Errorf("peer hit %d times, want exactly 1 (no pointless duplicate)", n)
	}
}
