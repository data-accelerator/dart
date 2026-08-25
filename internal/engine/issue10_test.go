package engine

// Regression tests for issue #10 (engine bundle). Each test names the item it
// pins (E2, E4, E5, E6, E7, E8) and inverts the audit's scratch repro.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/tracker"
)

// tailEngine builds a 2-node fanout=1 setup where the engine under test is the
// tree tail (its parent is the other member, at parentSrv).
func tailEngine(t *testing.T, cli *peer.Client, parentSrv *httptest.Server, url string) (*Engine, string) {
	t.Helper()
	key := blockKeyFor(url)
	ranked := hashring.Rank(key.Chunk, []hashring.Node{{ID: "P", Weight: 1}, {ID: "S", Weight: 1}})
	parentID, tailID := ranked[0].ID, ranked[1].ID
	parentAddr := strings.TrimPrefix(parentSrv.URL, "http://")
	var members []cluster.Member
	for _, id := range []string{"P", "S"} {
		addr := "127.0.0.1:1"
		if id == parentID {
			addr = parentAddr
		}
		if id == tailID {
			addr = "127.0.0.1:2" // tail's own addr is never dialed
		}
		members = append(members, cluster.Member{ID: id, Addr: addr, Weight: 1, State: cluster.Ready})
	}
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: cluster.NewStaticProvider(members...), Peer: cli, SelfID: tailID, Fanout: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e, tailID
}

// TestPeerSourceDeclinesRangeBlindOrigin pins E2: the buffered relay source
// used to go from store miss straight to fetching blocks — a Range-blind
// origin cost one whole-object GET per block, and the fragments were cached.
// It must decline like the streaming source does.
func TestPeerSourceDeclinesRangeBlindOrigin(t *testing.T) {
	content := blob(100)
	var reqs int64
	origin := rangeBlindOrigin(t, content, &reqs)

	st := openStoreAt(t)
	e, err := New(Options{
		Chunk: testCfg(), Store: st, Fetcher: newFetcher(),
		Cluster: cluster.NewStaticProvider(cluster.Member{ID: "A", Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready}),
		Peer:    peer.NewClient(), SelfID: "A",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	key := blockKeyFor(origin.URL)
	_, held, err := e.PeerSource()(context.Background(),
		peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1})
	if err != nil || held {
		t.Fatalf("PeerSource: held=%v err=%v, want a decline", held, err)
	}
	if st.Has(key) {
		t.Fatal("a fragment of a Range-blind object must not be cached")
	}
	if got := atomic.LoadInt64(&reqs); got > 2 {
		t.Fatalf("origin requests = %d, want at most the size probe(s) — no whole-object pulls", got)
	}
}

// TestBlockDoesNotCacheRangeIgnored pins E2's defense in depth: even when the
// RangeUnsupported marker is absent, block() must not cache bytes sliced out
// of a whole-object 200.
func TestBlockDoesNotCacheRangeIgnored(t *testing.T) {
	content := blob(100)
	origin := rangeBlindOrigin(t, content, nil)

	st := openStoreAt(t)
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	oid, _ := chunk.ObjectID(origin.URL)
	data, err := e.block(context.Background(), origin.URL, oid, int64(len(content)), 0, 0)
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	if len(data) != int(testCfg().BlockSize) {
		t.Fatalf("block served %d bytes, want %d", len(data), testCfg().BlockSize)
	}
	if st.Has(blockKeyFor(origin.URL)) {
		t.Fatal("bytes from a Range-ignored response must not be cached")
	}
}

// TestHedgeWinMetricsRequireFiredHedge pins E4: hedge win counters used to
// increment on every successful peer fetch — including with hedging disabled —
// making the prescribed backup_won/fired comparison uninterpretable exactly
// when an operator consults it.
func TestHedgeWinMetricsRequireFiredHedge(t *testing.T) {
	content := blob(48) // 3 blocks
	var cnt int64
	origin := countingOrigin(t, content, &cnt)
	peerAddr := startWarmPeer(t, content, origin.URL)

	reg := metrics.NewRegistry()
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: cluster.NewStaticProvider(cluster.Member{ID: "P", Addr: peerAddr, Weight: 1, State: cluster.Ready}),
		Peer:    peer.NewClient(), SelfID: "S",
		Metrics: NewMetrics(reg), // hedging disabled by default
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Serve(context.Background(), io.Discard, origin.URL, 0, int64(len(content)-1)); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if e.mx.hedgeWonPrim.Value() != 0 || e.mx.hedgeWonBackup.Value() != 0 {
		t.Fatalf("hedge wins recorded with no hedge fired: primary=%d backup=%d",
			e.mx.hedgeWonPrim.Value(), e.mx.hedgeWonBackup.Value())
	}
}

// TestMissesDoNotFeedLatencyEstimator pins E5: 16 fast 404 misses used to arm
// the hedge delay at the floor, spending the hedge budget on noise. Only
// held=true answers may feed the estimate.
func TestMissesDoNotFeedLatencyEstimator(t *testing.T) {
	ps := openStoreAt(t) // empty peer: every request 404s
	psrv := httptest.NewServer(&peer.Server{NodeID: "P", Src: peer.StoreSource(ps)})
	defer psrv.Close()
	addr := strings.TrimPrefix(psrv.URL, "http://")

	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: cluster.NewStaticProvider(), Peer: peer.NewClient(), SelfID: "S",
		Hedge: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := peer.BlockRequest{Key: blockKeyFor("http://x/y"), URL: "http://x/y", Hop: 1}
	for i := 0; i < 16; i++ {
		if _, held := e.fetchHedged(context.Background(), addr, "", req); held {
			t.Fatal("empty peer held the block?")
		}
	}
	if d, ok := e.hedgeDelay(); ok {
		t.Fatalf("hedgeDelay armed at %v from pure misses; misses must not feed the estimator", d)
	}
}

// TestReaderSetCacheFollowsGrantedLease pins E6: the reader-set cache used a
// hardcoded 2s TTL and never read the granted lease, so a tracker configured
// below 2s dropped a still-reading node from the frozen set. The cache period
// must follow the granted lease (half of it), so renewal lands before lapse.
func TestReaderSetCacheFollowsGrantedLease(t *testing.T) {
	reg := tracker.NewRegistry(tracker.Options{Tick: 20 * time.Millisecond, LeaseTTL: 80 * time.Millisecond})
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster:       cluster.NewStaticProvider(cluster.Member{ID: "S", Addr: "127.0.0.1:2", Weight: 1, State: cluster.Ready}),
		SelfID:        "S",
		TrackerClient: tracker.NewClient(), TrackerRegistry: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	view := e.cluster.Current()
	fk := e.fileKey("obj")

	// First call JOINs and caches for half the granted lease (40ms).
	e.readers(context.Background(), view, "obj", fk)
	// Still cached 20ms later (a fixed 2s TTL would also be cached — but the
	// point is the next step).
	time.Sleep(20 * time.Millisecond)
	e.readers(context.Background(), view, "obj", fk)
	// 60ms later the cache (40ms) must have expired: the call re-JOINs, which
	// refreshes our 80ms lease. Under the old fixed 2s TTL this re-JOIN would
	// not happen until 2s had passed — 25 lease periods dropped.
	time.Sleep(60 * time.Millisecond)
	e.readers(context.Background(), view, "obj", fk)

	readers, _ := reg.Readers("obj")
	// Let the tick publish the frozen set, then verify we are still a reader —
	// i.e. renewals kept pace with the lease.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		e.readers(context.Background(), view, "obj", fk) // renew
		readers, _ = reg.Readers("obj")
		if len(readers) == 1 && readers[0] == "S" {
			return // renewed in time: the lease never lapsed
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("node dropped from the reader set while still reading (readers=%v)", readers)
}

// TestStreamRelayParentErrorFallsBackToOrigin pins E7: with the parent's
// circuit open, the streaming relay used to propagate a 500 ("circuit open")
// with zero origin contact, while the buffered Get path routed around it. The
// relay must skip an unhealthy parent and serve from origin.
func TestStreamRelayParentErrorFallsBackToOrigin(t *testing.T) {
	content := blob(32)
	var originCnt int64
	origin := countingOrigin(t, content, &originCnt)

	psrv := httptest.NewServer(&peer.StreamServer{NodeID: "P", Src: func(_ context.Context, _ peer.BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
		sizer(16)
		n, _ := w.Write(content[:16])
		return int64(n), true, nil
	}})
	defer psrv.Close()
	parentAddr := strings.TrimPrefix(psrv.URL, "http://")

	cli := peer.NewClient()
	cli.Breaker = peer.NewBreaker(peer.BreakerOptions{})
	for i := 0; i < 5; i++ { // DefaultFailureThreshold
		cli.Breaker.RecordHardFailure(parentAddr)
	}

	e, _ := tailEngine(t, cli, psrv, origin.URL)
	key := blockKeyFor(origin.URL)

	var w bytes.Buffer
	n, held, err := e.PeerStreamSource()(context.Background(),
		peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1}, &w, func(int64) {})
	if err != nil || !held {
		t.Fatalf("relay with an open-circuit parent: n=%d held=%v err=%v, want origin fallback", n, held, err)
	}
	if atomic.LoadInt64(&originCnt) == 0 {
		t.Fatal("origin was never contacted; the open circuit must be skipped, not propagated")
	}
	if !bytes.Equal(w.Bytes(), content[:16]) {
		t.Fatalf("served %d wrong bytes", w.Len())
	}
}

// TestEmptyObjectGetServes200 pins E8: a plain GET of a zero-length object
// used to return 416 (RFC 7233 defines 416 only for Range requests). It is a
// valid empty 200 — on the block path; a Range-blind origin is proxied
// verbatim (passthrough), also 200.
func TestEmptyObjectGetServes200(t *testing.T) {
	t.Run("block path (416-style probe)", func(t *testing.T) {
		// A fetcher whose probe answers 416: Size reads that as an empty
		// object (bytes=0-0 is satisfiable for any non-empty object).
		e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: emptyFetcher{}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		h := NewStaticHandler(e, "http://origin/empty")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/empty", nil))
		if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
			t.Fatalf("GET empty object: status=%d body=%d bytes, want 200 empty", rec.Code, rec.Body.Len())
		}
		if got := rec.Header().Get("Content-Length"); got != "0" {
			t.Fatalf("Content-Length = %q, want 0", got)
		}

		// A Range request on an empty object stays 416 (RFC 7233).
		rec3 := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/empty", nil)
		req.Header.Set("Range", "bytes=0-0")
		h.ServeHTTP(rec3, req)
		if rec3.Code != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("Range on empty object: status=%d, want 416", rec3.Code)
		}
	})

	t.Run("range-blind origin (verbatim passthrough)", func(t *testing.T) {
		origin := countingOrigin(t, []byte{}, nil) // ServeContent, 0 bytes
		e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		h := NewStaticHandler(e, origin.URL)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/empty", nil))
		if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
			t.Fatalf("GET empty object via real fetcher: status=%d, want 200 empty", rec.Code)
		}
	})
}

// emptyFetcher answers the size probe the way an RFC-compliant origin answers
// bytes=0-0 on an empty object: 416.
type emptyFetcher struct{}

func (emptyFetcher) Fetch(_ context.Context, url string, start, end int64) (fetch.Range, error) {
	return fetch.Range{}, &fetch.StatusError{Code: http.StatusRequestedRangeNotSatisfiable, URL: url}
}
