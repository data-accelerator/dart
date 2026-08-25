package engine

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// TestPeerStreamSourceLocalHit: a locally-held block is streamed from the store
// with a known Content-Length.
func TestPeerStreamSourceLocalHit(t *testing.T) {
	content := blob(10)
	origin := countingOrigin(t, content, nil)
	st := openStoreAt(t)
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Warm the local cache.
	if err := e.Serve(context.Background(), io.Discard, origin.URL, 0, int64(len(content)-1)); err != nil {
		t.Fatalf("warm: %v", err)
	}

	srv := httptest.NewServer(&peer.StreamServer{NodeID: "A", Src: e.PeerStreamSource()})
	defer srv.Close()

	var buf bytes.Buffer
	n, held, err := peer.NewClient().Stream(context.Background(), strings.TrimPrefix(srv.URL, "http://"),
		peer.BlockRequest{Key: blockKeyFor(origin.URL)}, &buf)
	if err != nil || !held {
		t.Fatalf("Stream held=%v err=%v", held, err)
	}
	if n != int64(len(content)) || !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("streamed %d bytes, want %d", n, len(content))
	}
}

// TestPeerStreamSourceRootFetchesOrigin: the tree root has no parent, so a
// relay request it cannot serve locally is satisfied from origin and cached.
func TestPeerStreamSourceRootFetchesOrigin(t *testing.T) {
	content := blob(10)
	origin := countingOrigin(t, content, nil)
	st := openStoreAt(t)
	prov := cluster.NewStaticProvider(cluster.Member{ID: "A", Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready})
	e, err := New(Options{
		Chunk: testCfg(), Store: st, Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "A",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(&peer.StreamServer{NodeID: "A", Src: e.PeerStreamSource()})
	defer srv.Close()

	key := blockKeyFor(origin.URL)
	var buf bytes.Buffer
	n, held, err := peer.NewClient().Stream(context.Background(), strings.TrimPrefix(srv.URL, "http://"),
		peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1}, &buf)
	if err != nil || !held {
		t.Fatalf("Stream held=%v err=%v", held, err)
	}
	if !bytes.Equal(buf.Bytes(), content) || n != int64(len(content)) {
		t.Errorf("streamed %d bytes, mismatch", n)
	}
	if !st.Has(key) {
		t.Error("root did not cache the block it fetched from origin")
	}
}

// TestStreamRelayChainThroughEngines builds a 3-node fanout=1 chain of engines
// using the streaming (cut-through) peer servers and requests via the tail: the
// bytes must arrive intact and every node on the chain must end up caching the
// block.
func TestStreamRelayChainThroughEngines(t *testing.T) {
	content := blob(10) // single block
	origin := countingOrigin(t, content, nil)

	ids := []string{"A", "B", "C"}
	stores := map[string]*store.DiskStore{}
	engines := map[string]*Engine{}
	provs := map[string]*cluster.StaticProvider{}
	for _, id := range ids {
		st := openStoreAt(t)
		prov := cluster.NewStaticProvider()
		e, err := New(Options{
			Chunk: testCfg(), Store: st, Fetcher: newFetcher(),
			Cluster: prov, Peer: peer.NewClient(), SelfID: id, Fanout: 1,
		})
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}
		stores[id], engines[id], provs[id] = st, e, prov
	}

	members := make([]cluster.Member, 0, len(ids))
	for _, id := range ids {
		srv := httptest.NewServer(&peer.StreamServer{NodeID: id, Src: engines[id].PeerStreamSource()})
		t.Cleanup(srv.Close)
		members = append(members, cluster.Member{
			ID: id, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, State: cluster.Ready,
		})
	}
	for _, id := range ids {
		provs[id].Set(members)
	}

	key := blockKeyFor(origin.URL)
	ranked := hashring.Rank(key.Chunk, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}})
	tail := ranked[2].ID

	var buf bytes.Buffer
	if err := engines[tail].Serve(context.Background(), &buf, origin.URL, 0, int64(len(content)-1)); err != nil {
		t.Fatalf("tail Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("tail bytes mismatch")
	}
	for i, n := range ranked {
		if !stores[n.ID].Has(key) {
			t.Errorf("rank %d node %s missing block after streaming relay chain", i, n.ID)
		}
	}
}

// TestStreamRelayRejectsWrongLengthBlock pins issue #1: a cut-through relay
// must validate the streamed length against the object geometry before
// caching. Relay responses carry no Content-Length, so a short clean chunked
// EOF is indistinguishable from success at the transport; caching such a block
// would poison the write-once cache permanently and propagate downstream. The
// relayed bytes are still served on (the leaf's own geometry check protects
// clients), but a wrongly-sized block must never enter the cache.
func TestStreamRelayRejectsWrongLengthBlock(t *testing.T) {
	content := blob(16) // exactly one block
	var originCnt int64
	origin := countingOrigin(t, content, &originCnt)

	// Broken upstream: short-writes half a block on a chunked response (never
	// calls sizer) and reports success — the StreamSource contract's letter
	// permits it ("write exactly n bytes when returning n").
	short := peer.StreamSource(func(_ context.Context, req peer.BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
		n, err := w.Write(content[:8])
		return int64(n), true, err
	})
	rootSrv := httptest.NewServer(&peer.StreamServer{NodeID: "A", Src: short})
	defer rootSrv.Close()
	rootAddr := strings.TrimPrefix(rootSrv.URL, "http://")

	// Middle node B: real engine whose only cluster member is A, so A is
	// deterministically B's upstream (B is not a member; the owner is).
	st := openStoreAt(t)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: "A", Addr: rootAddr, Weight: 1, State: cluster.Ready},
	)
	e, err := New(Options{
		Chunk: testCfg(), Store: st, Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "B", Fanout: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(&peer.StreamServer{NodeID: "B", Src: e.PeerStreamSource()})
	defer srv.Close()

	key := blockKeyFor(origin.URL)
	var buf bytes.Buffer
	n, held, err := peer.NewClient().Stream(context.Background(), strings.TrimPrefix(srv.URL, "http://"),
		peer.BlockRequest{Key: key, URL: origin.URL}, &buf)
	if err != nil || !held {
		t.Fatalf("Stream held=%v err=%v", held, err)
	}
	if n != 8 {
		t.Fatalf("downstream received %d bytes, want the 8 the broken parent sent (served on)", n)
	}
	if st.Has(key) {
		t.Error("wrongly-sized relayed block was cached (issue #1)")
	}

	// A correct relay of the same block is cached: sanity that the geometry
	// check admits exactly the expected length.
	full := peer.StreamSource(func(_ context.Context, req peer.BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
		sizer(int64(len(content)))
		n, err := w.Write(content)
		return int64(n), true, err
	})
	fullSrv := httptest.NewServer(&peer.StreamServer{NodeID: "A", Src: full})
	defer fullSrv.Close()
	prov.Set([]cluster.Member{
		{ID: "A", Addr: strings.TrimPrefix(fullSrv.URL, "http://"), Weight: 1, State: cluster.Ready},
	})

	buf.Reset()
	n, held, err = peer.NewClient().Stream(context.Background(), strings.TrimPrefix(srv.URL, "http://"),
		peer.BlockRequest{Key: key, URL: origin.URL}, &buf)
	if err != nil || !held || n != 16 {
		t.Fatalf("correct relay: n=%d held=%v err=%v", n, held, err)
	}
	if got, ok, _ := st.Get(key); !ok || !bytes.Equal(got, content) {
		t.Errorf("correct relay not cached: ok=%v len=%d", ok, len(got))
	}
}

// TestStreamRelayDoesNotCacheWhenSizeUnresolved pins the companion guarantee
// of issue #1: when the object size cannot be resolved, relayed bytes cannot
// be validated and must not be cached (they are still served on).
func TestStreamRelayDoesNotCacheWhenSizeUnresolved(t *testing.T) {
	content := blob(16)
	// Origin that never answers Size's probe successfully.
	origin := httptest.NewServer(http.NotFoundHandler())
	origin.Close() // connection refused

	full := peer.StreamSource(func(_ context.Context, req peer.BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
		sizer(int64(len(content)))
		n, err := w.Write(content)
		return int64(n), true, err
	})
	rootSrv := httptest.NewServer(&peer.StreamServer{NodeID: "A", Src: full})
	defer rootSrv.Close()

	st := openStoreAt(t)
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: "A", Addr: strings.TrimPrefix(rootSrv.URL, "http://"), Weight: 1, State: cluster.Ready},
	)
	e, err := New(Options{
		Chunk: testCfg(), Store: st, Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "B", Fanout: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(&peer.StreamServer{NodeID: "B", Src: e.PeerStreamSource()})
	defer srv.Close()

	var buf bytes.Buffer
	n, held, err := peer.NewClient().Stream(context.Background(), strings.TrimPrefix(srv.URL, "http://"),
		peer.BlockRequest{Key: blockKeyFor(origin.URL), URL: origin.URL}, &buf)
	if err != nil || !held || n != 16 {
		t.Fatalf("Stream n=%d held=%v err=%v (bytes must still be served on)", n, held, err)
	}
	if st.Has(blockKeyFor(origin.URL)) {
		t.Error("relayed block cached although its size could not be validated")
	}
}
