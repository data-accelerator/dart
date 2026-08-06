package engine

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

func openStoreAt(t *testing.T) *store.DiskStore {
	t.Helper()
	s, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "b.dat"), SlotSize: testCfg().BlockSize, Slots: 1024})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newFetcher() fetch.Fetcher { return &fetch.Coalescing{F: &fetch.HTTPFetcher{}} }

// startWarmPeer builds a peer node whose cache is warmed with the whole object
// from origin, then exposes it via a peer.Server. Returns its host:port.
func startWarmPeer(t *testing.T, content []byte, originURL string) string {
	t.Helper()
	ps := openStoreAt(t)
	pe, err := New(Options{Chunk: testCfg(), Store: ps, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("peer engine: %v", err)
	}
	if err := pe.Serve(context.Background(), io.Discard, originURL, 0, int64(len(content)-1)); err != nil {
		t.Fatalf("warm peer: %v", err)
	}
	srv := httptest.NewServer(&peer.Server{NodeID: "P", Src: peer.StoreSource(ps)})
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// TestEngineP2PPeerHit: with a warmed peer as the owner, a serve pulls every
// block from the peer; origin is touched only by the one size probe.
func TestEngineP2PPeerHit(t *testing.T) {
	content := blob(300)
	var cnt int64
	origin := countingOrigin(t, content, &cnt)
	peerAddr := startWarmPeer(t, content, origin.URL)
	atomic.StoreInt64(&cnt, 0) // reset after warming the peer

	prov := cluster.NewStaticProvider(cluster.Member{ID: "P", Addr: peerAddr, Weight: 1, State: cluster.Ready})
	ce, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "S",
	})
	if err != nil {
		t.Fatalf("client engine: %v", err)
	}

	var buf bytes.Buffer
	if err := ce.Serve(context.Background(), &buf, origin.URL, 0, 299); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("bytes mismatch (%d)", buf.Len())
	}
	if got := atomic.LoadInt64(&cnt); got != 1 {
		t.Errorf("origin requests = %d, want 1 (size probe only; blocks from peer)", got)
	}
}

// TestEngineP2PPeerMissFallsBackToOrigin: an empty peer returns 404, so the
// engine falls back to origin and still serves correct bytes.
func TestEngineP2PPeerMissFallsBackToOrigin(t *testing.T) {
	content := blob(100)
	var cnt int64
	origin := countingOrigin(t, content, &cnt)

	ps := openStoreAt(t) // empty peer cache
	psrv := httptest.NewServer(&peer.Server{NodeID: "P", Src: peer.StoreSource(ps)})
	defer psrv.Close()
	peerAddr := strings.TrimPrefix(psrv.URL, "http://")

	prov := cluster.NewStaticProvider(cluster.Member{ID: "P", Addr: peerAddr, Weight: 1, State: cluster.Ready})
	ce, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "S",
	})
	if err != nil {
		t.Fatalf("client engine: %v", err)
	}

	var buf bytes.Buffer
	if err := ce.Serve(context.Background(), &buf, origin.URL, 0, 99); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("bytes mismatch")
	}
	// Peer missed, so origin served the blocks: more than just the size probe.
	if got := atomic.LoadInt64(&cnt); got <= 1 {
		t.Errorf("origin requests = %d, want > 1 (peer miss -> origin fallback)", got)
	}
}

// TestEngineP2PSelfOwnerUsesOrigin: when this node is itself the owner, it does
// not consult peers (the bogus peer addr is never contacted) and fetches origin.
func TestEngineP2PSelfOwnerUsesOrigin(t *testing.T) {
	content := blob(100)
	var cnt int64
	origin := countingOrigin(t, content, &cnt)

	// Only self is Ready, so self is always the top-ranked candidate.
	prov := cluster.NewStaticProvider(cluster.Member{ID: "S", Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready})
	ce, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "S",
	})
	if err != nil {
		t.Fatalf("client engine: %v", err)
	}

	var buf bytes.Buffer
	if err := ce.Serve(context.Background(), &buf, origin.URL, 0, 99); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("bytes mismatch")
	}
	if got := atomic.LoadInt64(&cnt); got <= 1 {
		t.Errorf("origin requests = %d, want > 1 (self is owner -> origin)", got)
	}
}
