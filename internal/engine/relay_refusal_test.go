package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// TestRelayUpstreamRefusalDoesNotBlamePeer is the fix for a misattribution: a
// relay fetches on the requester's behalf using the requester's own upstream URL.
// When that URL is a presigned link whose signature has expired, the origin
// answers 403 through no fault of the relay — yet the requester used to record a
// soft failure against it, so a handful of blocks would eject a healthy peer.
func TestRelayUpstreamRefusalDoesNotBlamePeer(t *testing.T) {
	// An origin that refuses the (notionally expired) signature.
	var originHits int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&originHits, 1)
		http.Error(w, "signature expired", http.StatusForbidden)
	}))
	defer origin.Close()

	// A relay node with an empty cache: it must fetch on behalf, and fail.
	relayProv := cluster.NewStaticProvider(
		cluster.Member{ID: "R", Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready})
	relayEngine, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: relayProv, Peer: peer.NewClient(), SelfID: "R",
	})
	if err != nil {
		t.Fatalf("New relay: %v", err)
	}
	relaySrv := httptest.NewServer(&peer.StreamServer{NodeID: "R", Src: relayEngine.PeerStreamSource()})
	defer relaySrv.Close()
	relayAddr := strings.TrimPrefix(relaySrv.URL, "http://")

	// The requester asks the relay for a block, supplying the refused URL.
	brk := peer.NewBreaker(peer.BreakerOptions{FailureThreshold: 3})
	c := peer.NewClient()
	c.Breaker = brk

	oid, _ := chunk.ObjectID(origin.URL + "/blob")
	key := store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, 0), Block: 0}
	req := peer.BlockRequest{Key: key, URL: origin.URL + "/blob?Signature=expired", Hop: 1}

	for i := 0; i < 5; i++ {
		var buf bytes.Buffer
		_, held, err := c.Stream(context.Background(), relayAddr, req, &buf)
		if held {
			t.Fatalf("attempt %d: relay claimed to hold the block", i)
		}
		if err == nil {
			t.Fatalf("attempt %d: expected an error", i)
		}
		if !strings.Contains(err.Error(), "upstream") {
			t.Errorf("attempt %d: error %v does not identify an upstream refusal", i, err)
		}
	}

	// The relay is healthy: its circuit must still be closed after 5 refusals,
	// even though the failure threshold is 3.
	if got := brk.State(relayAddr); got != peer.BreakerClosed {
		t.Errorf("relay circuit = %v after 5 upstream refusals, want closed "+
			"(an expired client credential is not the relay's fault)", got)
	}
	if atomic.LoadInt64(&originHits) == 0 {
		t.Error("the relay never attempted the origin fetch")
	}
}

// TestRelayGenuineFailureStillBlamesPeer is the control: a relay that fails for
// its own reasons must still be penalized, or the breaker would never fire.
func TestRelayGenuineFailureStillBlamesPeer(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal", http.StatusInternalServerError)
	}))
	defer broken.Close()
	addr := strings.TrimPrefix(broken.URL, "http://")

	brk := peer.NewBreaker(peer.BreakerOptions{FailureThreshold: 3})
	c := peer.NewClient()
	c.Breaker = brk

	req := peer.BlockRequest{Key: store.BlockKey{Chunk: 1, Block: 0}}
	for i := 0; i < 3; i++ {
		var buf bytes.Buffer
		_, _, _ = c.Stream(context.Background(), addr, req, &buf)
	}
	if got := brk.State(addr); got != peer.BreakerOpen {
		t.Errorf("circuit = %v after 3 genuine 500s, want open", got)
	}
}

// TestRelayRefusalPropagatesStatus checks the wire contract: the relay reports the
// origin's status in a header so the requester can classify without guessing.
func TestRelayRefusalPropagatesStatus(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer origin.Close()

	prov := cluster.NewStaticProvider(
		cluster.Member{ID: "R", Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready})
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "R",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(&peer.StreamServer{NodeID: "R", Src: e.PeerStreamSource()})
	defer srv.Close()

	oid, _ := chunk.ObjectID(origin.URL + "/b")
	key := store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, 0), Block: 0}
	url := srv.URL + "/peer/v1/block/" +
		strings.TrimPrefix(hexOf(key.Chunk), "0x") + "/0"

	hreq, _ := http.NewRequest(http.MethodGet, url, nil)
	hreq.Header.Set(peer.HeaderOrigin, origin.URL+"/b?Signature=expired")
	hreq.Header.Set(peer.HeaderHop, "1")
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get(peer.HeaderUpstreamStatus); got != "401" {
		t.Errorf("%s = %q, want 401", peer.HeaderUpstreamStatus, got)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 (relay ok, upstream refused)", resp.StatusCode)
	}
}

// hexOf formats a chunk key the way the peer path expects.
func hexOf(v uint64) string {
	const digits = "0123456789abcdef"
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{digits[v&0xF]}, b...)
		v >>= 4
	}
	return string(b)
}
