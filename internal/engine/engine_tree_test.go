package engine

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// blockKeyFor computes the store key for block 0 of the object at url under the
// default namespace, matching what the engine computes internally.
func blockKeyFor(url string) store.BlockKey {
	oid, _ := chunk.ObjectID(url)
	return store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, 0), Block: 0}
}

// TestPeerSourceRelayFetchOnBehalf: a peer.Server backed by PeerSource fetches a
// block it does not hold (via origin, since it is the sole owner) and serves it.
func TestPeerSourceRelayFetchOnBehalf(t *testing.T) {
	content := blob(10) // < blockSize (16) => single block
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
	srv := httptest.NewServer(&peer.Server{NodeID: "A", Src: e.PeerSource()})
	defer srv.Close()

	c := peer.NewClient()
	req := peer.BlockRequest{Key: blockKeyFor(origin.URL), URL: origin.URL, Hop: 1}
	data, held, err := c.Get(context.Background(), strings.TrimPrefix(srv.URL, "http://"), req)
	if err != nil || !held {
		t.Fatalf("relay Get held=%v err=%v", held, err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("relayed block bytes mismatch")
	}
}

// TestTreeMultiHopRelay wires three P2P nodes with fanout=1 (so the preorder
// tree is a chain r0 <- r1 <- r2) and requests via the tail node. The block must
// flow owner(origin) -> r1 -> r2 through relays, populating every node's cache.
func TestTreeMultiHopRelay(t *testing.T) {
	content := blob(10) // single block
	var cnt int64
	origin := countingOrigin(t, content, &cnt)

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

	// Start each node's peer server (relay-capable) and collect addresses.
	members := make([]cluster.Member, 0, len(ids))
	for _, id := range ids {
		srv := httptest.NewServer(&peer.Server{NodeID: id, Src: engines[id].PeerSource()})
		t.Cleanup(srv.Close)
		members = append(members, cluster.Member{
			ID: id, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, State: cluster.Ready,
		})
	}
	for _, id := range ids {
		provs[id].Set(members)
	}

	// Determine the chain order for this object's chunk.
	key := blockKeyFor(origin.URL)
	ranked := hashring.Rank(key.Chunk, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}})
	tail := ranked[2].ID

	// Request via the tail node; it must reach the owner through the relays.
	var buf bytes.Buffer
	if err := engines[tail].Serve(context.Background(), &buf, origin.URL, 0, int64(len(content)-1)); err != nil {
		t.Fatalf("tail Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("tail bytes mismatch")
	}

	// Every node on the chain must now hold the block (relay populated them).
	for i, n := range ranked {
		if !stores[n.ID].Has(key) {
			t.Errorf("rank %d node %s missing block after relay chain", i, n.ID)
		}
	}
}
