package engine

import (
	"bytes"
	"context"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestWholeBlockRelayChainThroughEngines builds a 3-node fanout=1 chain of engines
// using complete-block peer servers and requests via the tail: the
// bytes must arrive intact and every node on the chain must end up caching the
// block.
func TestWholeBlockRelayChainThroughEngines(t *testing.T) {
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
		srv := httptest.NewServer(&peer.Server{NodeID: id, Src: engines[id].PeerSource()})
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
			t.Errorf("rank %d node %s missing block after whole-block relay chain", i, n.ID)
		}
	}
}
