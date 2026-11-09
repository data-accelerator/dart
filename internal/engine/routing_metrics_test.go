package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
)

func TestPeerFetchTargetMetrics(t *testing.T) {
	// The first three nodes form a chain above self. A failed parent and an
	// unavailable ancestor lead to a terminal root, with or without the parent
	// initially excluded by the breaker.
	for _, skipParent := range []bool{false, true} {
		t.Run(fmt.Sprint(skipParent), func(t *testing.T) {
			const url = "http://origin.invalid/target-metrics"
			key := blockKeyFor(url)
			oid, _ := chunk.ObjectID(url)
			order := hashring.Rank(key.Chunk, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}})
			var parentCalls, ancestorCalls, rootCalls atomic.Int32
			parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { parentCalls.Add(1); w.WriteHeader(404) }))
			defer parent.Close()
			ancestor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ancestorCalls.Add(1); w.WriteHeader(404) }))
			defer ancestor.Close()
			content := blob(16)
			root := httptest.NewServer(&peer.Server{Src: func(_ context.Context, req peer.BlockRequest) ([]byte, bool, error) {
				rootCalls.Add(1)
				if !req.Supply {
					t.Error("root attempt was not terminal")
				}
				return content, true, nil
			}})
			defer root.Close()
			p := cluster.NewStaticProvider(
				cluster.Member{ID: "R", Addr: root.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly},
				cluster.Member{ID: order[0].ID, Addr: ancestor.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.NonRoot},
				cluster.Member{ID: order[1].ID, Addr: parent.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.NonRoot},
				cluster.Member{ID: order[2].ID, Addr: "unused:1", Weight: 1, State: cluster.Ready, Role: cluster.NonRoot})
			pc := peer.NewClient()
			pc.Breaker = peer.NewBreaker(peer.BreakerOptions{})
			if skipParent {
				pc.Breaker.RecordHardFailure(parent.Listener.Addr().String())
			}
			mx := NewMetrics(metrics.NewRegistry())
			e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: &forbiddenOrigin{}, Cluster: p, Peer: pc, SelfID: order[2].ID, Role: cluster.NonRoot, Fanout: 1, Metrics: mx})
			if err != nil {
				t.Fatal(err)
			}
			l, err := e.borrowBlock(context.Background(), url, oid, 16, 0, 0)
			defer l.Release()
			if err != nil || !bytes.Equal(l.Bytes(), content) {
				t.Fatal(err)
			}
			wantParent := uint64(1)
			if skipParent {
				wantParent = 0
			}
			if mx.peerFetchRoot.Value() != 1 || mx.peerFetchParent.Value() != wantParent || mx.peerFetchAncestor.Value() != 1 || rootCalls.Load() != 1 || uint64(parentCalls.Load()) != wantParent || ancestorCalls.Load() != 1 {
				t.Fatal("target counts", mx.peerFetchRoot.Value(), mx.peerFetchParent.Value(), mx.peerFetchAncestor.Value(), rootCalls.Load(), parentCalls.Load(), ancestorCalls.Load())
			}
		})
	}
}
