package engine

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/data-accelerator/dart/internal/peer"
)

func TestRootFanoutOptions(t *testing.T) {
	for _, tc := range []struct{ root, fanout, wantRoot, wantFanout int }{
		{0, 0, 1, 2}, {0, 4, 1, 4}, {1, 2, 1, 2}, {4, 4, 4, 4}, {3, 1, 3, 1},
	} {
		e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(), RootFanout: tc.root, Fanout: tc.fanout})
		if err != nil {
			t.Fatal(err)
		}
		if e.rootFanout != tc.wantRoot || e.fanout != tc.wantFanout {
			t.Fatal("incorrect default", tc, e.rootFanout, e.fanout)
		}
	}
	if _, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(), RootFanout: -1}); err == nil {
		t.Fatal("negative root fanout accepted")
	}
}

// Real HTTP payload counts distinguish one root child from a uniform tree.
// Metadata is preloaded so this test measures block distribution only.
func TestRootFanoutHTTPDistribution(t *testing.T) {
	for _, rootK := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint(rootK), func(t *testing.T) {
			const url = "http://origin.invalid/root-fanout"
			content := blob(16)
			key := blockKeyFor(url)
			oid, _ := chunk.ObjectID(url)
			provider := cluster.NewStaticProvider()
			forbidden := &forbiddenOrigin{}
			engines := make([]*Engine, 8)
			var servers []*httptest.Server
			var sent [8]atomic.Int64
			var members []cluster.Member
			for i := range engines {
				role := cluster.NonRoot
				if i == 0 {
					role = cluster.RootOnly
				}
				id := fmt.Sprint("node-", i)
				st := openStoreAt(t)
				if i == 0 {
					if err := st.Put(key, content); err != nil {
						t.Fatal(err)
					}
				}
				e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: forbidden, Cluster: provider,
					Peer: peer.NewClient(), SelfID: id, Role: role, Fanout: 2, RootFanout: rootK, Hedge: false})
				if err != nil {
					t.Fatal(err)
				}
				e.sizes[fetch.OriginKey(url)] = sizeMeta{size: 16}
				engines[i] = e
				h := &peer.Server{Borrow: e.PeerLeaseSource()}
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					h.ServeHTTP(countedPeerWriter{w, &sent[i]}, r)
				}))
				t.Cleanup(srv.Close)
				servers = append(servers, srv)
				members = append(members, cluster.Member{ID: id, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, State: cluster.Ready, Role: role})
			}
			provider.Set(members)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 7)
			for _, e := range engines[1:] {
				go func(e *Engine) {
					l, err := e.borrowBlock(ctx, url, oid, 16, 0, 0)
					if err == nil && !bytes.Equal(l.Bytes(), content) {
						err = fmt.Errorf("wrong bytes")
					}
					l.Release()
					done <- err
				}(e)
			}
			// Join every reader before any server or store cleanup, even on error.
			for range engines[1:] {
				if err := <-done; err != nil {
					t.Error(err)
				}
			}
			// A client can finish before the sender's Write wrapper records its
			// return value. Join HTTP handlers before inspecting wire counters.
			for _, srv := range servers {
				srv.Close()
			}
			order, _ := engines[1].treeNodes(ctx, provider.Current(), oid, engines[1].fileKey(oid), key.Chunk)
			if rootK == 0 {
				rootK = 1
			}
			for i, e := range engines {
				pos := -1
				for j, n := range order {
					if n.ID == e.selfID {
						pos = j
					}
				}
				want := int64(len(hashring.ChildrenWithRootFanout(pos, 8, 2, rootK)) * 16)
				if sent[i].Load() != want {
					t.Errorf("%s sent %d payload bytes, want %d", e.selfID, sent[i].Load(), want)
				}
			}
			if sent[0].Load() != int64(rootK*16) || forbidden.calls.Load() != 0 {
				t.Fatal("wrong root transfers or origin access", sent[0].Load(), forbidden.calls.Load())
			}
		})
	}
}
