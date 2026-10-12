package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

func TestRelayDeclinesInconsistentKeyURL(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, warm := range []bool{false, true} {
			name := "buffered/cold"
			if streaming {
				name = "stream/cold"
			}
			if warm {
				name = strings.Replace(name, "cold", "warm", 1)
			}
			t.Run(name, func(t *testing.T) {
				content := blob(16)
				var parentCalls, originCalls, unrelatedCalls int64
				origin := countingOrigin(t, content, &originCalls)
				unrelated := countingOrigin(t, blob(8), &unrelatedCalls)
				key := blockKeyFor(origin.URL)
				parentStore := openStoreAt(t)
				if err := parentStore.Put(key, content); err != nil {
					t.Fatal(err)
				}
				parentSource := &peer.StreamServer{Src: peer.StoreStreamSource(parentStore)}
				parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					atomic.AddInt64(&parentCalls, 1)
					parentSource.ServeHTTP(w, r)
				}))
				defer parent.Close()
				client := peer.NewClient()
				client.Breaker = peer.NewBreaker(peer.BreakerOptions{Cooldown: time.Hour})
				e, _ := tailEngine(t, client, parent, origin.URL)
				if warm {
					if err := e.store.Put(key, content); err != nil {
						t.Fatal(err)
					}
				}
				var handler http.Handler = &peer.Server{Src: e.PeerSource()}
				if streaming {
					handler = &peer.StreamServer{Src: e.PeerStreamSource()}
				}
				relay := httptest.NewServer(handler)
				defer relay.Close()
				caller := peer.NewClient()
				caller.Breaker = peer.NewBreaker(peer.BreakerOptions{})
				addr := strings.TrimPrefix(relay.URL, "http://")
				wrongBlock := key
				wrongBlock.Block = uint64(testCfg().BlocksPerChunk())
				oid, _ := chunk.ObjectID(origin.URL)
				wrongNamespace := store.BlockKey{Chunk: chunk.ChunkKey("different", oid, 0)}
				for _, req := range []peer.BlockRequest{
					{Key: key, URL: unrelated.URL},
					{Key: wrongBlock, URL: origin.URL},
					{Key: wrongNamespace, URL: origin.URL},
				} {
					data, held, err := caller.Get(context.Background(), addr, req)
					if err != nil || held || len(data) != 0 {
						t.Fatalf("inconsistent request: %d bytes, held=%v, err=%v; want a clean decline", len(data), held, err)
					}
				}
				if atomic.LoadInt64(&parentCalls) != 0 || atomic.LoadInt64(&originCalls) != 0 || atomic.LoadInt64(&unrelatedCalls) != 0 {
					t.Fatal("inconsistent request contacted parent or origin")
				}
				if client.Breaker.State(strings.TrimPrefix(parent.URL, "http://")) != peer.BreakerClosed || caller.Breaker.State(addr) != peer.BreakerClosed {
					t.Fatal("requester error quarantined an honest parent or relay")
				}
				// A consistent request still works, and a cache-only request
				// remains valid without an Origin header once that block is held.
				for _, raw := range []string{origin.URL, ""} {
					data, held, err := caller.Get(context.Background(), addr, peer.BlockRequest{Key: key, URL: raw})
					if err != nil || !held || !bytes.Equal(data, content) {
						t.Fatalf("legitimate request: held=%v, err=%v, bytes=%d", held, err, len(data))
					}
				}
			})
		}
	}
}
