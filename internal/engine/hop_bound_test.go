package engine

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// TestRelayHopBoundary pins the maxHop gate on both peer-facing sources
// (PeerSource and PeerStreamSource): at hop == maxHop-1 a miss still relays
// (falls through to origin here, this node being a lone root), while at
// hop == maxHop the request is declined without the origin being touched at
// all. Together with the peer transport rejecting negative/malformed hops
// (internal/peer), this is the whole loop-safety contract: hop enters the
// engine in [0, maxHop), grows by one per relay, and is cut off at maxHop.
func TestRelayHopBoundary(t *testing.T) {
	content := blob(100)

	cases := []struct {
		name      string
		hop       int
		wantHeld  bool
		wantFetch bool
	}{
		{"one below the bound relays", maxHop - 1, true, true},
		{"at the bound declines", maxHop, false, false},
	}

	sources := map[string]func(e *Engine, req peer.BlockRequest, buf *bytes.Buffer) (bool, error){
		"buffered": func(e *Engine, req peer.BlockRequest, buf *bytes.Buffer) (bool, error) {
			data, held, err := e.PeerSource()(context.Background(), req)
			if held {
				buf.Write(data)
			}
			return held, err
		},
		"stream": func(e *Engine, req peer.BlockRequest, buf *bytes.Buffer) (bool, error) {
			_, held, err := e.PeerStreamSource()(context.Background(), req, buf, func(int64) {})
			return held, err
		},
	}

	for srcName, call := range sources {
		for _, tc := range cases {
			t.Run(srcName+"/"+tc.name, func(t *testing.T) {
				var originHits int64
				origin := countingOrigin(t, content, &originHits)

				prov := cluster.NewStaticProvider(
					cluster.Member{ID: "R", Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready})
				e, err := New(Options{
					Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
					Cluster: prov, Peer: peer.NewClient(), SelfID: "R",
				})
				if err != nil {
					t.Fatalf("New: %v", err)
				}

				url := origin.URL + "/blob"
				oid, _ := chunk.ObjectID(url)
				key := store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, 0), Block: 0}
				req := peer.BlockRequest{Key: key, URL: url, Hop: tc.hop}

				var buf bytes.Buffer
				held, err := call(e, req, &buf)
				if err != nil {
					t.Fatalf("source error: %v", err)
				}
				if held != tc.wantHeld {
					t.Errorf("held = %v, want %v", held, tc.wantHeld)
				}
				if gotFetch := atomic.LoadInt64(&originHits) > 0; gotFetch != tc.wantFetch {
					t.Errorf("origin contacted = %v, want %v", gotFetch, tc.wantFetch)
				}
				if tc.wantHeld && !bytes.Equal(buf.Bytes(), content[:16]) {
					t.Errorf("served bytes mismatch: got %d bytes %q", buf.Len(), strings.TrimSpace(buf.String()))
				}
			})
		}
	}
}
