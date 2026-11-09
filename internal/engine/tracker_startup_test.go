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
	"github.com/data-accelerator/dart/internal/store"
	"github.com/data-accelerator/dart/internal/tracker"
)

type startupCluster struct {
	url, oid                               string
	content                                []byte
	readers                                []*Engine // HRW order, excluding the root
	servers                                []*httptest.Server
	rootBytes, peerBytes, joins, joinBytes atomic.Int64
	joinNanos                              atomic.Int64
	registry                               *tracker.Registry
	trackerTime                            time.Time
}

// The tracker clock is fixed: the initial tick can never hide a JOIN publication
// bug. Every reader uses the real HTTP tracker client and its real snapshot cache.
func newStartupCluster(t *testing.T, blocks int, rootDelay time.Duration) *startupCluster {
	t.Helper()
	const block = 4 * chunk.MiB
	s := &startupCluster{url: "http://origin.invalid/startup", content: blob(int(block) * blocks), trackerTime: time.Now()}
	s.oid, _ = chunk.ObjectID(s.url)
	cfg := chunk.Config{ChunkSize: 256 * chunk.MiB, BlockSize: block}
	ck := chunk.ChunkKey("dart", s.oid, 0)
	fk := chunk.ChunkKey("dart", s.oid, -1)
	nodes := make([]hashring.Node, 8)
	for i := range nodes {
		nodes[i] = hashring.Node{ID: fmt.Sprint("reader-", i), Weight: 1}
	}
	rootID := ""
	for i := 0; ; i++ {
		id := fmt.Sprint("root-", i)
		if hashring.Rank(fk, append(append([]hashring.Node(nil), nodes...), hashring.Node{ID: id, Weight: 1}))[0].ID == id {
			rootID = id
			break
		}
	}
	s.registry = tracker.NewRegistry(tracker.Options{Now: func() time.Time { return s.trackerTime }})
	open := func() *store.MemStore {
		st, err := store.OpenMem(store.MemOptions{SlotSize: block, Slots: blocks})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	}
	root := open()
	for i := 0; i < blocks; i++ {
		if err := root.Put(store.BlockKey{Chunk: ck, Block: uint64(i)}, s.content[int(block)*i:int(block)*(i+1)]); err != nil {
			t.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	th := (&tracker.Server{R: s.registry}).Handler()
	mux.Handle("/tracker/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.joins.Add(1)
		start := time.Now()
		defer func() { s.joinNanos.Add(time.Since(start).Nanoseconds()) }()
		th.ServeHTTP(countedPeerWriter{w, &s.joinBytes}, r)
	}))
	ph := &peer.Server{Borrow: func(_ context.Context, req peer.BlockRequest) (store.Lease, bool, error) {
		return root.Acquire(req.Key)
	}}
	rootSlot := make(chan struct{}, 1)
	mux.Handle("/peer/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rootDelay > 0 {
			select {
			case rootSlot <- struct{}{}:
			case <-r.Context().Done():
				return
			}
			defer func() { <-rootSlot }()
			timer := time.NewTimer(rootDelay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		ph.ServeHTTP(countedPeerWriter{w, &s.rootBytes}, r)
	}))
	rs := httptest.NewServer(mux)
	t.Cleanup(rs.Close)
	s.servers = append(s.servers, rs)
	members := []cluster.Member{{ID: rootID, Addr: strings.TrimPrefix(rs.URL, "http://"), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly}}
	p := cluster.NewStaticProvider()
	for _, node := range hashring.Rank(ck, nodes) {
		e, err := New(Options{Chunk: cfg, Store: open(), Fetcher: &forbiddenOrigin{}, Cluster: p, Peer: peer.NewClient(), SelfID: node.ID, Role: cluster.NonRoot, TrackerClient: tracker.NewClient(), Hedge: false, PrefetchWindow: 1})
		if err != nil {
			t.Fatal(err)
		}
		e.sizes[fetch.OriginKey(s.url)] = sizeMeta{size: int64(len(s.content))}
		s.readers = append(s.readers, e)
		h := &peer.Server{Borrow: e.PeerLeaseSource()}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(countedPeerWriter{w, &s.peerBytes}, r) }))
		t.Cleanup(srv.Close)
		s.servers = append(s.servers, srv)
		members = append(members, cluster.Member{ID: node.ID, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, State: cluster.Ready, Role: cluster.NonRoot})
	}
	p.Set(members)
	return s
}

func (s *startupCluster) closeHTTP() {
	for _, srv := range s.servers {
		srv.Close()
	}
}

func TestTrackerStartupPublishesBeforeTick(t *testing.T) {
	s := newStartupCluster(t, 1, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The worst-ranked first reader causes every later reader to choose the root
	// when all JOIN responses remain frozen at that singleton. Immediate growth
	// instead lets ranks 1..6 discover rank 0, without refreshing their predecessors.
	for _, i := range []int{7, 0, 1, 2, 3, 4, 5, 6} {
		var out bytes.Buffer
		if err := s.readers[i].Serve(ctx, &out, s.url, 0, int64(len(s.content)-1)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), s.content) {
			t.Fatal("wrong bytes")
		}
	}
	s.closeHTTP() // Join sender handlers before inspecting completed Write counts.
	t.Logf("root_bytes=%d peer_bytes=%d joins=%d", s.rootBytes.Load(), s.peerBytes.Load(), s.joins.Load())
	if s.rootBytes.Load() != 2*4*chunk.MiB || s.peerBytes.Load() != 6*4*chunk.MiB || s.joins.Load() != 8 {
		t.Fatal("startup did not use the growing reader set")
	}
}
