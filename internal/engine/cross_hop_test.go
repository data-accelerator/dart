package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

type pausedPublication struct {
	store.Store
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
	puts    atomic.Int32
	reject  bool
}

func (s *pausedPublication) Put(k store.BlockKey, data []byte) error {
	s.puts.Add(1)
	s.once.Do(func() { close(s.started) })
	<-s.release
	if s.reject {
		return nil
	}
	return s.Store.Put(k, data)
}

type countedPeerWriter struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (w countedPeerWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

func TestCrossHopReadersShareOnePeerTransfer(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "cached"
		if reject {
			name = "cache-declined"
		}
		t.Run(name, func(t *testing.T) {
			const block = 4 * chunk.MiB
			const url = "http://origin.invalid/cross-hop"
			content := blob(block)
			oid, _ := chunk.ObjectID(url)
			key := store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, 0)}
			open := func() *store.MemStore {
				s, err := store.OpenMem(store.MemOptions{SlotSize: block, Slots: 2})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				return s
			}
			root := open()
			if err := root.Put(key, content); err != nil {
				t.Fatal(err)
			}
			var rootRequests, rootBytes atomic.Int64
			rootHandler := &peer.Server{Borrow: func(ctx context.Context, req peer.BlockRequest) (store.Lease, bool, error) {
				return root.Acquire(req.Key)
			}}
			rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rootRequests.Add(1)
				rootHandler.ServeHTTP(countedPeerWriter{w, &rootBytes}, r)
			}))
			defer rs.Close()
			ordered := hashring.Rank(key.Chunk, []hashring.Node{{ID: "reader-a", Weight: 1}, {ID: "reader-b", Weight: 1}})
			aID, bID := ordered[0].ID, ordered[1].ID
			provider := cluster.NewStaticProvider()
			gate := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate) }) }
			defer release()
			cacheA := &pausedPublication{Store: open(), started: make(chan struct{}), release: gate, reject: reject}
			forbidden := &forbiddenOrigin{}
			mxA := NewMetrics(metrics.NewRegistry())
			newReader := func(id string, s store.Store, mx *Metrics) *Engine {
				e, err := New(Options{Chunk: chunk.Config{BlockSize: block, ChunkSize: 256 * chunk.MiB}, Store: s, Fetcher: forbidden,
					Cluster: provider, Peer: peer.NewClient(), SelfID: id, Role: cluster.NonRoot, Fanout: 1, Hedge: false, PrefetchWindow: 1, Metrics: mx})
				if err != nil {
					t.Fatal(err)
				}
				e.sizes[fetch.OriginKey(url)] = sizeMeta{size: block}
				return e
			}
			a := newReader(aID, cacheA, mxA)
			as := httptest.NewServer(&peer.Server{Borrow: a.PeerLeaseSource()})
			defer as.Close()
			b := newReader(bID, open(), nil)
			provider.Set([]cluster.Member{
				{ID: "root", Addr: strings.TrimPrefix(rs.URL, "http://"), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly},
				{ID: aID, Addr: strings.TrimPrefix(as.URL, "http://"), Weight: 1, State: cluster.Ready, Role: cluster.NonRoot},
				{ID: bID, Addr: "unused:1", Weight: 1, State: cluster.Ready, Role: cluster.NonRoot}})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 2)
			read := func(e *Engine) {
				var out bytes.Buffer
				err := e.Serve(ctx, &out, url, 0, block-1)
				if err == nil && !bytes.Equal(out.Bytes(), content) {
					err = errors.New("wrong reader bytes")
				}
				done <- err
			}
			go read(a)
			waitBlockEvent(t, ctx, cacheA.started) // A has received R's block but still has no visible cache entry.
			go read(b)
			// Admission is the deterministic barrier. On the buggy implementation
			// two hop keys enter; retain the publication gate until both transfers
			// finish. A single shared flight instead has two admitted consumers.
			for {
				a.borrowed.mu.Lock()
				refs, flights := 0, len(a.borrowed.m)
				for _, c := range a.borrowed.m {
					refs += c.refs
				}
				a.borrowed.mu.Unlock()
				if refs >= 2 && (flights == 1 || cacheA.puts.Load() >= 2) {
					break
				}
				if ctx.Err() != nil {
					release()
					t.Fatal("overlapping readers did not reach publication", ctx.Err())
				}
				runtime.Gosched()
			}
			// Keep two independently owned result leases after both HTTP readers
			// finish. A canceled third waiter must not release either result.
			type heldResult struct {
				l   store.Lease
				err error
			}
			held := make(chan heldResult, 2)
			for i := 0; i < 2; i++ {
				go func() { l, err := a.borrowBlock(ctx, url, oid, block, 0, 7); held <- heldResult{l, err} }()
			}
			canceled, stop := context.WithCancel(ctx)
			canceledDone := make(chan error, 1)
			go func() { l, err := a.borrowBlock(canceled, url, oid, block, 0, 9); l.Release(); canceledDone <- err }()
			for {
				a.borrowed.mu.Lock()
				refs := 0
				for _, c := range a.borrowed.m {
					refs += c.refs
				}
				a.borrowed.mu.Unlock()
				if refs == 5 {
					break
				}
				if ctx.Err() != nil {
					stop()
					release()
					t.Fatal("lease consumers did not join")
				}
				runtime.Gosched()
			}
			stop()
			if err := <-canceledDone; !errors.Is(err, context.Canceled) {
				t.Fatal("canceled waiter", err)
			}
			release()
			x, y := <-held, <-held
			defer x.l.Release()
			defer y.l.Release()
			if x.err != nil || y.err != nil {
				t.Fatal(x.err, y.err)
			}
			for i := 0; i < 2; i++ {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			as.Close()
			rs.Close()
			if rootRequests.Load() != 1 || rootBytes.Load() != block || mxA.peerBytes.Value() != block {
				t.Fatalf("R->A requests=%d HTTP payload=%d A peer ingress=%d; want 1, %d, %d", rootRequests.Load(), rootBytes.Load(), mxA.peerBytes.Value(), block, block)
			}
			if forbidden.calls.Load() != 0 || cacheA.Has(key) == reject {
				t.Fatal("unexpected origin/cache activity")
			}
			if n, bytes := receiveUsage(a); n != 1 || bytes != block {
				t.Fatal("shared leases lost capacity ownership", n, bytes)
			}
			x.l.Release()
			if !bytes.Equal(y.l.Bytes(), content) {
				t.Fatal("first release invalidated sibling")
			}
			if n, _ := receiveUsage(a); n != 1 {
				t.Fatal("released before final consumer")
			}
			y.l.Release()
			if n, bytes := receiveUsage(a); n != 0 || bytes != 0 {
				t.Fatal("shared result leaked temporary capacity", n, bytes)
			}
		})
	}
}

func TestNormalFlightCanReceiveFallbackSupply(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint("reject-cache=", reject), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			content := blob(16)
			var originGets int64
			origin := countingOrigin(t, content, &originGets)
			miss := httptest.NewServer(http.NotFoundHandler())
			defer miss.Close()
			p := cluster.NewStaticProvider()
			gate := make(chan struct{})
			close(gate)
			cache := &pausedPublication{Store: openStoreAt(t), started: make(chan struct{}), release: gate, reject: reject}
			a, err := New(Options{Chunk: testCfg(), Store: cache, Fetcher: newFetcher(), Cluster: p, Peer: peer.NewClient(), SelfID: "A", Role: cluster.Ordinary, Fanout: 1, PrefetchWindow: 1})
			if err != nil {
				t.Fatal(err)
			}
			b, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: &forbiddenOrigin{}, Cluster: p, Peer: peer.NewClient(), SelfID: "B", Role: cluster.NonRoot, Fanout: 1, PrefetchWindow: 1})
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range []*Engine{a, b} {
				e.sizes[fetch.OriginKey(origin.URL)] = sizeMeta{size: 16}
			}
			var supplyCalls atomic.Int32
			ah := &peer.Server{Borrow: a.PeerLeaseSource()}
			as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(peer.HeaderSupply) != "1" {
					t.Error("fallback was not terminal supply")
				}
				a.borrowed.mu.Lock()
				waiting := false
				for key := range a.borrowed.m {
					if !key.supply {
						waiting = true
					}
				}
				a.borrowed.mu.Unlock()
				if !waiting {
					t.Error("test did not overlap normal and supply dependency")
				}
				supplyCalls.Add(1)
				ah.ServeHTTP(w, r)
			}))
			defer as.Close()
			bs := httptest.NewServer(&peer.Server{Borrow: b.PeerLeaseSource()})
			defer bs.Close()
			p.Set([]cluster.Member{
				{ID: "R", Addr: miss.Listener.Addr().String(), Weight: 1e9, State: cluster.Ready, Role: cluster.RootOnly},
				{ID: "A", Addr: as.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.Ordinary},
				{ID: "B", Addr: bs.Listener.Addr().String(), Weight: 1e12, State: cluster.Ready, Role: cluster.NonRoot},
			})
			var output bytes.Buffer
			if err := a.Serve(ctx, &output, origin.URL, 0, 15); err != nil || !bytes.Equal(output.Bytes(), content) {
				t.Fatal("fallback dependency failed", err)
			}
			as.Close()
			bs.Close()
			if supplyCalls.Load() != 1 || atomic.LoadInt64(&originGets) != 1 {
				t.Fatal("wrong fallback path", supplyCalls.Load(), originGets)
			}
			for _, e := range []*Engine{a, b} {
				if n, b := receiveUsage(e); n != 0 || b != 0 {
					t.Fatal("fallback leaked buffer", n, b)
				}
			}
		})
	}
}
