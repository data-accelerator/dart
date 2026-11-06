package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

type intoFetchFunc func(context.Context, string, int64, int64, []byte) (fetch.Range, error)

func (f intoFetchFunc) Fetch(context.Context, string, int64, int64) (fetch.Range, error) {
	panic("owned fetch used instead of direct fill")
}
func (f intoFetchFunc) FetchInto(ctx context.Context, u string, s, e int64, b []byte) (fetch.Range, error) {
	return f(ctx, u, s, e, b)
}

func TestBorrowedOriginUsesReceiveSlot(t *testing.T) {
	var received []byte
	f := intoFetchFunc(func(_ context.Context, _ string, s, e int64, dst []byte) (fetch.Range, error) {
		received = dst
		copy(dst, "0123456789abcdef")
		return fetch.Range{Data: dst, Total: 16}, nil
	})
	e := prefetchEngine(t, blob(16), 1, f)
	oid, _ := chunk.ObjectID(prefetchURL)
	l, err := e.borrowBlock(context.Background(), prefetchURL, oid, 16, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	cached, ok, err := store.Acquire(e.store, prefetchKey(prefetchURL, 0))
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	if &l.Bytes()[0] != &received[0] || &cached.Bytes()[0] != &received[0] {
		t.Fatal("origin receive bytes were copied")
	}
	l.Release()
	cached.Release()
	// Once warm, the cache-serving block path has no per-block allocations.
	if n := testing.AllocsPerRun(1000, func() {
		l, err := e.borrowBlock(context.Background(), prefetchURL, oid, 16, 0, 0)
		if err != nil {
			panic(err)
		}
		l.Release()
	}); n != 0 {
		t.Fatalf("warm block allocations %g", n)
	}
}

func TestBorrowedPressureBypassesPublication(t *testing.T) {
	st, err := store.OpenMem(store.MemOptions{SlotSize: 16, Slots: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pin, _ := st.Reserve(context.Background(), store.BlockKey{Chunk: 99}, 16, store.Owned)
	defer pin.Abort()
	reg := metrics.NewRegistry()
	mx := NewMetrics(reg)
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: intoFetchFunc(func(_ context.Context, _ string, s, e int64, dst []byte) (fetch.Range, error) {
		copy(dst, "0123456789abcdef")
		return fetch.Range{Data: dst, Total: 16}, nil
	}), Metrics: mx, PrefetchWindow: 1})
	if err != nil {
		t.Fatal(err)
	}
	e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: 16}
	var output bytes.Buffer
	if err := e.Serve(context.Background(), &output, prefetchURL, 3, 8); err != nil {
		t.Fatal(err)
	}
	if output.String() != "345678" || st.Has(prefetchKey(prefetchURL, 0)) {
		t.Fatal("pressure response or publication wrong")
	}
	if mx.temporaryBuffers.Value() != 1 || mx.temporaryBytes.Value() != 16 {
		t.Fatal("temporary buffer not accounted")
	}
}

func TestBorrowedWriteFailureReleasesPins(t *testing.T) {
	content := blob(160)
	e := prefetchEngine(t, content, 8, blockFetchFunc(func(_ context.Context, _ string, s, end int64) (fetch.Range, error) {
		return prefetchResult(content, s, end), nil
	}))
	failure := errors.New("client write failed")
	if err := e.Serve(context.Background(), rangeWriteFunc(func([]byte) (int, error) { return 0, failure }), prefetchURL, 0, 159); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if n, b := budgetUsage(e); n != 0 || b != 0 {
		t.Fatal("speculative budget retained")
	}
	// Delete every published entry, then occupy the complete arena. Any leaked
	// result, queued completion or writer pin makes the final Reserve fail.
	mem := e.store.(*store.MemStore)
	for i := int64(0); i < 10; i++ {
		mem.Delete(prefetchKey(prefetchURL, i))
	}
	held := make([]store.Reservation, 64)
	for i := range held {
		r, err := mem.Reserve(context.Background(), store.BlockKey{Chunk: 900, Block: uint64(i)}, 16, store.Owned)
		if err != nil {
			t.Fatal("pin leak", i, err)
		}
		held[i] = r
	}
	for i := range held {
		held[i].Abort()
	}
}

func TestLeasedFlightSharesOneImmutableBuffer(t *testing.T) {
	ctx, cancel := prefetchContext(t)
	defer cancel()
	st, err := store.OpenMem(store.MemOptions{SlotSize: 16, Slots: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var g leaseLoads
	started, release := make(chan struct{}), make(chan struct{})
	load := func(context.Context) (leasedRead, error) {
		close(started)
		<-release
		r, _ := st.Reserve(ctx, store.BlockKey{}, 16, store.Owned)
		copy(r.Bytes(), "shared immutable")
		l, err := r.Commit()
		return leasedRead{lease: l, source: blockFromPeer, wireBytes: 16}, err
	}
	results := make(chan leasedRead, 2)
	go func() { r, _ := g.do(ctx, blockLoadKey{}, "url", false, load); results <- r }()
	waitBlockEvent(t, ctx, started)
	joined := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	go func() { r, _ := g.do(joined, blockLoadKey{}, "url", false, load); results <- r }()
	waitBlockEvent(t, ctx, joined.joined)
	close(release)
	a, b := <-results, <-results
	if &a.lease.Bytes()[0] != &b.lease.Bytes()[0] || a.wireBytes+b.wireBytes != 16 {
		t.Fatal("flight copied data or duplicated transfer credit")
	}
	st.Delete(store.BlockKey{})
	a.lease.Release()
	if string(b.lease.Bytes()) != "shared immutable" {
		t.Fatal("waiter lost its buffer")
	}
	b.lease.Release()
	r, err := st.Reserve(ctx, store.BlockKey{Chunk: 1}, 16, store.Owned)
	if err != nil {
		t.Fatal("flight retained slot", err)
	}
	r.Abort()
}

func TestSharedLeaseFlightCancellationOwnership(t *testing.T) {
	for _, survivor := range []bool{false, true} {
		t.Run(map[bool]string{false: "last-caller-joins", true: "survivor-owns-work"}[survivor], func(t *testing.T) {
			ctx, end := prefetchContext(t)
			defer end()
			caller, cancel := context.WithCancel(ctx)
			var g leaseLoads
			started, release, backendCanceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			load := func(work context.Context) (leasedRead, error) {
				close(started)
				select {
				case <-work.Done():
					close(backendCanceled)
					<-release
					return leasedRead{}, work.Err()
				case <-release:
					return leasedRead{lease: store.HeapLease([]byte("good")), wireBytes: 4}, nil
				}
			}
			go func() { r, err := g.do(caller, blockLoadKey{}, "url", true, load); r.lease.Release(); done <- err }()
			waitBlockEvent(t, ctx, started)
			var waiter chan leasedRead
			if survivor {
				waiter = make(chan leasedRead, 1)
				joined := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
				go func() {
					r, err := g.do(joined, blockLoadKey{}, "url", true, load)
					if err != nil {
						t.Error(err)
					}
					waiter <- r
				}()
				waitBlockEvent(t, ctx, joined.joined)
			}
			cancel()
			if survivor {
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				select {
				case <-backendCanceled:
					t.Fatal("canceled surviving work")
				default:
				}
				close(release)
				r := <-waiter
				if string(r.lease.Bytes()) != "good" || r.wireBytes != 4 {
					t.Fatal("survivor lost result/credit")
				}
				r.lease.Release()
			} else {
				waitBlockEvent(t, ctx, backendCanceled)
				select {
				case <-done:
					t.Fatal("final caller did not join backend")
				default:
				}
				close(release)
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestBorrowedPeerHTTPAndWarmReuse(t *testing.T) {
	content := blob(64)
	origin := countingOrigin(t, content, nil)
	root := prefetchEngine(t, content, 1, newFetcher())
	root.sizes[fetch.OriginKey(origin.URL)] = sizeMeta{size: 64}
	for i := int64(0); i < 4; i++ {
		root.store.Put(prefetchKey(origin.URL, i), content[i*16:(i+1)*16])
	}
	var calls atomic.Int64
	src := root.PeerLeaseSource()
	parent := httptest.NewServer(&peer.Server{Borrow: func(ctx context.Context, r peer.BlockRequest) (store.Lease, bool, error) {
		calls.Add(1)
		return src(ctx, r)
	}})
	defer parent.Close()
	provider := cluster.NewStaticProvider(cluster.Member{ID: "root", Addr: strings.TrimPrefix(parent.URL, "http://"), Weight: 1, State: cluster.Ready})
	st, err := store.OpenTiered(store.TieredOptions{Memory: true, SlotSize: 16, Slots: 16, OwnedFraction: .1})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher(), Cluster: provider, Peer: peer.NewClient(), SelfID: "leaf", PrefetchWindow: 4})
	if err != nil {
		t.Fatal(err)
	}
	e.sizes[fetch.OriginKey(origin.URL)] = sizeMeta{size: 64}
	server := httptest.NewServer(NewStaticHandler(e, origin.URL))
	defer server.Close()
	for run := 0; run < 2; run++ {
		response, err := http.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || !bytes.Equal(data, content) {
			t.Fatal("HTTP bytes", err)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("peer downloads %d", calls.Load())
	}
	if st.Stats().BorrowedBlocks != 4 {
		t.Fatal(st.Stats())
	}
}

func TestAbandonedOriginLeaseFlightStopsAdmission(t *testing.T) {
	ctx, end := prefetchContext(t)
	defer end()
	caller, cancel := context.WithCancel(ctx)
	var g leaseLoads
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	oldDone := make(chan error, 1)
	go func() {
		r, err := g.do(caller, blockLoadKey{}, "url", true, func(work context.Context) (leasedRead, error) {
			close(started)
			<-work.Done()
			close(canceled)
			<-release
			return leasedRead{}, work.Err()
		})
		r.lease.Release()
		oldDone <- err
	}()
	waitBlockEvent(t, ctx, started)
	cancel()
	waitBlockEvent(t, ctx, canceled)
	freshDone := make(chan leasedRead, 1)
	go func() {
		r, err := g.do(ctx, blockLoadKey{}, "url", true, func(context.Context) (leasedRead, error) {
			return leasedRead{lease: store.HeapLease([]byte("fresh"))}, nil
		})
		if err != nil {
			t.Error(err)
		}
		freshDone <- r
	}()
	select {
	case r := <-freshDone:
		if string(r.lease.Bytes()) != "fresh" {
			t.Error("new caller inherited canceled flight")
		}
		r.lease.Release()
	case <-ctx.Done():
		close(release)
		t.Fatal("new caller joined abandoned flight")
	}
	close(release)
	if err := <-oldDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestInlineLeaseFlightRetriesCancellationAndDifferentURL(t *testing.T) {
	for _, mode := range []string{"canceled-leader", "different-url", "same-url-failure", "backend-timeout", "backend-canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, end := prefetchContext(t)
			defer end()
			leader, cancel := context.WithCancel(ctx)
			defer cancel()
			var g leaseLoads
			started, release := make(chan struct{}), make(chan struct{})
			firstDone := make(chan error, 1)
			failure := errors.New("origin refused request")
			if mode == "backend-timeout" {
				failure = fmt.Errorf("backend: %w", context.DeadlineExceeded)
			} else if mode == "backend-canceled" {
				failure = fmt.Errorf("backend: %w", context.Canceled)
			}
			go func() {
				r, err := g.do(leader, blockLoadKey{}, "first", false, func(context.Context) (leasedRead, error) {
					close(started)
					<-release
					if mode == "canceled-leader" {
						return leasedRead{}, fmt.Errorf("backend: %w", leader.Err())
					}
					return leasedRead{}, failure
				})
				r.lease.Release()
				firstDone <- err
			}()
			waitBlockEvent(t, ctx, started)
			joined := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
			waiterDone := make(chan error, 1)
			var retries atomic.Int64
			url := "first"
			if mode == "different-url" {
				url = "renewed"
			}
			go func() {
				r, err := g.do(joined, blockLoadKey{}, url, false, func(context.Context) (leasedRead, error) {
					retries.Add(1)
					return leasedRead{lease: store.HeapLease([]byte("retry"))}, nil
				})
				r.lease.Release()
				waiterDone <- err
			}()
			waitBlockEvent(t, ctx, joined.joined)
			if mode == "canceled-leader" {
				cancel()
			}
			close(release)
			if err := <-firstDone; err == nil {
				t.Fatal("leader unexpectedly succeeded")
			}
			err := <-waiterDone
			if mode == "same-url-failure" || mode == "backend-timeout" || mode == "backend-canceled" {
				if !errors.Is(err, failure) || retries.Load() != 0 {
					t.Fatal("unexpected same-URL retry", err)
				}
			} else if err != nil || retries.Load() != 1 {
				t.Fatal("live waiter did not retry", err, retries.Load())
			}
		})
	}
}

func TestBackendTimeoutSharedByEngineWaiters(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(map[bool]string{false: "borrowed", true: "owned"}[owned], func(t *testing.T) {
			ctx, cancel := prefetchContext(t)
			defer cancel()
			started := make(chan struct{})
			var once sync.Once
			var calls atomic.Int64
			backend := intoFetchFunc(func(work context.Context, _ string, _, _ int64, _ []byte) (fetch.Range, error) {
				calls.Add(1)
				once.Do(func() { close(started) })
				<-work.Done()
				return fetch.Range{}, work.Err()
			})
			e := prefetchEngine(t, blob(16), 1, &fetch.Coalescing{F: backend, MaxFlight: 500 * time.Millisecond})
			oid, _ := chunk.ObjectID(prefetchURL)
			run := func(caller context.Context) error {
				if owned {
					_, err := e.block(caller, prefetchURL, oid, 16, 0, 0)
					return err
				}
				l, err := e.borrowBlock(caller, prefetchURL, oid, 16, 0, 0)
				l.Release()
				return err
			}
			results := make(chan error, 2)
			go func() { results <- run(ctx) }()
			waitBlockEvent(t, ctx, started)
			waiter := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
			go func() { results <- run(waiter) }()
			waitBlockEvent(t, ctx, waiter.joined)
			for i := 0; i < 2; i++ {
				select {
				case err := <-results:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("backend timeout was not shared", err)
					}
				case <-ctx.Done():
					t.Fatal("joined caller did not finish")
				}
			}
			if ctx.Err() != nil || calls.Load() != 1 {
				t.Fatal("live callers retried the shared timeout", ctx.Err(), calls.Load())
			}
		})
	}
}

func TestPeerDeclinesBlocksOutsideObject(t *testing.T) {
	for _, mode := range []string{"memory", "disk", "hybrid"} {
		t.Run(mode, func(t *testing.T) {
			var st store.Store
			mem, _ := store.OpenMem(store.MemOptions{SlotSize: 16, Slots: 8})
			if mode == "memory" {
				st = mem
			} else {
				disk, err := store.OpenTiered(store.TieredOptions{Path: filepath.Join(t.TempDir(), "cache"), SlotSize: 16, Slots: 8})
				if err != nil {
					t.Fatal(err)
				}
				st = disk
				if mode == "hybrid" {
					st = store.NewHybrid(mem, disk)
				} else {
					mem.Close()
				}
			}
			defer st.Close()
			f := blockFetchFunc(func(context.Context, string, int64, int64) (fetch.Range, error) {
				t.Error("out-of-object request reached origin")
				return fetch.Range{}, nil
			})
			e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: f})
			if err != nil {
				t.Fatal(err)
			}
			e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: 16}
			req := peer.BlockRequest{Key: prefetchKey(prefetchURL, 2), URL: prefetchURL}
			for _, owned := range []bool{false, true} {
				l, held, err := e.peerRead(context.Background(), req, owned)
				l.Release()
				if held || err != nil {
					t.Fatal("malformed peer block not declined", held, err)
				}
			}
			oid, _ := chunk.ObjectID(prefetchURL)
			for _, index := range []int64{-1, 1, 2, math.MaxInt64} {
				l, err := e.borrowBlock(context.Background(), prefetchURL, oid, 16, index, 0)
				l.Release()
				if !errors.Is(err, ErrRangeNotSatisfiable) {
					t.Fatal("bad geometry reached a worker", index, err)
				}
			}
		})
	}
}

func TestOversizedCacheSlotFallsBackToOrigin(t *testing.T) {
	mem, err := store.OpenMem(store.MemOptions{SlotSize: 8, Slots: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()
	e, err := New(Options{Chunk: testCfg(), Store: mem, Fetcher: intoFetchFunc(func(_ context.Context, _ string, s, end int64, dst []byte) (fetch.Range, error) {
		copy(dst, "0123456789abcdef")
		return fetch.Range{Data: dst, Total: 16}, nil
	}), PrefetchWindow: 1})
	if err != nil {
		t.Fatal(err)
	}
	e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: 16}
	var out bytes.Buffer
	if err := e.Serve(context.Background(), &out, prefetchURL, 0, 15); err != nil || out.String() != "0123456789abcdef" || mem.Len() != 0 {
		t.Fatal("cache capacity failed valid read", out.String(), err)
	}
}

func TestCanceledEngineLeaderDoesNotBlockReplacement(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprint("owned=", owned), func(t *testing.T) {
			ctx, end := prefetchContext(t)
			defer end()
			leader, cancel := context.WithCancel(ctx)
			defer cancel()
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			f := intoFetchFunc(func(work context.Context, _ string, s, end int64, dst []byte) (fetch.Range, error) {
				if calls.Add(1) == 1 {
					close(started)
					<-work.Done()
					close(canceled)
					<-release
					return fetch.Range{}, work.Err()
				}
				copy(dst, "replacement data")
				return fetch.Range{Data: dst, Total: 16}, nil
			})
			e := prefetchEngine(t, blob(16), 1, f)
			oid, _ := chunk.ObjectID(prefetchURL)
			run := func(ctx context.Context) ([]byte, error) {
				if owned {
					return e.block(ctx, prefetchURL, oid, 16, 0, 0)
				}
				l, err := e.borrowBlock(ctx, prefetchURL, oid, 16, 0, 0)
				defer l.Release()
				return append([]byte(nil), l.Bytes()...), err
			}
			oldDone := make(chan error, 1)
			go func() { _, err := run(leader); oldDone <- err }()
			waitBlockEvent(t, ctx, started)
			freshDone := make(chan error, 1)
			fresh := func(freshCtx context.Context) {
				data, err := run(freshCtx)
				if err == nil && string(data) != "replacement data" {
					err = errors.New("wrong replacement bytes")
				}
				freshDone <- err
			}
			cancel()
			waitBlockEvent(t, ctx, canceled)
			go fresh(ctx)
			select {
			case err := <-freshDone:
				if err != nil {
					t.Error(err)
				}
			case <-ctx.Done():
				close(release)
				t.Fatal("new engine request joined canceled cleanup")
			}
			select {
			case <-oldDone:
				t.Fatal("original owner did not join cleanup")
			default:
			}
			close(release)
			if err := <-oldDone; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatal("replacement backend count", calls.Load())
			}
		})
	}
}

func TestInlineLeaseWaiterCanReplaceCanceledCleanup(t *testing.T) {
	ctx, end := prefetchContext(t)
	defer end()
	leader, cancel := context.WithCancel(ctx)
	defer cancel()
	var g leaseLoads
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	oldDone := make(chan error, 1)
	go func() {
		_, err := g.do(leader, blockLoadKey{}, "url", false, func(work context.Context) (leasedRead, error) {
			close(started)
			<-work.Done()
			close(canceled)
			<-release
			return leasedRead{}, work.Err()
		})
		oldDone <- err
	}()
	waitBlockEvent(t, ctx, started)
	joiner := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() {
		r, err := g.do(joiner, blockLoadKey{}, "url", false, func(context.Context) (leasedRead, error) {
			return leasedRead{lease: store.HeapLease([]byte("replacement"))}, nil
		})
		if err == nil && string(r.lease.Bytes()) != "replacement" {
			err = errors.New("wrong replacement")
		}
		r.lease.Release()
		waiterDone <- err
	}()
	waitBlockEvent(t, ctx, joiner.joined)
	cancel()
	waitBlockEvent(t, ctx, canceled)
	select {
	case err := <-waiterDone:
		if err != nil {
			t.Error(err)
		}
	case <-ctx.Done():
		close(release)
		t.Fatal("joined waiter remained coupled to cleanup")
	}
	select {
	case <-oldDone:
		t.Fatal("leader released cleanup ownership")
	default:
	}
	close(release)
	if err := <-oldDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExpiredEngineFlightsAllowFreshAdmission(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owned bool
		hop   int
	}{
		{"borrowed-same-depth", false, 0}, {"borrowed-other-depth", false, 1}, {"owned-adapter", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, end := context.WithTimeout(context.Background(), 5*time.Second)
			defer end()
			started1, canceled1, release1 := make(chan struct{}), make(chan struct{}), make(chan struct{})
			started2, release2 := make(chan struct{}), make(chan struct{})
			var once1, once2 sync.Once
			free1 := func() { once1.Do(func() { close(release1) }) }
			free2 := func() { once2.Do(func() { close(release2) }) }
			defer free1()
			defer free2()
			var calls atomic.Int64
			backend := intoFetchFunc(func(work context.Context, _ string, s, end int64, dst []byte) (fetch.Range, error) {
				switch calls.Add(1) {
				case 1:
					close(started1)
					<-work.Done()
					close(canceled1)
					<-release1
					return fetch.Range{}, work.Err()
				case 2:
					close(started2)
					<-release2
				default:
					t.Error("late completion evicted replacement")
				}
				copy(dst, "replacement data")
				return fetch.Range{Data: dst, Total: 16}, nil
			})
			f := &fetch.Coalescing{F: &fetch.Coalescing{F: backend, MaxFlight: 250 * time.Millisecond}, MaxFlight: time.Second}
			e := prefetchEngine(t, blob(16), 1, f)
			oid, _ := chunk.ObjectID(prefetchURL)
			if e.origins.maxFlight != 250*time.Millisecond || e.borrowed.maxFlight != e.origins.maxFlight || e.loads.maxFlight != e.origins.maxFlight {
				t.Fatal("nested deadline not propagated")
			}
			run := func(hop int) error {
				if tc.owned {
					data, err := e.block(ctx, prefetchURL, oid, 16, 0, hop)
					if err == nil && string(data) != "replacement data" {
						err = errors.New("wrong owned bytes")
					}
					return err
				}
				l, err := e.borrowBlock(ctx, prefetchURL, oid, 16, 0, hop)
				defer l.Release()
				if err == nil && string(l.Bytes()) != "replacement data" {
					err = errors.New("wrong borrowed bytes")
				}
				return err
			}
			first := make(chan error, 1)
			go func() { first <- run(0) }()
			waitBlockEvent(t, ctx, started1)
			e.origins.mu.Lock()
			var expiry time.Time
			var original *leaseCall
			for _, c := range e.origins.m {
				expiry = c.stale
				original = c
			}
			e.origins.mu.Unlock()
			waitBlockEvent(t, ctx, canceled1)
			timer := time.NewTimer(time.Until(expiry) + time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				t.Fatal("flight expiry did not arrive")
			}
			// Hold cleanup until the original consumer actually observes expiry
			// and enters its final-owner join. Otherwise done and expiry can both
			// become ready before its select runs, legitimately sharing a timeout
			// instead of retrying the replacement.
			for {
				e.origins.mu.Lock()
				joining := original != nil && original.refs == 0
				e.origins.mu.Unlock()
				if joining {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("original consumer did not enter cleanup join")
				}
				runtime.Gosched()
			}
			second := make(chan error, 1)
			go func() { second <- run(tc.hop) }()
			waitBlockEvent(t, ctx, started2)
			select {
			case <-first:
				t.Fatal("original owner stopped joining cleanup")
			default:
			}
			free1() // old completion must not remove the still-running replacement
			for {
				e.origins.mu.Lock()
				joined := false
				for _, c := range e.origins.m {
					joined = c.refs >= 2
				}
				e.origins.mu.Unlock()
				if joined {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("old live caller did not rejoin replacement")
				}
				runtime.Gosched()
			}
			free2()
			if err := <-second; err != nil {
				t.Fatal(err)
			}
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatal("underlying calls", calls.Load())
			}
		})
	}
}
