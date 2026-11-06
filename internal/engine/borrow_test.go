package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
	for _, mode := range []string{"canceled-leader", "different-url", "same-url-failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, end := prefetchContext(t)
			defer end()
			leader, cancel := context.WithCancel(ctx)
			defer cancel()
			var g leaseLoads
			started, release := make(chan struct{}), make(chan struct{})
			firstDone := make(chan error, 1)
			failure := errors.New("origin refused request")
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
			if mode == "same-url-failure" {
				if !errors.Is(err, failure) || retries.Load() != 0 {
					t.Fatal("unexpected same-URL retry", err)
				}
			} else if err != nil || retries.Load() != 1 {
				t.Fatal("live waiter did not retry", err, retries.Load())
			}
		})
	}
}
