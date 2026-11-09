package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

func receiveUsage(e *Engine) (int, int64) {
	e.receive.mu.Lock()
	defer e.receive.mu.Unlock()
	return e.receive.loads, e.receive.bytes
}

func uncachedEngine(t *testing.T, f fetch.Fetcher, window, loads int, byteLimit int64) *Engine {
	t.Helper()
	// Standalone reads are Owned, which this borrowed-only store disables.
	st, err := store.OpenTiered(store.TieredOptions{Memory: true, SlotSize: 16, Slots: 1, OwnedFractionSet: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: f, PrefetchWindow: window, ReceiveLoads: loads, ReceiveBytes: byteLimit})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestReceiveBudgetTracksSharedDelivery(t *testing.T) {
	ctx, cancel := prefetchContext(t)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	f := intoFetchFunc(func(ctx context.Context, _ string, _, _ int64, dst []byte) (fetch.Range, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return fetch.Range{}, ctx.Err()
			}
		}
		copy(dst, "0123456789abcdef")
		return fetch.Range{Data: dst, Total: 32}, nil
	})
	e := uncachedEngine(t, f, 1, 1, 16)
	e.mx = NewMetrics(metrics.NewRegistry())
	e.receive.mx = e.mx
	oid, _ := chunk.ObjectID(prefetchURL)
	type result struct {
		l   store.Lease
		err error
	}
	results := make(chan result, 8)
	for i := 0; i < cap(results); i++ {
		go func() {
			l, err := e.borrowBlock(ctx, prefetchURL, oid, 32, 0, 0)
			results <- result{l, err}
		}()
	}
	waitBlockEvent(t, ctx, started)
	for {
		e.borrowed.mu.Lock()
		joined := false
		for _, c := range e.borrowed.m {
			joined = c.refs == cap(results)
		}
		e.borrowed.mu.Unlock()
		if joined {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("readers did not join", ctx.Err())
		}
		runtime.Gosched()
	}
	close(release)
	var owners []store.Lease
	defer func() {
		for i := range owners {
			owners[i].Release()
		}
	}()
	for i := 0; i < cap(results); i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		owners = append(owners, r.l)
		if &owners[0].Bytes()[0] != &r.l.Bytes()[0] {
			t.Fatal("shared refill copied")
		}
	}
	if calls.Load() != 1 || e.store.Len() != 0 {
		t.Fatal("refill not shared or disabled class cached")
	}
	last := owners[0].Clone()
	defer last.Release()
	for i := range owners {
		owners[i].Release()
	}
	if loads, n := receiveUsage(e); loads != 1 || n != 16 {
		t.Fatal("early budget release", loads, n)
	}
	if _, err := e.borrowBlock(ctx, prefetchURL, oid, 32, 1, 0); !errors.Is(err, ErrReceiveBudget) {
		t.Fatal("unbounded temporary allocation", err)
	}
	if calls.Load() != 1 {
		t.Fatal("exhausted receiver contacted source")
	}
	if e.mx.receiveLoads.Value() != 1 || e.mx.receiveBytes.Value() != 16 || e.mx.receiveRejected.Value() != 1 {
		t.Fatal("receive pressure not observable")
	}
	last.Release()
	if e.mx.receiveLoads.Value() != 0 || e.mx.receiveBytes.Value() != 0 {
		t.Fatal("stale receive gauges")
	}
	next, err := e.borrowBlock(ctx, prefetchURL, oid, 32, 1, 0)
	if err != nil {
		t.Fatal("released capacity unusable", err)
	}
	next.Release()
	if loads, n := receiveUsage(e); loads != 0 || n != 0 {
		t.Fatal("capacity leak", loads, n)
	}
}

func TestReceiveBudgetPrefetchDemandProgress(t *testing.T) {
	for _, limits := range []struct {
		loads int
		bytes int64
	}{{1, 128}, {8, 16}, {2, 32}} {
		content := blob(83)
		var calls atomic.Int32
		e := uncachedEngine(t, intoFetchFunc(func(_ context.Context, _ string, start, end int64, dst []byte) (fetch.Range, error) {
			calls.Add(1)
			copy(dst, content[start:end+1])
			return fetch.Range{Data: dst, Total: int64(len(content))}, nil
		}), 8, limits.loads, limits.bytes)
		e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: int64(len(content))}
		ctx, cancel := prefetchContext(t)
		var output bytes.Buffer
		err := e.Serve(ctx, &output, prefetchURL, 3, 80)
		cancel()
		if err != nil || !bytes.Equal(output.Bytes(), content[3:81]) {
			t.Fatal(limits, err, output.Bytes())
		}
		if calls.Load() != 6 {
			t.Fatal("speculative refusal repeated source transfer", calls.Load())
		}
		if loads, n := receiveUsage(e); loads != 0 || n != 0 {
			t.Fatal("receive leak", loads, n)
		}
		if loads, n := budgetUsage(e); loads != 0 || n != 0 {
			t.Fatal("prefetch leak", loads, n)
		}
		if e.store.Len() != 0 {
			t.Fatal("disabled class cached")
		}
	}
}

func TestReceiveBudgetFailureAndCancellation(t *testing.T) {
	for _, mode := range []string{"error", "cancel", "short", "write"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := prefetchContext(t)
			defer cancel()
			started := make(chan struct{})
			e := uncachedEngine(t, intoFetchFunc(func(work context.Context, _ string, _, _ int64, dst []byte) (fetch.Range, error) {
				close(started)
				switch mode {
				case "error":
					return fetch.Range{}, io.ErrUnexpectedEOF
				case "cancel":
					<-work.Done()
					return fetch.Range{}, work.Err()
				case "short":
					return fetch.Range{Data: dst[:1], Total: 16}, nil
				default:
					return fetch.Range{Data: dst, Total: 16}, nil
				}
			}), 1, 1, 16)
			e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: 16}
			done := make(chan error, 1)
			go func() {
				done <- e.Serve(ctx, rangeWriteFunc(func([]byte) (int, error) { return 0, io.ErrClosedPipe }), prefetchURL, 0, 15)
			}()
			waitBlockEvent(t, ctx, started)
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected read/write failure")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not join")
			}
			if loads, n := receiveUsage(e); loads != 0 || n != 0 {
				t.Fatal("failed buffer leaked", loads, n)
			}
		})
	}
}

func TestReceiveBudgetRangeIgnoredRetainsOneBuffer(t *testing.T) {
	for _, managed := range []bool{false, true} {
		f := intoFetchFunc(func(_ context.Context, _ string, _, _ int64, dst []byte) (fetch.Range, error) {
			copy(dst, "0123456789abcdef")
			return fetch.Range{Data: dst, Total: 16, RangeIgnored: true}, nil
		})
		e := uncachedEngine(t, f, 1, 1, 16)
		if managed {
			st, err := store.OpenMem(store.MemOptions{SlotSize: 16, Slots: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			e.store = st
		}
		oid, _ := chunk.ObjectID(prefetchURL)
		l, err := e.borrowBlock(context.Background(), prefetchURL, oid, 16, 0, 0)
		if err != nil || string(l.Bytes()) != "0123456789abcdef" {
			t.Fatal(err)
		}
		if e.store.Len() != 0 {
			t.Fatal("range-ignored response published")
		}
		want := 1
		if managed {
			want = 0
		}
		if loads, _ := receiveUsage(e); loads != want {
			t.Fatal("unaccounted range-ignored payload", loads)
		}
		l.Release()
		if loads, n := receiveUsage(e); loads != 0 || n != 0 {
			t.Fatal("ignored range leak", loads, n)
		}
	}
}

func TestReceiveBudgetAdmission(t *testing.T) {
	for _, limits := range []struct {
		loads int
		bytes int64
	}{{1, 32}, {2, 16}} {
		b := receiveBudget{maxLoads: limits.loads, maxBytes: limits.bytes, blockSize: 16}
		ctx, cancel := context.WithCancel(context.Background())
		if err := b.acquire(ctx, 16, true); !errors.Is(err, ErrReceiveBudget) {
			t.Fatal("speculation consumed demand capacity", err)
		}
		if err := b.acquire(ctx, 16, false); err != nil {
			t.Fatal(err)
		}
		cancel()
		if err := b.acquire(ctx, 16, false); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := b.acquire(context.Background(), 16, false); !errors.Is(err, ErrReceiveBudget) {
			t.Fatal("exhaustion did not terminate", err)
		}
		b.release(16)
		if err := b.acquire(context.Background(), 16, false); err != nil {
			t.Fatal(err)
		}
		b.release(16)
	}
	st := openStoreAt(t)
	for _, opt := range []Options{{ReceiveBytes: -1}, {ReceiveBytes: 15}, {ReceiveLoads: -1}} {
		opt.Chunk, opt.Store, opt.Fetcher = testCfg(), st, newFetcher()
		if _, err := New(opt); err == nil {
			t.Fatal("invalid limits accepted", opt)
		}
	}
}

// Pause only the first memory reservation, so a foreground reader can join the
// speculative flight before its separate temporary admission declines.
type gatedReserveStore struct {
	store.Store
	entered chan struct{}
	resume  <-chan struct{}
	calls   atomic.Int32
}

func (s *gatedReserveStore) Reserve(ctx context.Context, _ store.BlockKey, _ int64, _ store.Class) (store.Reservation, error) {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return store.Reservation{}, ctx.Err()
		}
	}
	return store.Reservation{}, store.ErrNoSlot
}

func TestDemandRetriesSkippedSpeculativeFlight(t *testing.T) {
	for _, mode := range []string{"borrowed", "owned", "peer-source"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := prefetchContext(t)
			defer cancel()
			var calls atomic.Int32
			e := uncachedEngine(t, intoFetchFunc(func(_ context.Context, _ string, _, _ int64, dst []byte) (fetch.Range, error) {
				calls.Add(1)
				copy(dst, "0123456789abcdef")
				return fetch.Range{Data: dst, Total: 16}, nil
			}), 2, 1, 16)
			resume := make(chan struct{})
			gate := &gatedReserveStore{Store: e.store, entered: make(chan struct{}), resume: resume}
			e.store = gate
			oid, _ := chunk.ObjectID(prefetchURL)
			skipped := make(chan error, 1)
			go func() {
				l, err := e.borrowBlock(speculativeContext(ctx), prefetchURL, oid, 16, 0, 0)
				l.Release()
				skipped <- err
			}()
			waitBlockEvent(t, ctx, gate.entered)
			joined := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				var l store.Lease
				var err error
				switch mode {
				case "borrowed":
					l, err = e.borrowBlock(joined, prefetchURL, oid, 16, 0, 0)
				case "owned":
					var data []byte
					data, err = e.block(joined, prefetchURL, oid, 16, 0, 0)
					l = store.HeapLease(data)
				case "peer-source":
					e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: 16}
					var data []byte
					var held bool
					data, held, err = e.PeerSource()(joined, peer.BlockRequest{Key: blockKeyFor(prefetchURL), URL: prefetchURL, Hop: 1})
					if err == nil && !held {
						err = errors.New("demand declined")
					}
					l = store.HeapLease(data)
				}
				if err == nil && string(l.Bytes()) != "0123456789abcdef" {
					err = errors.New("wrong demand data")
				}
				l.Release()
				done <- err
			}()
			waitBlockEvent(t, ctx, joined.joined)
			close(resume)
			if err := <-skipped; !errors.Is(err, errSpeculativeReceive) {
				t.Fatal("speculation unexpectedly admitted", err)
			}
			if err := <-done; err != nil {
				t.Fatal("demand inherited speculative refusal", err)
			}
			if calls.Load() != 1 {
				t.Fatal("wrong number of origin fills", calls.Load())
			}
			if loads, n := receiveUsage(e); loads != 0 || n != 0 {
				t.Fatal("capacity leak", loads, n)
			}
		})
	}
}
