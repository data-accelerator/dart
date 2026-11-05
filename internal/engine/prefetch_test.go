package engine

import (
	"bytes"
	"context"
	"errors"
	"github.com/data-accelerator/dart/internal/peer"
	"io"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/store"
)

type rangeWriteFunc func([]byte) (int, error)

func (f rangeWriteFunc) Write(b []byte) (int, error) { return f(b) }

const prefetchURL = "http://origin.test/large-file"

func prefetchEngine(t *testing.T, content []byte, window int, f fetch.Fetcher) *Engine {
	t.Helper()
	st, err := store.OpenMem(store.MemOptions{SlotSize: 16, Slots: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: f, PrefetchWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	e.sizes[fetch.OriginKey(prefetchURL)] = sizeMeta{size: int64(len(content))}
	return e
}

func prefetchResult(content []byte, start, end int64) fetch.Range {
	return fetch.Range{Data: append([]byte(nil), content[start:end+1]...), Total: int64(len(content))}
}

func prefetchContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func budgetUsage(e *Engine) (int, int64) {
	e.prefetch.mu.Lock()
	defer e.prefetch.mu.Unlock()
	return e.prefetch.loads, e.prefetch.bytes
}

func TestRangePrefetchOrderedAndBounded(t *testing.T) {
	content := blob(83)
	ctx, cancel := prefetchContext(t)
	defer cancel()
	started := make([]chan struct{}, 6)
	release := make([]chan struct{}, 6)
	for i := range started {
		started[i] = make(chan struct{})
		release[i] = make(chan struct{})
	}
	f := blockFetchFunc(func(ctx context.Context, _ string, start, end int64) (fetch.Range, error) {
		i := start / 16
		close(started[i])
		select {
		case <-release[i]:
		case <-ctx.Done():
			return fetch.Range{}, ctx.Err()
		}
		return prefetchResult(content, start, end), nil
	})
	e := prefetchEngine(t, content, 4, f)
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- e.Serve(ctx, &output, prefetchURL, 3, 70) }()
	for i := 0; i < 4; i++ {
		waitBlockEvent(t, ctx, started[i])
	}
	select {
	case <-started[4]:
		t.Fatal("request exceeded its four-block window")
	default:
	}
	// Complete later blocks first. Bytes must remain ordered when demand completes.
	close(release[3])
	close(release[2])
	close(release[1])
	close(release[0])
	waitBlockEvent(t, ctx, started[4])
	close(release[4])
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), content[3:71]) {
		t.Fatal("wrong or reordered bytes")
	}
	select {
	case <-started[5]:
		t.Fatal("prefetched beyond requested range")
	default:
	}
	for i := int64(0); i <= 4; i++ {
		if !e.store.Has(prefetchKey(prefetchURL, i)) {
			t.Fatalf("covering block %d not cached", i)
		}
	}
	if n, b := budgetUsage(e); n != 0 || b != 0 {
		t.Fatalf("leaked budget %d/%d", n, b)
	}
}

func prefetchKey(url string, index int64) store.BlockKey {
	oid, _ := chunk.ObjectID(url)
	return store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, testCfg().ChunkOfBlock(index)), Block: uint64(index)}
}

func TestRangePrefetchBackpressureAndDemandProgress(t *testing.T) {
	content := blob(96)
	ctx, cancel := prefetchContext(t)
	defer cancel()
	started := make([]chan struct{}, 6)
	for i := range started {
		started[i] = make(chan struct{})
	}
	f := blockFetchFunc(func(_ context.Context, _ string, start, end int64) (fetch.Range, error) {
		close(started[start/16])
		return prefetchResult(content, start, end), nil
	})
	e := prefetchEngine(t, content, 4, f)
	e.prefetch.maxBytes = 48
	e.prefetch.maxLoads = 3
	writing, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- e.Serve(ctx, rangeWriteFunc(func(b []byte) (int, error) {
			select {
			case <-writing:
			default:
				close(writing)
			}
			select {
			case <-release:
				return len(b), nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}), prefetchURL, 0, 63)
	}()
	waitBlockEvent(t, ctx, writing)
	for i := 1; i < 4; i++ {
		waitBlockEvent(t, ctx, started[i])
	}
	if n, b := budgetUsage(e); n != 3 || b != 48 {
		t.Fatalf("queued results lost reservations: %d/%d", n, b)
	}
	// A second multi-block Serve cannot prefetch, but its demand still completes.
	other := make(chan error, 1)
	go func() { other <- e.Serve(ctx, io.Discard, prefetchURL, 64, 95) }()
	waitBlockEvent(t, ctx, started[4])
	waitBlockEvent(t, ctx, started[5])
	if err := <-other; err != nil {
		t.Fatal(err)
	}
	if n, b := budgetUsage(e); n != 3 || b != 48 {
		t.Fatalf("global limit bypassed: %d/%d", n, b)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n, b := budgetUsage(e); n != 0 || b != 0 {
		t.Fatalf("leaked budget %d/%d", n, b)
	}
}

func TestRangePrefetchHonorsGlobalLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bytes int64
		loads int
		want  int
	}{
		{"byte-budget", 16, 8, 1}, {"load-budget", 1024, 1, 1}, {"block-too-large", 1, 8, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := blob(64)
			ctx, cancel := prefetchContext(t)
			defer cancel()
			started := make(chan int64, 4)
			e := prefetchEngine(t, content, 4, blockFetchFunc(func(ctx context.Context, _ string, start, end int64) (fetch.Range, error) {
				started <- start / 16
				<-ctx.Done()
				return fetch.Range{}, ctx.Err()
			}))
			e.prefetch.maxBytes = tc.bytes
			e.prefetch.maxLoads = tc.loads
			done := make(chan error, 1)
			go func() { done <- e.Serve(ctx, io.Discard, prefetchURL, 0, 63) }()
			for i := 0; i < tc.want+1; i++ {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("expected loads not started")
				}
			}
			if n, b := budgetUsage(e); n != tc.want || b > tc.bytes {
				t.Fatalf("usage %d/%d", n, b)
			}
			select {
			case extra := <-started:
				t.Fatalf("unexpected block %d", extra)
			default:
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if n, b := budgetUsage(e); n != 0 || b != 0 {
				t.Fatalf("leaked budget %d/%d", n, b)
			}
		})
	}
}

func TestRangePrefetchFailureIsObservedInOrder(t *testing.T) {
	content := blob(64)
	ctx, cancel := prefetchContext(t)
	defer cancel()
	sentinel := errors.New("later block failed")
	failed := make(chan struct{})
	e := prefetchEngine(t, content, 3, blockFetchFunc(func(ctx context.Context, _ string, start, end int64) (fetch.Range, error) {
		if start == 32 {
			close(failed)
			return fetch.Range{}, sentinel
		}
		if start == 0 {
			select {
			case <-failed:
			case <-ctx.Done():
				return fetch.Range{}, ctx.Err()
			}
		}
		return prefetchResult(content, start, end), nil
	}))
	var output bytes.Buffer
	if err := e.Serve(ctx, &output, prefetchURL, 0, 63); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), content[:32]) {
		t.Fatalf("failed ahead block discarded valid prefix: %d", output.Len())
	}
	if e.store.Has(prefetchKey(prefetchURL, 2)) {
		t.Fatal("failed block cached")
	}
	if n, b := budgetUsage(e); n != 0 || b != 0 {
		t.Fatalf("leaked budget %d/%d", n, b)
	}
}

func TestRangePrefetchCancelAndJoin(t *testing.T) {
	for _, writeFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller-canceled", true: "writer-failed"}[writeFailure], func(t *testing.T) {
			content := blob(48)
			ctx, cancel := prefetchContext(t)
			defer cancel()
			started := make(chan struct{}, 2)
			canceled := make(chan struct{}, 2)
			exit := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(exit) }) }
			defer unblock()
			sentinel := errors.New("downstream write failed")
			e := prefetchEngine(t, content, 3, blockFetchFunc(func(ctx context.Context, _ string, start, end int64) (fetch.Range, error) {
				if start == 0 {
					return prefetchResult(content, start, end), nil
				}
				started <- struct{}{}
				<-ctx.Done()
				canceled <- struct{}{}
				<-exit
				return fetch.Range{}, ctx.Err()
			}))
			writing := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- e.Serve(ctx, rangeWriteFunc(func(b []byte) (int, error) {
					for i := 0; i < 2; i++ {
						select {
						case <-started:
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}
					close(writing)
					if writeFailure {
						return 0, sentinel
					}
					<-ctx.Done()
					return 0, ctx.Err()
				}), prefetchURL, 0, 47)
			}()
			waitBlockEvent(t, ctx, writing)
			if !writeFailure {
				cancel()
			}
			for i := 0; i < 2; i++ {
				select {
				case <-canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("worker not canceled")
				}
			}
			select {
			case err := <-done:
				t.Fatalf("Serve returned before workers exited: %v", err)
			default:
			}
			unblock()
			err := <-done
			want := error(context.Canceled)
			if writeFailure {
				want = sentinel
			}
			if !errors.Is(err, want) {
				t.Fatalf("error %v, want %v", err, want)
			}
			if n, b := budgetUsage(e); n != 0 || b != 0 {
				t.Fatalf("leaked budget %d/%d", n, b)
			}
		})
	}
}

func TestRangePrefetchDisabledAndShortWrite(t *testing.T) {
	content := blob(32)
	ctx, cancel := prefetchContext(t)
	defer cancel()
	var calls int
	e := prefetchEngine(t, content, 1, blockFetchFunc(func(_ context.Context, _ string, start, end int64) (fetch.Range, error) {
		calls++
		return prefetchResult(content, start, end), nil
	}))
	if err := e.Serve(ctx, rangeWriteFunc(func(b []byte) (int, error) { return len(b) - 1, nil }), prefetchURL, 0, 31); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("disabled prefetch loaded %d blocks", calls)
	}
}

func TestPrefetchOptions(t *testing.T) {
	for _, opt := range []Options{{PrefetchWindow: -1}, {PrefetchWindow: 65}, {PrefetchBytes: -1}, {PrefetchLoads: -1}} {
		opt.Chunk = testCfg()
		opt.Store = openStoreAt(t)
		opt.Fetcher = newFetcher()
		if _, err := New(opt); err == nil {
			t.Fatalf("invalid options accepted: %+v", opt)
		}
	}
}

func TestRangePrefetchSharesDemandFlight(t *testing.T) {
	content := blob(48)
	ctx, cancel := prefetchContext(t)
	defer cancel()
	origin := countingOrigin(t, content, nil)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	parent := httptest.NewServer(&peer.Server{Src: func(ctx context.Context, req peer.BlockRequest) ([]byte, bool, error) {
		if req.Key.Block == 1 {
			calls.Add(1)
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
		start := int(req.Key.Block) * 16
		return content[start : start+16], true, nil
	}})
	defer parent.Close()
	defer unblock()
	e, _ := tailEngine(t, peer.NewClient(), parent, origin.URL)
	longDone := make(chan error, 1)
	var output bytes.Buffer
	go func() { longDone <- e.Serve(ctx, &output, origin.URL, 0, 47) }()
	waitBlockEvent(t, ctx, started)
	joined := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	shortDone := make(chan error, 1)
	go func() {
		var b bytes.Buffer
		err := e.Serve(joined, &b, origin.URL, 20, 23)
		if err == nil && !bytes.Equal(b.Bytes(), content[20:24]) {
			err = errors.New("wrong demand bytes")
		}
		shortDone <- err
	}()
	waitBlockEvent(t, ctx, joined.joined)
	unblock()
	if err := <-shortDone; err != nil {
		t.Fatal(err)
	}
	if err := <-longDone; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || !bytes.Equal(output.Bytes(), content) {
		t.Fatalf("flight duplicated: calls %d", calls.Load())
	}
}

type notifyingPrefetchStore struct {
	store.Store
	published chan store.BlockKey
}

func (s *notifyingPrefetchStore) Put(key store.BlockKey, b []byte) error {
	err := s.Store.Put(key, b)
	if err == nil {
		s.published <- key
	}
	return err
}

func TestRangePrefetchKeepsPublishedCacheOnCancel(t *testing.T) {
	content := blob(48)
	ctx, cancel := prefetchContext(t)
	defer cancel()
	e := prefetchEngine(t, content, 3, blockFetchFunc(func(ctx context.Context, _ string, start, end int64) (fetch.Range, error) {
		if start == 32 {
			<-ctx.Done()
			return fetch.Range{}, ctx.Err()
		}
		return prefetchResult(content, start, end), nil
	}))
	published := make(chan store.BlockKey, 3)
	e.store = &notifyingPrefetchStore{Store: e.store, published: published}
	err := e.Serve(ctx, rangeWriteFunc(func(b []byte) (int, error) {
		for {
			select {
			case key := <-published:
				if key.Block == 1 {
					cancel()
					return 0, ctx.Err()
				}
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	}), prefetchURL, 0, 47)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	data, ok, err := e.store.Get(prefetchKey(prefetchURL, 1))
	if err != nil || !ok || !bytes.Equal(data, content[16:32]) {
		t.Fatalf("successful prefetch lost on cancel: %x %v %v", data, ok, err)
	}
	if e.store.Has(prefetchKey(prefetchURL, 2)) {
		t.Fatal("canceled incomplete block cached")
	}
	if n, b := budgetUsage(e); n != 0 || b != 0 {
		t.Fatalf("leaked budget %d/%d", n, b)
	}
}
