package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
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

// Done is evaluated by the in-flight wait select. No sleeps are needed to
// release a publisher only after a particular caller has joined it.
type blockJoinContext struct {
	context.Context
	joined chan struct{}
	once   sync.Once
}

func (c *blockJoinContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
}

func waitBlockEvent(t *testing.T, ctx context.Context, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal("block load did not reach its barrier")
	}
}

// Put stays inside the leader handler until publication completes. The
// optional rejection tests joining successful bytes even without admission.
type blockPublishStore struct {
	store.Store
	started chan struct{}
	release chan struct{}
	reject  bool
}

func (s *blockPublishStore) Put(k store.BlockKey, b []byte) error {
	close(s.started)
	<-s.release
	if s.reject {
		return nil
	}
	return s.Store.Put(k, b)
}

func TestConcurrentSubBlockPeerMissesSharePublication(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "cached"
		if reject {
			name = "admission-declined"
		}
		t.Run(name, func(t *testing.T) {
			const block = 4 << 20
			const piece = 1 << 20
			content := blob(block)
			var originCalls int64
			origin := countingOrigin(t, content, &originCalls)
			oid, _ := chunk.ObjectID(origin.URL)
			key := store.BlockKey{Chunk: chunk.ChunkKey("dart", oid, 0)}
			root, err := store.OpenMem(store.MemOptions{SlotSize: block, Slots: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if err := root.Put(key, content); err != nil {
				t.Fatal(err)
			}
			var peerCalls atomic.Int64
			source := peer.StoreSource(root)
			parent := httptest.NewServer(&peer.Server{Src: func(ctx context.Context, req peer.BlockRequest) ([]byte, bool, error) {
				peerCalls.Add(1)
				return source(ctx, req)
			}})
			defer parent.Close()
			ranked := hashring.Rank(key.Chunk, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}})
			provider := cluster.NewStaticProvider(
				cluster.Member{ID: ranked[0].ID, Addr: strings.TrimPrefix(parent.URL, "http://"), Weight: 1, State: cluster.Ready},
				cluster.Member{ID: ranked[1].ID, Addr: "127.0.0.1:1", Weight: 1, State: cluster.Ready})
			mem, err := store.OpenMem(store.MemOptions{SlotSize: block, Slots: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer mem.Close()
			st := &blockPublishStore{Store: mem, started: make(chan struct{}), release: make(chan struct{}), reject: reject}
			var once sync.Once
			unblock := func() { once.Do(func() { close(st.release) }) }
			defer unblock()
			mx := NewMetrics(metrics.NewRegistry())
			e, err := New(Options{Chunk: chunk.Config{BlockSize: block, ChunkSize: 256 << 20}, Store: st,
				Fetcher: newFetcher(), Cluster: provider, Peer: peer.NewClient(), SelfID: ranked[1].ID, Metrics: mx})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := e.Size(ctx, origin.URL); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 4)
			read := func(ctx context.Context, i int) {
				var b bytes.Buffer
				err := e.Serve(ctx, &b, origin.URL, int64(i*piece), int64((i+1)*piece-1))
				if err == nil && !bytes.Equal(b.Bytes(), content[i*piece:(i+1)*piece]) {
					err = errors.New("wrong sub-block bytes")
				}
				done <- err
			}
			go read(ctx, 0)
			waitBlockEvent(t, ctx, st.started)
			for i := 1; i < 4; i++ {
				joiner := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
				go read(joiner, i)
				waitBlockEvent(t, ctx, joiner.joined)
			}
			unblock()
			for i := 0; i < 4; i++ {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			if peerCalls.Load() != 1 || mx.peerBytes.Value() != block || mx.peerHits.Value() != 4 || mx.clientBytes.Value() != block {
				t.Fatalf("peer calls=%d bytes=%d reads=%d client=%d", peerCalls.Load(), mx.peerBytes.Value(), mx.peerHits.Value(), mx.clientBytes.Value())
			}
			if atomic.LoadInt64(&originCalls) != 1 || mx.originBytes.Value() != 0 || mem.Has(key) == reject {
				t.Fatal("wrong origin traffic or cache publication")
			}
			if !reject {
				if err := e.Serve(ctx, io.Discard, origin.URL, 0, piece-1); err != nil || peerCalls.Load() != 1 || mx.cacheHits.Value() != 1 {
					t.Fatal("a completed flight did not leave a usable cache entry")
				}
			}
		})
	}
}

func TestBlockLoadsCancellation(t *testing.T) {
	for _, cancelLeader := range []bool{false, true} {
		name := "waiter"
		if cancelLeader {
			name = "leader"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			leadCtx, cancelLead := context.WithCancel(ctx)
			defer cancelLead()
			waitCtx, cancelWait := context.WithCancel(ctx)
			defer cancelWait()
			var g blockLoads
			var calls atomic.Int64
			started := make(chan struct{}, 3)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			run := func(ctx context.Context, done chan error) {
				_, _, err := g.do(ctx, blockLoadKey{}, "same", func() (blockRead, error) {
					calls.Add(1)
					started <- struct{}{}
					select {
					case <-release:
						return blockRead{data: []byte("valid")}, nil
					case <-ctx.Done():
						return blockRead{}, ctx.Err()
					}
				})
				done <- err
			}
			leadDone, waitDone, liveDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
			go run(leadCtx, leadDone)
			waitBlockEvent(t, ctx, started)
			join := func(c context.Context, done chan error) {
				j := &blockJoinContext{Context: c, joined: make(chan struct{})}
				go run(j, done)
				waitBlockEvent(t, ctx, j.joined)
			}
			join(waitCtx, waitDone)
			if !cancelLeader {
				join(ctx, liveDone)
			}
			if cancelLeader {
				cancelLead()
				if err := <-leadDone; !errors.Is(err, context.Canceled) {
					t.Fatalf("leader = %v", err)
				}
				waitBlockEvent(t, ctx, started) // one live caller takes over
			} else {
				cancelWait()
				if err := <-waitDone; !errors.Is(err, context.Canceled) {
					t.Fatalf("waiter = %v", err)
				}
			}
			unblock()
			other := leadDone
			wantCalls := int64(1)
			if cancelLeader {
				other, wantCalls = waitDone, 2
			} else if err := <-liveDone; err != nil {
				t.Fatal(err)
			}
			if err := <-other; err != nil || calls.Load() != wantCalls || len(g.m) != 0 {
				t.Fatalf("other=%v calls=%d flights=%d", err, calls.Load(), len(g.m))
			}
		})
	}
}

func TestBlockLoadsRetryFailuresWithOwnURL(t *testing.T) {
	for _, differentURL := range []bool{false, true} {
		name := "same-url"
		if differentURL {
			name = "different-credential-or-endpoint"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var g blockLoads
			failure := errors.New("failed credential or endpoint")
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			leader := make(chan error, 1)
			go func() {
				_, _, err := g.do(ctx, blockLoadKey{}, "old", func() (blockRead, error) {
					close(started)
					<-release
					return blockRead{}, failure
				})
				leader <- err
			}()
			waitBlockEvent(t, ctx, started)
			url := "old"
			if differentURL {
				url = "renewed"
			}
			joiner := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
			done := make(chan error, 1)
			var retried bool
			go func() {
				_, _, err := g.do(joiner, blockLoadKey{}, url, func() (blockRead, error) {
					retried = true
					return blockRead{data: []byte("valid")}, nil
				})
				done <- err
			}()
			waitBlockEvent(t, ctx, joiner.joined)
			unblock()
			if err := <-leader; !errors.Is(err, failure) {
				t.Fatal(err)
			}
			err := <-done
			if retried != differentURL || (differentURL && err != nil) || (!differentURL && !errors.Is(err, failure)) {
				t.Fatalf("retry=%v err=%v", retried, err)
			}
			if _, shared, err := g.do(ctx, blockLoadKey{}, "old", func() (blockRead, error) { return blockRead{}, nil }); shared || err != nil {
				t.Fatal("a completed error was retained")
			}
		})
	}
}

func TestBlockLoadsSeparateKeysAndRelayDepth(t *testing.T) {
	for _, other := range []blockLoadKey{{block: store.BlockKey{Block: 1}}, {hop: 1}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var g blockLoads
		r, _, err := g.do(ctx, blockLoadKey{}, "url", func() (blockRead, error) {
			// A same-key higher-hop relay must not join the outer call and
			// deadlock before the hop limit can advance under membership skew.
			r, shared, err := g.do(ctx, other, "url", func() (blockRead, error) { return blockRead{data: []byte("valid")}, nil })
			if shared {
				t.Error("different key/depth shared a flight")
			}
			return r, err
		})
		cancel()
		if err != nil || string(r.data) != "valid" {
			t.Fatalf("different block/depth = %q, %v", r.data, err)
		}
	}
}

func TestBlockLoadsDetachLeaderBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var g blockLoads
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	leader, waiter := make(chan blockRead, 1), make(chan blockRead, 1)
	go func() {
		r, _, _ := g.do(ctx, blockLoadKey{}, "url", func() (blockRead, error) {
			close(started)
			<-release
			return blockRead{data: []byte("valid")}, nil
		})
		leader <- r
	}()
	waitBlockEvent(t, ctx, started)
	joiner := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	go func() {
		r, _, _ := g.do(joiner, blockLoadKey{}, "url", func() (blockRead, error) {
			t.Error("waiter performed a separate load")
			return blockRead{}, nil
		})
		waiter <- r
	}()
	waitBlockEvent(t, ctx, joiner.joined)
	unblock()
	r := <-leader
	r.data[0] = 'X'
	if got := <-waiter; string(got.data) != "valid" {
		t.Fatalf("leader mutation changed waiter bytes: %q", got.data)
	}
}

type delayedBlockMiss struct {
	store.Store
	first   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (s *delayedBlockMiss) Get(k store.BlockKey) ([]byte, bool, error) {
	b, ok, err := s.Store.Get(k)
	if !ok && err == nil && s.first.CompareAndSwap(false, true) {
		close(s.started)
		<-s.release // the miss was observed before another caller published
	}
	return b, ok, err
}

func TestBlockLoadRechecksCacheAfterAdmission(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	var calls atomic.Int64
	parent := httptest.NewServer(&peer.Server{Src: func(_ context.Context, _ peer.BlockRequest) ([]byte, bool, error) {
		calls.Add(1)
		return append([]byte(nil), content...), true, nil
	}})
	defer parent.Close()
	e, _ := tailEngine(t, peer.NewClient(), parent, origin.URL)
	st := &delayedBlockMiss{Store: e.store, started: make(chan struct{}), release: make(chan struct{})}
	e.store = st
	e.mx = NewMetrics(metrics.NewRegistry())
	var once sync.Once
	unblock := func() { once.Do(func() { close(st.release) }) }
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.Size(ctx, origin.URL); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Serve(ctx, io.Discard, origin.URL, 0, 15) }()
	waitBlockEvent(t, ctx, st.started)
	if err := e.Serve(ctx, io.Discard, origin.URL, 0, 15); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := <-done; err != nil || calls.Load() != 1 || e.mx.cacheHits.Value() != 1 {
		t.Fatalf("late miss=%v peer calls=%d cache reads=%d", err, calls.Load(), e.mx.cacheHits.Value())
	}
}

func TestBlockLoadPanicReleasesAdmission(t *testing.T) {
	var g blockLoads
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected loader panic")
			}
		}()
		_, _, _ = g.do(context.Background(), blockLoadKey{}, "url", func() (blockRead, error) { panic("failed source") })
	}()
	if _, shared, err := g.do(context.Background(), blockLoadKey{}, "url", func() (blockRead, error) { return blockRead{}, nil }); err != nil || shared {
		t.Fatalf("panic left a flight behind: shared=%v err=%v", shared, err)
	}
}

func TestCoalescedPeerRefusalRetriesCallerCredential(t *testing.T) {
	content := blob(16)
	var refusedOrigin atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=0-0" && r.URL.Query().Get("sig") == "expired" {
			refusedOrigin.Add(1)
			http.Error(w, "expired", http.StatusForbidden)
			return
		}
		http.ServeContent(w, r, "blob", time.Time{}, bytes.NewReader(content))
	}))
	defer origin.Close()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var parentCalls atomic.Int64
	parent := httptest.NewServer(&peer.Server{Src: func(ctx context.Context, req peer.BlockRequest) ([]byte, bool, error) {
		parentCalls.Add(1)
		if strings.Contains(req.URL, "sig=expired") {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
			return nil, false, asRelayError(&fetch.StatusError{Code: http.StatusForbidden})
		}
		return append([]byte(nil), content...), true, nil
	}})
	defer parent.Close()
	defer unblock()
	client := peer.NewClient()
	client.Breaker = peer.NewBreaker(peer.BreakerOptions{FailureThreshold: 1})
	e, _ := tailEngine(t, client, parent, origin.URL)
	reg := metrics.NewRegistry()
	e.mx = NewMetrics(reg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.Size(ctx, origin.URL+"?sig=renewed"); err != nil {
		t.Fatal(err)
	}
	leader, waiter := make(chan error, 1), make(chan error, 1)
	go func() { leader <- e.Serve(ctx, io.Discard, origin.URL+"?sig=expired", 0, 15) }()
	waitBlockEvent(t, ctx, started)
	joiner := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	go func() {
		var b bytes.Buffer
		err := e.Serve(joiner, &b, origin.URL+"?sig=renewed", 0, 15)
		if err == nil && !bytes.Equal(b.Bytes(), content) {
			err = errors.New("renewed caller received wrong bytes")
		}
		waiter <- err
	}()
	waitBlockEvent(t, ctx, joiner.joined)
	waitStart := time.Now()
	<-time.After(50 * time.Millisecond) // deliberate wait measured by the retry's histogram
	waited := time.Since(waitStart).Seconds()
	unblock()
	var status *fetch.StatusError
	if err := <-leader; !errors.As(err, &status) || status.Code != http.StatusForbidden {
		t.Fatalf("expired caller = %v", err)
	}
	if err := <-waiter; err != nil || parentCalls.Load() != 2 || refusedOrigin.Load() != 1 {
		t.Fatalf("renewed caller=%v parent=%d refused origin=%d", err, parentCalls.Load(), refusedOrigin.Load())
	}
	if client.Breaker.State(strings.TrimPrefix(parent.URL, "http://")) != peer.BreakerClosed {
		t.Fatal("credential refusal quarantined the healthy parent")
	}
	var text bytes.Buffer
	if err := reg.Render(&text); err != nil {
		t.Fatal(err)
	}
	var observed float64
	for _, line := range strings.Split(text.String(), "\n") {
		if strings.HasPrefix(line, `dart_block_fetch_seconds_sum{source="peer"} `) {
			observed, _ = strconv.ParseFloat(strings.Fields(line)[1], 64)
		}
	}
	if observed < waited || e.mx.peerHits.Value() != 1 || e.mx.peerBytes.Value() != 16 {
		t.Fatalf("retry wait=%fs observed=%fs reads=%d bytes=%d", waited, observed, e.mx.peerHits.Value(), e.mx.peerBytes.Value())
	}
}

type blockFetchFunc func(context.Context, string, int64, int64) (fetch.Range, error)

func (f blockFetchFunc) Fetch(ctx context.Context, url string, start, end int64) (fetch.Range, error) {
	return f(ctx, url, start, end)
}

type witnessedOriginFlight struct {
	inner   *fetch.Coalescing
	waiting chan (<-chan struct{})
}

func (f *witnessedOriginFlight) Fetch(ctx context.Context, url string, start, end int64) (fetch.Range, error) {
	j := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	f.waiting <- j.joined
	return f.inner.Fetch(j, url, start, end)
}

func TestMixedDepthOriginFlightsReturnOwnedBytes(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	parent := httptest.NewServer(http.NotFoundHandler())
	defer parent.Close()
	e, _ := tailEngine(t, peer.NewClient(), parent, origin.URL)
	e.mx = NewMetrics(metrics.NewRegistry())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.Size(ctx, origin.URL); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int64
	f := &witnessedOriginFlight{waiting: make(chan (<-chan struct{}), 2), inner: &fetch.Coalescing{F: blockFetchFunc(
		func(ctx context.Context, _ string, _, _ int64) (fetch.Range, error) {
			calls.Add(1)
			select {
			case <-release:
				return fetch.Range{Data: content, Total: 16}, nil
			case <-ctx.Done():
				return fetch.Range{}, ctx.Err()
			}
		})}}
	e.fetcher = f
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 2)
	oid, _ := chunk.ObjectID(origin.URL)
	for _, hop := range []int{0, 1} {
		go func(hop int) {
			data, err := e.block(ctx, origin.URL, oid, 16, 0, hop)
			done <- result{data, err}
		}(hop)
	}
	// Both relay depths now join the engine's immutable origin lease flight;
	// only its one backend consumer enters the Fetcher-level compatibility flight.
	select {
	case waiting := <-f.waiting:
		waitBlockEvent(t, ctx, waiting)
	case <-ctx.Done():
		t.Fatal("origin backend did not start")
	}
	for {
		e.origins.mu.Lock()
		joined := false
		for _, c := range e.origins.m {
			joined = c.refs == 2
		}
		e.origins.mu.Unlock()
		if joined {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("mixed-depth loads did not join the origin lease flight")
		}
		runtime.Gosched()
	}
	unblock()
	a, b := <-done, <-done
	if a.err != nil || b.err != nil || !bytes.Equal(a.data, content) || !bytes.Equal(b.data, content) {
		t.Fatalf("origin results: %v, %v", a.err, b.err)
	}
	a.data[0] = 255
	if b.data[0] != 0 || content[0] != 0 || calls.Load() != 1 || e.mx.originBytes.Value() != 16 || e.mx.originGet.Value() != 2 {
		t.Fatalf("shared origin buffer escaped: other=%d origin=%d calls=%d reads=%d wire=%d", b.data[0], content[0], calls.Load(), e.mx.originGet.Value(), e.mx.originBytes.Value())
	}
}

func TestBlockLoadAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var g blockLoads
	_, _, err := g.do(ctx, blockLoadKey{}, "url", func() (blockRead, error) {
		t.Error("canceled caller started new work")
		return blockRead{}, nil
	})
	if !errors.Is(err, context.Canceled) || len(g.m) != 0 {
		t.Fatalf("canceled caller = %v, flights=%d", err, len(g.m))
	}
}

func TestCoalescedBlockValidatesBeforeSharingOriginFallback(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	parent := httptest.NewServer(&peer.Server{Src: func(_ context.Context, _ peer.BlockRequest) ([]byte, bool, error) {
		return content[:8], true, nil
	}})
	defer parent.Close()
	e, _ := tailEngine(t, peer.NewClient(), parent, origin.URL)
	st := &blockPublishStore{Store: e.store, started: make(chan struct{}), release: make(chan struct{})}
	e.store = st
	e.mx = NewMetrics(metrics.NewRegistry())
	var once sync.Once
	unblock := func() { once.Do(func() { close(st.release) }) }
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.Size(ctx, origin.URL); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	read := func(ctx context.Context) {
		var b bytes.Buffer
		err := e.Serve(ctx, &b, origin.URL, 0, 15)
		if err == nil && !bytes.Equal(b.Bytes(), content) {
			err = errors.New("invalid peer data shared instead of validated origin bytes")
		}
		done <- err
	}
	go read(ctx)
	waitBlockEvent(t, ctx, st.started)
	joiner := &blockJoinContext{Context: ctx, joined: make(chan struct{})}
	go read(joiner)
	waitBlockEvent(t, ctx, joiner.joined)
	unblock()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if e.mx.originGet.Value() != 2 || e.mx.originBytes.Value() != 16 || e.mx.peerHits.Value() != 0 {
		t.Fatalf("origin reads=%d bytes=%d peer reads=%d", e.mx.originGet.Value(), e.mx.originBytes.Value(), e.mx.peerHits.Value())
	}
}
