package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/tracker"
)

type waitingReaderContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *waitingReaderContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func refreshClient(t *testing.T, handler http.HandlerFunc) (*Engine, *cluster.View, *metrics.Registry) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume the POST body so net/http can observe a canceled connection
		// while a test handler waits. The real tracker also consumes its JOIN.
		_, _ = io.Copy(io.Discard, r.Body)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	p := cluster.NewStaticProvider(cluster.Member{ID: "tracker", Addr: strings.TrimPrefix(srv.URL, "http://"), State: cluster.Ready, Weight: 1})
	reg := metrics.NewRegistry()
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(), SelfID: "reader", Cluster: p, TrackerClient: tracker.NewClient(), Metrics: NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	return e, p.Current(), reg
}

func TestReaderRefreshCoalescesAndCancelsWaiters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	e, view, reg := refreshClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"A", "reader"}, TTLMs: 6000, EpochS: 1})
	})
	done := make(chan []string, 17)
	go func() { done <- e.readers(ctx, view, "object", 1) }()
	waitBlockEvent(t, ctx, entered)
	var waiting []*waitingReaderContext
	for i := 0; i < 16; i++ {
		wc := &waitingReaderContext{Context: ctx, waiting: make(chan struct{})}
		waiting = append(waiting, wc)
		go func() { done <- e.readers(wc, view, "object", 1) }()
	}
	for _, wc := range waiting {
		waitBlockEvent(t, ctx, wc.waiting)
	}
	abandoned, stop := context.WithCancel(ctx)
	wc := &waitingReaderContext{Context: abandoned, waiting: make(chan struct{})}
	canceled := make(chan []string, 1)
	go func() { canceled <- e.readers(wc, view, "object", 1) }()
	waitBlockEvent(t, ctx, wc.waiting)
	stop()
	if got := <-canceled; got != nil {
		t.Fatal("canceled waiter returned a snapshot", got)
	}
	unblock()
	for i := 0; i < 17; i++ {
		if got := <-done; !reflect.DeepEqual(got, []string{"A", "reader"}) {
			t.Error(got)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate JOIN", calls.Load())
	}
	var rendered bytes.Buffer
	reg.Render(&rendered)
	if !strings.Contains(rendered.String(), "dart_reader_snapshot_size_count 1\n") || !strings.Contains(rendered.String(), "dart_reader_snapshot_size_sum 2\n") {
		t.Fatal(rendered.String())
	}
	// Pin a fresh deadline: this assertion must not depend on the scheduler
	// finishing the preceding checks inside the 50 ms startup interval.
	e.rsMu.Lock()
	fresh := *e.rs["object"]
	fresh.expiresAt = time.Now().Add(time.Minute)
	e.rs["object"] = &fresh
	e.rsMu.Unlock()
	e.readers(ctx, view, "object", 1)
	if calls.Load() != 1 {
		t.Fatal("fresh cache caused JOIN")
	}
	// Expiration alone does no work; only a subsequent routing lookup refreshes.
	e.rsMu.Lock()
	old := *e.rs["object"]
	old.expiresAt = time.Now().Add(-time.Second)
	e.rs["object"] = &old
	e.rsMu.Unlock()
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("background refresh")
	}
	e.readers(ctx, view, "object", 1)
	if calls.Load() != 2 {
		t.Fatal("expired view did not refresh")
	}
}

func TestReaderRefreshCanceledLeaderRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{})
	var calls atomic.Int32
	e, view, _ := refreshClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-r.Context().Done():
			case <-ctx.Done():
			}
			return
		}
		json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"new"}, TTLMs: 6000})
	})
	leader, stop := context.WithCancel(ctx)
	defer stop()
	first := make(chan []string, 1)
	go func() { first <- e.readers(leader, view, "object", 1) }()
	waitBlockEvent(t, ctx, entered)
	wc := &waitingReaderContext{Context: ctx, waiting: make(chan struct{})}
	second := make(chan []string, 1)
	go func() { second <- e.readers(wc, view, "object", 1) }()
	waitBlockEvent(t, ctx, wc.waiting)
	stop()
	if got := <-first; got != nil {
		t.Fatal(got)
	}
	if got := <-second; !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatal(got)
	}
	if calls.Load() != 2 || len(e.rsCalls) != 0 {
		t.Fatal("refresh flight leaked or was not replaced", calls.Load())
	}
}

func TestReaderRefreshOldTrackerCannotOverwriteNew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	e, oldView, _ := refreshClient(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"old"}, TTLMs: 6000, EpochS: 999})
	})
	newServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"new"}, TTLMs: 6000, EpochS: 1})
	}))
	defer newServer.Close()
	newView := cluster.NewStaticProvider(cluster.Member{ID: "new-tracker", Addr: strings.TrimPrefix(newServer.URL, "http://"), State: cluster.Ready, Weight: 1}).Current()
	done := make(chan []string, 1)
	go func() { done <- e.readers(ctx, oldView, "object", 1) }()
	waitBlockEvent(t, ctx, entered)
	if got := e.readers(ctx, newView, "object", 1); !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatal(got)
	}
	unblock()
	if got := <-done; !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatal("old caller lost its view", got)
	}
	if got := e.rs["object"].nodes; !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatal("late old tracker replaced newer view", got)
	}
}

func TestReaderRefreshLeaseBounds(t *testing.T) {
	for _, tc := range []struct {
		ttl  int64
		want time.Duration
	}{{0, 4 * time.Second}, {math.MinInt64, 4 * time.Second}, {math.MaxInt64, tracker.MaxLeaseTTL}, {1, time.Millisecond}} {
		e, view, _ := refreshClient(t, func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"reader"}, TTLMs: tc.ttl})
		})
		e.readers(context.Background(), view, "object", 1)
		rs := e.rs["object"]
		if rs == nil || rs.leaseUntil.Sub(rs.cachedAt) != tc.want || rs.interval > tc.want/2 {
			t.Fatal(tc, rs)
		}
	}
}

func TestReaderRefreshBackoffAndLeaseReset(t *testing.T) {
	var calls atomic.Int32
	e, view, _ := refreshClient(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		// Change both the published list and its epoch on every refresh.
		json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"reader", strings.Repeat("A", int(n))}, TTLMs: 6000, EpochS: uint64(n)})
	})
	// Every successful refresh advances solely with time. Changing the reader
	// list and publication epoch must not reset this schedule.
	for _, want := range []time.Duration{50, 100, 200, 400, 800, 1600, 3000, 3000} {
		e.readers(context.Background(), view, "object", 1)
		s := e.rs["object"]
		if s.interval != want*time.Millisecond {
			t.Fatal("interval", s.interval, want)
		}
		copy := *s
		copy.expiresAt = time.Now().Add(-time.Second)
		e.rs["object"] = &copy
	}
	s := *e.rs["object"]
	s.leaseUntil = time.Now().Add(-time.Second)
	e.rs["object"] = &s
	e.readers(context.Background(), view, "object", 1)
	if e.rs["object"].interval != 50*time.Millisecond || calls.Load() != 9 {
		t.Fatal("idle lease did not restart schedule")
	}
	// A short grant wins over the startup interval and the previous backoff.
	if d := readerRefreshDelay(0, time.Millisecond); d != time.Millisecond {
		t.Fatal(d)
	}
	if d := readerRefreshDelay(time.Second, time.Millisecond); d != time.Millisecond {
		t.Fatal(d)
	}
}

func TestReaderRefreshFailureDoesNotCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	e, view, _ := refreshClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(tracker.JoinResponse{Readers: []string{"reader"}, TTLMs: 6000})
	})
	leader := make(chan []string, 1)
	go func() { leader <- e.readers(ctx, view, "object", 1) }()
	waitBlockEvent(t, ctx, entered)
	wc := &waitingReaderContext{Context: ctx, waiting: make(chan struct{})}
	waiter := make(chan []string, 1)
	go func() { waiter <- e.readers(wc, view, "object", 1) }()
	waitBlockEvent(t, ctx, wc.waiting)
	unblock()
	if a, b := <-leader, <-waiter; a != nil || b != nil {
		t.Fatal(a, b)
	}
	if calls.Load() != 1 || len(e.rsCalls) != 0 || len(e.rs) != 0 {
		t.Fatal("failure retained or duplicated")
	}
	if got := e.readers(ctx, view, "object", 1); !reflect.DeepEqual(got, []string{"reader"}) || calls.Load() != 2 {
		t.Fatal("next demand did not retry", got)
	}
}
