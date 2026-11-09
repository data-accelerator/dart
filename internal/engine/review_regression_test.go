package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

func TestRootOnlyWarmBlocksIgnorePlacement(t *testing.T) {
	const url = "http://origin.invalid/warm"
	content := blob(16)
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprint("owned=", owned), func(t *testing.T) {
			p := cluster.NewStaticProvider(cluster.Member{ID: "other", Addr: "other:1", Weight: 1, State: cluster.Ready, Role: cluster.RootOnly})
			st := openStoreAt(t)
			key := blockKeyFor(url)
			if err := st.Put(key, content); err != nil {
				t.Fatal(err)
			}
			f := &forbiddenOrigin{}
			e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: f, Cluster: p, Peer: peer.NewClient(), SelfID: "self", Role: cluster.RootOnly})
			if err != nil {
				t.Fatal(err)
			}
			for _, req := range []peer.BlockRequest{{Key: key}, {Key: key, URL: url, HasEpoch: true, Epoch: p.Current().Epoch() ^ 1}} {
				l, held, err := e.peerRead(context.Background(), req, owned)
				if err != nil || !held || !bytes.Equal(l.Bytes(), content) {
					t.Errorf("warm bytes rejected: held=%v err=%v", held, err)
				}
				l.Release()
			}
			// Key/URL validation still precedes warm lookup.
			l, held, err := e.peerRead(context.Background(), peer.BlockRequest{Key: key, URL: "http://origin.invalid/other"}, owned)
			l.Release()
			if err != nil || held {
				t.Fatal("invalid identity served", held, err)
			}
			st.Delete(key)
			l, held, err = e.peerRead(context.Background(), peer.BlockRequest{Key: key, URL: url}, owned)
			l.Release()
			if err != nil || held || f.calls.Load() != 0 {
				t.Fatal("non-owner root-only cold request relayed", held, err)
			}
		})
	}
}

func TestRootFallbackAfterHedgeAdmissionDeclines(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			content := blob(16)
			const url = "http://origin.invalid/hedge-admission"
			key := blockKeyFor(url)
			oid, _ := chunk.ObjectID(url)
			var rootCalls, parentCalls atomic.Int32
			root := httptest.NewServer(&peer.Server{Src: func(_ context.Context, req peer.BlockRequest) ([]byte, bool, error) {
				rootCalls.Add(1)
				if !req.Supply {
					t.Error("root fallback not terminal")
				}
				return content, true, nil
			}})
			defer root.Close()
			release := make(chan struct{})
			parentEntered := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				parentCalls.Add(1)
				close(parentEntered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				if status == http.StatusServiceUnavailable {
					w.Header().Set(peer.HeaderSourceFailure, "1")
				}
				w.WriteHeader(status)
			}))
			defer parent.Close()
			defer unblock()
			order := hashring.Rank(key.Chunk, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}})
			p := cluster.NewStaticProvider(
				cluster.Member{ID: "R", Addr: root.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly},
				cluster.Member{ID: order[0].ID, Addr: parent.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.NonRoot},
				cluster.Member{ID: order[1].ID, Addr: "unused:1", Weight: 1, State: cluster.Ready, Role: cluster.NonRoot})
			st, err := store.OpenTiered(store.TieredOptions{Memory: true, SlotSize: 16, Slots: 1, OwnedFraction: 1, OwnedFractionSet: true})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			mx := NewMetrics(metrics.NewRegistry())
			e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: &forbiddenOrigin{}, Cluster: p, Peer: peer.NewClient(), SelfID: order[1].ID, Role: cluster.NonRoot, Fanout: 1, Hedge: true, HedgeRatio: 1, ReceiveLoads: 1, ReceiveBytes: 16, Metrics: mx})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 16; i++ {
				e.latency.Observe(time.Millisecond)
			}
			armHedge := pauseHedgeArming(e)
			defer armHedge()
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				l, err := e.borrowBlock(ctx, url, oid, 16, 0, 0)
				if err == nil && !bytes.Equal(l.Bytes(), content) {
					err = fmt.Errorf("wrong bytes")
				}
				l.Release()
				done <- err
			}()
			// Join the reader on every assertion failure before closing its store.
			defer func() { cancel(); unblock(); armHedge(); <-finished }()
			select {
			case <-parentEntered:
			case err := <-done:
				t.Fatalf("read finished before parent admission: %v", err)
			case <-ctx.Done():
				t.Fatal("parent never acquired the receive buffer", ctx.Err())
			}
			if n, b := receiveUsage(e); n != 1 || b != 16 {
				t.Fatalf("parent admission = (%d, %d), want (1, 16)", n, b)
			}
			armHedge()
			// The parent owns the only buffer until its root hedge actually declines
			// admission. Release that parent only after the rejected-admission event.
			for mx.receiveRejected.Value() == 0 {
				select {
				case err := <-done:
					t.Fatalf("read finished before hedge pressure: err=%v parent=%d root=%d", err, parentCalls.Load(), rootCalls.Load())
				case <-ctx.Done():
					t.Fatalf("hedge never reached pressure: parent=%d root=%d: %v", parentCalls.Load(), rootCalls.Load(), ctx.Err())
				default:
					runtime.Gosched()
				}
			}
			if rootCalls.Load() != 0 {
				t.Fatal("root contacted without admission")
			}
			unblock()
			if err := <-done; err != nil {
				t.Fatal("healthy root was skipped after capacity returned", err)
			}
			if rootCalls.Load() != 1 || parentCalls.Load() != 1 {
				t.Fatal("wrong attempts", rootCalls.Load(), parentCalls.Load())
			}
			if n, b := receiveUsage(e); n != 0 || b != 0 {
				t.Fatal("receive leak", n, b)
			}
		})
	}
}

// Quantile sampling precedes hedge timer creation. Holding this test-only
// barrier lets the primary reach a specified stage before the timer is armed;
// launching its goroutine alone does not establish that ordering.
func pauseHedgeArming(e *Engine) func() {
	e.latency.mu.Lock()
	var once sync.Once
	return func() { once.Do(e.latency.mu.Unlock) }
}

// Reproduce a possible schedule that invalidates the pressure-test assumption:
// primary is paused before temporary admission, so the root hedge wins the only
// buffer. There must be no rejected admission to wait for in this schedule.
func TestRootHedgeCanWinBeforeParentAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	content := blob(16)
	const url = "http://origin.invalid/hedge-first"
	key := blockKeyFor(url)
	oid, _ := chunk.ObjectID(url)
	var rootCalls, parentCalls atomic.Int32
	root := httptest.NewServer(&peer.Server{Src: func(_ context.Context, req peer.BlockRequest) ([]byte, bool, error) {
		rootCalls.Add(1)
		if !req.Supply {
			t.Error("root hedge not terminal")
		}
		return content, true, nil
	}})
	defer root.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parentCalls.Add(1)
		http.Error(w, "unexpected parent request", 503)
	}))
	defer parent.Close()
	order := hashring.Rank(key.Chunk, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}})
	p := cluster.NewStaticProvider(
		cluster.Member{ID: "R", Addr: root.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly},
		cluster.Member{ID: order[0].ID, Addr: parent.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.NonRoot},
		cluster.Member{ID: order[1].ID, Addr: "unused:1", Weight: 1, State: cluster.Ready, Role: cluster.NonRoot})
	st := openStoreAt(t)
	// The primary is the first reservation caller, held until the winning
	// hedge cancels it. The second reservation falls through to temporary RAM.
	gate := &gatedReserveStore{Store: st, entered: make(chan struct{}), resume: make(chan struct{})}
	mx := NewMetrics(metrics.NewRegistry())
	e, err := New(Options{Chunk: testCfg(), Store: gate, Fetcher: &forbiddenOrigin{}, Cluster: p, Peer: peer.NewClient(), SelfID: order[1].ID, Role: cluster.NonRoot, Fanout: 1, Hedge: true, HedgeRatio: 1, ReceiveLoads: 1, ReceiveBytes: 16, Metrics: mx})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		e.latency.Observe(time.Millisecond)
	}
	armHedge := pauseHedgeArming(e)
	defer armHedge()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		l, err := e.borrowBlock(ctx, url, oid, 16, 0, 0)
		if err == nil && !bytes.Equal(l.Bytes(), content) {
			err = fmt.Errorf("wrong bytes")
		}
		l.Release()
		done <- err
	}()
	defer func() { cancel(); armHedge(); <-finished }()
	waitBlockEvent(t, ctx, gate.entered)
	if n, b := receiveUsage(e); n != 0 || b != 0 {
		t.Fatal("primary already admitted", n, b)
	}
	armHedge()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if rootCalls.Load() != 1 || parentCalls.Load() != 0 || mx.receiveRejected.Value() != 0 {
		t.Fatal("unexpected root-first schedule", rootCalls.Load(), parentCalls.Load(), mx.receiveRejected.Value())
	}
	if n, b := receiveUsage(e); n != 0 || b != 0 {
		t.Fatal("receive leak", n, b)
	}
}
