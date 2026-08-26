package peer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

// brkClock is a manually advanced clock so cooldown behavior is deterministic.
type brkClock struct {
	mu sync.Mutex
	t  time.Time
}

func newBrkClock() *brkClock { return &brkClock{t: time.Unix(1_700_000_000, 0)} }

func (c *brkClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *brkClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Minute, Now: clk.now})
	const addr = "a:1"

	for i := 0; i < 2; i++ {
		b.RecordFailure(addr)
		if got := b.State(addr); got != BreakerClosed {
			t.Fatalf("after %d failures state = %v, want closed", i+1, got)
		}
		if !b.Allow(addr) {
			t.Fatalf("after %d failures requests should still be allowed", i+1)
		}
	}
	b.RecordFailure(addr) // third: threshold reached
	if got := b.State(addr); got != BreakerOpen {
		t.Errorf("state = %v, want open", got)
	}
	if b.Allow(addr) {
		t.Error("an open circuit must reject requests")
	}
	if b.Healthy(addr) {
		t.Error("Healthy should be false while open")
	}
	if n := b.OpenCount(); n != 1 {
		t.Errorf("OpenCount = %d, want 1", n)
	}
}

// TestBreakerSuccessResetsFailures: intermittent failures must not accumulate
// into an open circuit; only consecutive ones count.
func TestBreakerSuccessResetsFailures(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Minute, Now: clk.now})
	const addr = "a:1"
	for i := 0; i < 10; i++ {
		b.RecordFailure(addr)
		b.RecordFailure(addr)
		b.RecordSuccess(addr) // breaks the streak
	}
	if got := b.State(addr); got != BreakerClosed {
		t.Errorf("state = %v, want closed (failures were not consecutive)", got)
	}
}

// TestBreakerHalfOpenRecovery: after the cooldown a probe is admitted, and a
// success closes the circuit.
func TestBreakerHalfOpenRecovery(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 1, Cooldown: 10 * time.Second, Now: clk.now})
	const addr = "a:1"
	b.RecordFailure(addr)
	if b.State(addr) != BreakerOpen {
		t.Fatal("expected open")
	}

	// Still cooling down.
	clk.advance(9 * time.Second)
	if b.Allow(addr) {
		t.Error("requests should be rejected during cooldown")
	}

	// Cooldown elapsed: half-open admits exactly one probe.
	clk.advance(2 * time.Second)
	if got := b.State(addr); got != BreakerHalfOpen {
		t.Fatalf("state = %v, want half-open", got)
	}
	if !b.Allow(addr) {
		t.Fatal("half-open should admit a probe")
	}
	if b.Allow(addr) {
		t.Error("half-open should admit only HalfOpenProbes concurrent probes")
	}
	b.RecordSuccess(addr)
	if got := b.State(addr); got != BreakerClosed {
		t.Errorf("state = %v, want closed after a successful probe", got)
	}
	if !b.Allow(addr) {
		t.Error("a closed circuit should allow requests")
	}
}

// TestBreakerHalfOpenFailureReopens: a failed probe re-opens immediately and
// restarts the cooldown, rather than letting probes trickle through.
func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 5, Cooldown: 10 * time.Second, Now: clk.now})
	const addr = "a:1"
	for i := 0; i < 5; i++ {
		b.RecordFailure(addr)
	}
	clk.advance(11 * time.Second)
	if !b.Allow(addr) {
		t.Fatal("expected a half-open probe slot")
	}
	b.RecordFailure(addr) // probe failed
	if got := b.State(addr); got != BreakerOpen {
		t.Errorf("state = %v, want open again", got)
	}
	clk.advance(9 * time.Second)
	if b.Allow(addr) {
		t.Error("the cooldown should have restarted")
	}
}

func TestBreakerIsolatesPeers(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 1, Cooldown: time.Minute, Now: clk.now})
	b.RecordFailure("sick:1")
	if b.Allow("sick:1") {
		t.Error("the sick peer should be open")
	}
	if !b.Allow("healthy:1") {
		t.Error("an unrelated peer must not be affected")
	}
	if n := b.OpenCount(); n != 1 {
		t.Errorf("OpenCount = %d, want 1", n)
	}
}

func TestBreakerDefaultsAndStateNames(t *testing.T) {
	b := NewBreaker(BreakerOptions{})
	if b.threshold != DefaultFailureThreshold || b.cooldown != DefaultBreakerCooldown || b.probes != DefaultHalfOpenProbes {
		t.Errorf("defaults not applied: %d/%v/%d", b.threshold, b.cooldown, b.probes)
	}
	if BreakerClosed.String() != "closed" || BreakerOpen.String() != "open" || BreakerHalfOpen.String() != "half-open" {
		t.Error("state names wrong")
	}
	if !b.Healthy("never-seen:1") {
		t.Error("an unseen peer should be healthy")
	}
}

func TestBreakerConcurrent(t *testing.T) {
	b := NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Millisecond})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			addr := string(rune('a'+g)) + ":1"
			for i := 0; i < 1000; i++ {
				if b.Allow(addr) {
					if i%3 == 0 {
						b.RecordFailure(addr)
					} else {
						b.RecordSuccess(addr)
					}
				}
				b.State(addr)
				b.Healthy(addr)
				b.OpenCount()
			}
		}(g)
	}
	wg.Wait()
}

// --- Client integration ---

// TestClientBreakerShortCircuits: once a dead peer has tripped the breaker, the
// client stops dialing it and fails immediately with ErrCircuitOpen. Without
// this, every block would pay a connect timeout.
func TestClientBreakerShortCircuits(t *testing.T) {
	var hits int64
	// A server that always 500s: an unexpected status counts as a failure.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	addr := addrOf(t, srv.URL)

	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Hour})
	key := store.BlockKey{Chunk: 1, Block: 0}
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, _, err := c.Get(ctx, addr, BlockRequest{Key: key}); err == nil {
			t.Fatalf("attempt %d should have failed", i)
		}
	}
	if got := atomic.LoadInt64(&hits); got != 3 {
		t.Fatalf("server saw %d requests, want 3 before the circuit opened", got)
	}

	// Further calls must not reach the network.
	for i := 0; i < 5; i++ {
		_, _, err := c.Get(ctx, addr, BlockRequest{Key: key})
		if !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d error = %v, want ErrCircuitOpen", i, err)
		}
	}
	if got := atomic.LoadInt64(&hits); got != 3 {
		t.Errorf("server saw %d requests; the open circuit still dialed", got)
	}

	// Stream is gated by the same breaker.
	if _, _, err := c.Stream(ctx, addr, BlockRequest{Key: key}, nil); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Stream error = %v, want ErrCircuitOpen", err)
	}
}

// TestClientBreakerIgnores404 is the important distinction: a 404 is a valid
// answer ("I do not hold that block"), which happens constantly in a distributed
// cache. Counting it would trip the breaker on perfectly healthy peers.
func TestClientBreakerIgnores404(t *testing.T) {
	st := newStore(t)
	srv := httptest.NewServer(&Server{Src: StoreSource(st)}) // empty store: always 404
	defer srv.Close()
	addr := addrOf(t, srv.URL)

	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Hour})
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		_, held, err := c.Get(ctx, addr, BlockRequest{Key: store.BlockKey{Chunk: uint64(i)}})
		if err != nil || held {
			t.Fatalf("call %d: held=%v err=%v, want a clean miss", i, held, err)
		}
	}
	if got := c.Breaker.State(addr); got != BreakerClosed {
		t.Errorf("state = %v after 20 misses, want closed (404 is not a failure)", got)
	}
}

// TestClientBreakerRecovers: a peer that comes back is picked up again after the
// cooldown, without operator intervention.
func TestClientBreakerRecovers(t *testing.T) {
	var healthy atomic.Bool
	data := make([]byte, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()
	addr := addrOf(t, srv.URL)

	clk := newBrkClock()
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 2, Cooldown: 5 * time.Second, Now: clk.now})
	key := store.BlockKey{Chunk: 1, Block: 0}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		_, _, _ = c.Get(ctx, addr, BlockRequest{Key: key})
	}
	if _, _, err := c.Get(ctx, addr, BlockRequest{Key: key}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected the circuit to be open, got %v", err)
	}

	// The peer recovers and the cooldown elapses.
	healthy.Store(true)
	clk.advance(6 * time.Second)
	got, held, err := c.Get(ctx, addr, BlockRequest{Key: key})
	if err != nil || !held || len(got) != 8 {
		t.Fatalf("recovery probe: held=%v err=%v len=%d", held, err, len(got))
	}
	if s := c.Breaker.State(addr); s != BreakerClosed {
		t.Errorf("state = %v after recovery, want closed", s)
	}
}

// TestLateFailureDoesNotExtendCooldown pins issue #9 (P3): a failure that
// arrives while the circuit is already open — a late in-flight request dying —
// used to restamp openedAt, sliding the cooldown. A steady trickle of late
// completions could pin the circuit open forever. The cooldown must start when
// the circuit opens, not when the last in-flight request dies.
func TestLateFailureDoesNotExtendCooldown(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 1, Cooldown: 5 * time.Second, Now: clk.now})

	b.RecordFailure("p") // opens at t0
	clk.advance(4 * time.Second)
	b.RecordFailure("p") // late failure while open: must not restamp
	clk.advance(time.Second + time.Millisecond)
	if !b.Allow("p") {
		t.Fatal("cooldown must be measured from the open, not from a late failure")
	}
	// A failed half-open probe DOES start a fresh cooldown.
	b.RecordFailure("p")
	if b.Allow("p") {
		t.Fatal("a failed half-open probe must re-open with a fresh cooldown")
	}
	clk.advance(5*time.Second + time.Millisecond)
	if !b.Allow("p") {
		t.Fatal("after the fresh cooldown a new probe must be allowed")
	}
}

// TestBreakerSweepsCleanEntries pins issue #9 (P4): the peer map used to grow
// without bound (every address ever seen, never deleted). Past the cap,
// information-free entries (closed, no failures, no probes) are swept; entries
// with state are preserved.
func TestBreakerSweepsCleanEntries(t *testing.T) {
	b := NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Minute})
	// One dirty entry that must survive sweeping.
	for i := 0; i < 3; i++ {
		b.RecordFailure("sick-peer")
	}
	// Churn more distinct clean peers than the cap.
	for i := 0; i < maxTrackedPeers+100; i++ {
		b.Allow(fmt.Sprintf("peer-%d", i))
	}
	b.mu.Lock()
	n := len(b.peers)
	b.mu.Unlock()
	if n > maxTrackedPeers {
		t.Fatalf("peers map = %d entries, want <= %d (clean entries must be swept)", n, maxTrackedPeers)
	}
	if b.State("sick-peer") != BreakerOpen {
		t.Fatal("a dirty entry must never be swept")
	}
}

// TestBreakerCapHardUnderFailedChurn pins issue #54: the sweep only removes
// information-free entries, so churning distinct addresses that all carry
// failure state used to grow the map past maxTrackedPeers without bound. The
// cap is now a hard bound: once the sweep frees nothing, the
// least-recently-active entry is evicted instead. Each flavor below builds
// dirty entries a different way; all must stay within the cap.
func TestBreakerCapHardUnderFailedChurn(t *testing.T) {
	const churn = maxTrackedPeers + 100
	flavors := []struct {
		name string
		// dirty gives addr state that survives the information-free sweep.
		dirty func(b *Breaker, clk *brkClock, addr string)
		// check pins the surviving behavior of a recently-touched address.
		check func(t *testing.T, b *Breaker, addr string)
	}{
		{
			name:  "failures below threshold",
			dirty: func(b *Breaker, clk *brkClock, addr string) { b.RecordFailure(addr) },
			check: func(t *testing.T, b *Breaker, addr string) {
				b.RecordFailure(addr) // second of three: must still be closed
				if b.State(addr) != BreakerClosed {
					t.Fatalf("addr with failures below threshold = %v, want closed", b.State(addr))
				}
			},
		},
		{
			name:  "open circuits",
			dirty: func(b *Breaker, clk *brkClock, addr string) { b.RecordHardFailure(addr) },
			check: func(t *testing.T, b *Breaker, addr string) {
				if b.State(addr) != BreakerOpen {
					t.Fatalf("recently hard-failed addr = %v, want open", b.State(addr))
				}
				if b.Allow(addr) {
					t.Fatal("an open circuit must reject requests")
				}
			},
		},
		{
			name: "half-open probes in flight",
			dirty: func(b *Breaker, clk *brkClock, addr string) {
				b.RecordHardFailure(addr)
				clk.advance(time.Minute + time.Millisecond) // past cooldown
				if !b.Allow(addr) {                         // reserves the probe slot
					t.Fatalf("addr %s should admit a probe after cooldown", addr)
				}
			},
			check: func(t *testing.T, b *Breaker, addr string) {
				if b.State(addr) != BreakerHalfOpen {
					t.Fatalf("addr with a probe in flight = %v, want half-open", b.State(addr))
				}
			},
		},
	}
	for _, f := range flavors {
		t.Run(f.name, func(t *testing.T) {
			clk := newBrkClock()
			b := NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Minute, Now: clk.now})
			for i := 0; i < churn; i++ {
				clk.advance(time.Millisecond) // distinct lastActive per address
				f.dirty(b, clk, fmt.Sprintf("peer-%d", i))
			}
			b.mu.Lock()
			n := len(b.peers)
			b.mu.Unlock()
			if n > maxTrackedPeers {
				t.Fatalf("peers map = %d entries after churning %d dirty addresses, want <= %d (hard bound)", n, churn, maxTrackedPeers)
			}
			// The most recently touched address must survive eviction with
			// its breaker behavior intact.
			f.check(t, b, fmt.Sprintf("peer-%d", churn-1))
		})
	}
}

// TestBreakerEvictionDropsStalest pins the eviction victim choice under issue
// #54: under map pressure with no sweepable entries, the least-recently-active
// entry goes first, so a peer that is still being routed to never loses its
// circuit state.
func TestBreakerEvictionDropsStalest(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Hour, Now: clk.now})
	// Fill the map to the cap with dirty entries, oldest first.
	for i := 0; i < maxTrackedPeers; i++ {
		clk.advance(time.Millisecond)
		b.RecordFailure(fmt.Sprintf("stale-%d", i))
	}
	// A sick peer that stays in rotation: opened, then touched again most
	// recently of all.
	for i := 0; i < 3; i++ {
		b.RecordFailure("sick-peer")
	}
	clk.advance(time.Millisecond)
	b.Allow("sick-peer") // rejected (open) but still activity: freshest entry
	// Churn more dirty addresses to force LRU evictions.
	for i := 0; i < 100; i++ {
		clk.advance(time.Millisecond)
		b.RecordFailure(fmt.Sprintf("churn-%d", i))
	}
	b.mu.Lock()
	n := len(b.peers)
	_, stale0Present := b.peers["stale-0"] // the stalest entry
	_, sickPresent := b.peers["sick-peer"]
	b.mu.Unlock()
	if n > maxTrackedPeers {
		t.Fatalf("peers map = %d entries, want <= %d", n, maxTrackedPeers)
	}
	if stale0Present {
		t.Fatal("the stalest entry must be the first eviction victim")
	}
	if !sickPresent {
		t.Fatal("an actively routed-to peer must never be evicted")
	}
	if b.State("sick-peer") != BreakerOpen {
		t.Fatal("the active peer's open circuit must survive eviction pressure")
	}
}

// TestCallerCancelNotChargedToPeer pins issue #9 (P2a): a caller-aborted read
// (hedging loser, client disconnect) used to record a soft failure against the
// healthy peer — one cancel with FailureThreshold 1 opened the circuit. Caller
// cancellation is not a peer failure and must not count.
func TestCallerCancelNotChargedToPeer(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{1}) // drip one byte, then stall
		w.(http.Flusher).Flush()
		<-release
	}))
	defer srv.Close()
	defer close(release)

	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 1, Cooldown: time.Minute})
	addr := strings.TrimPrefix(srv.URL, "http://")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := c.Get(ctx, addr, BlockRequest{Key: store.BlockKey{Chunk: 1, Block: 0}})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // the drip arrived; body read is stalled
	cancel()
	if err := <-done; err == nil {
		t.Fatal("Get should return after caller cancel")
	}
	if st := c.Breaker.State(addr); st != BreakerClosed {
		t.Fatalf("caller cancel opened the circuit: state = %v", st)
	}
}
