package tracker

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock so lease/tick behavior is deterministic.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newReg(clk *fakeClock, tick, ttl time.Duration) *Registry {
	return NewRegistry(Options{Tick: tick, LeaseTTL: ttl, Now: clk.now})
}

func TestJoinPublishesFirstReaderImmediately(t *testing.T) {
	clk := newClock()
	r := newReg(clk, time.Second, 2*time.Second)
	resp := r.Join("f1", "A", 0)
	if len(resp.Readers) != 1 || resp.Readers[0] != "A" {
		t.Fatalf("readers = %v, want [A]", resp.Readers)
	}
	if resp.TTLMs != 2000 {
		t.Errorf("ttlMs = %d, want 2000 (registry default)", resp.TTLMs)
	}
	if resp.EpochS == 0 {
		t.Error("epochS should be non-zero after the first publish")
	}
}

// TestTickFreeze: a second reader joining within the same tick must not change
// the published set (topology stays stable between ticks).
func TestTickFreeze(t *testing.T) {
	clk := newClock()
	r := newReg(clk, 5*time.Second, 10*time.Second)
	first := r.Join("f1", "A", 0)

	// B joins in the same tick: still frozen at [A].
	resp := r.Join("f1", "B", 0)
	if len(resp.Readers) != 1 || resp.Readers[0] != "A" {
		t.Errorf("within tick readers = %v, want [A] (frozen)", resp.Readers)
	}
	if resp.EpochS != first.EpochS {
		t.Errorf("epochS changed within a tick: %d -> %d", first.EpochS, resp.EpochS)
	}

	// After the tick, both are published and the epoch advances.
	clk.advance(6 * time.Second)
	resp2 := r.Join("f1", "B", 0)
	if len(resp2.Readers) != 2 || resp2.Readers[0] != "A" || resp2.Readers[1] != "B" {
		t.Errorf("after tick readers = %v, want [A B]", resp2.Readers)
	}
	if resp2.EpochS == first.EpochS {
		t.Error("epochS should change when the frozen set changes")
	}
}

// TestLeaseExpiry: a reader that stops renewing drops out on a later tick.
func TestLeaseExpiry(t *testing.T) {
	clk := newClock()
	r := newReg(clk, time.Second, 2*time.Second)
	r.Join("f1", "A", 0)
	// Advance past the tick but within A's 2s lease so the tick publishes [A B].
	clk.advance(1500 * time.Millisecond)
	r.Join("f1", "B", 0) // tick fires: [A B]
	if got, _ := r.Readers("f1"); len(got) != 2 {
		t.Fatalf("readers = %v, want [A B]", got)
	}

	// Only B renews; A's 2s lease lapses.
	clk.advance(3 * time.Second)
	r.Join("f1", "B", 0)
	got, _ := r.Readers("f1")
	if len(got) != 1 || got[0] != "B" {
		t.Errorf("after A's lease expiry readers = %v, want [B]", got)
	}
}

func TestLeave(t *testing.T) {
	clk := newClock()
	r := newReg(clk, time.Second, 10*time.Second)
	r.Join("f1", "A", 0)
	r.Join("f1", "B", 0)
	clk.advance(2 * time.Second)
	r.Join("f1", "B", 0) // publish [A B]

	r.Leave("f1", "A")
	clk.advance(2 * time.Second)
	got, _ := r.Readers("f1")
	if len(got) != 1 || got[0] != "B" {
		t.Errorf("after Leave readers = %v, want [B]", got)
	}

	// Leaving the last reader forgets the file entirely.
	r.Leave("f1", "B")
	if r.Files() != 0 {
		t.Errorf("Files = %d, want 0 after all readers left", r.Files())
	}
	if got, epoch := r.Readers("f1"); got != nil || epoch != 0 {
		t.Errorf("Readers(unknown) = %v, %d", got, epoch)
	}
}

// TestReadersSortedDeterministic: the published set is sorted, so every node
// derives the same tree from it regardless of join order.
func TestReadersSortedDeterministic(t *testing.T) {
	clk := newClock()
	r1 := newReg(clk, time.Millisecond, time.Minute)
	r2 := newReg(clk, time.Millisecond, time.Minute)
	for _, n := range []string{"c", "a", "b"} {
		r1.Join("f", n, 0)
	}
	for _, n := range []string{"b", "c", "a"} {
		r2.Join("f", n, 0)
	}
	clk.advance(time.Second)
	got1, _ := r1.Readers("f")
	got2, _ := r2.Readers("f")
	want := []string{"a", "b", "c"}
	if !equalStrings(got1, want) || !equalStrings(got2, want) {
		t.Errorf("readers not deterministic/sorted: %v vs %v (want %v)", got1, got2, want)
	}
}

func TestEpochStableWhenSetUnchanged(t *testing.T) {
	clk := newClock()
	r := newReg(clk, time.Second, time.Minute)
	first := r.Join("f", "A", 0)
	for i := 0; i < 5; i++ {
		clk.advance(2 * time.Second)
		if got := r.Join("f", "A", 0); got.EpochS != first.EpochS {
			t.Fatalf("epochS changed across ticks with an unchanged set: %d -> %d", first.EpochS, got.EpochS)
		}
	}
}

func TestReadersCopyNotAliased(t *testing.T) {
	clk := newClock()
	r := newReg(clk, time.Second, time.Minute)
	r.Join("f", "A", 0)
	got, _ := r.Readers("f")
	got[0] = "MUTATED"
	again, _ := r.Readers("f")
	if again[0] != "A" {
		t.Errorf("Readers returned an aliased slice: %v", again)
	}
}

// --- HTTP ---

func TestHTTPJoinLeave(t *testing.T) {
	clk := newClock()
	r := newReg(clk, time.Second, 2*time.Second)
	srv := httptest.NewServer((&Server{R: r}).Handler())
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient()
	ctx := context.Background()

	resp, err := c.Join(ctx, addr, "f1", "A", 0)
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if len(resp.Readers) != 1 || resp.Readers[0] != "A" || resp.TTLMs != 2000 {
		t.Errorf("join resp = %+v", resp)
	}

	clk.advance(1500 * time.Millisecond) // past the tick, within A's 2s lease
	resp2, err := c.Join(ctx, addr, "f1", "B", 5*time.Second)
	if err != nil {
		t.Fatalf("Join B: %v", err)
	}
	if len(resp2.Readers) != 2 {
		t.Errorf("readers = %v, want 2", resp2.Readers)
	}
	if resp2.TTLMs != 5000 {
		t.Errorf("granted ttl = %d, want 5000 (requested)", resp2.TTLMs)
	}

	if err := c.Leave(ctx, addr, "f1", "A"); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	clk.advance(2 * time.Second)
	got, _ := r.Readers("f1")
	if len(got) != 1 || got[0] != "B" {
		t.Errorf("after HTTP Leave readers = %v, want [B]", got)
	}
}

func TestHTTPBadRequests(t *testing.T) {
	r := newReg(newClock(), time.Second, time.Second)
	srv := httptest.NewServer((&Server{R: r}).Handler())
	defer srv.Close()

	// GET instead of POST.
	resp, err := srv.Client().Get(srv.URL + JoinPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("GET join status = %d, want 405", resp.StatusCode)
	}

	// Malformed JSON.
	resp2, err := srv.Client().Post(srv.URL+JoinPath, "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 400 {
		t.Errorf("bad json status = %d, want 400", resp2.StatusCode)
	}

	// Missing fields.
	resp3, err := srv.Client().Post(srv.URL+JoinPath, "application/json", strings.NewReader(`{"file":"f"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 400 {
		t.Errorf("missing node status = %d, want 400", resp3.StatusCode)
	}
}

func TestClientTrackerDown(t *testing.T) {
	c := NewClient()
	if _, err := c.Join(context.Background(), "127.0.0.1:1", "f", "A", 0); err == nil {
		t.Error("expected an error when the tracker is unreachable")
	}
	if err := c.Leave(context.Background(), "127.0.0.1:1", "f", "A"); err == nil {
		t.Error("expected an error when the tracker is unreachable")
	}
}

// TestConcurrent hammers a registry from many goroutines (-race).
func TestConcurrent(t *testing.T) {
	clk := newClock()
	r := newReg(clk, 10*time.Millisecond, time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			node := string(rune('A' + g))
			for i := 0; i < 500; i++ {
				r.Join("shared", node, 0)
				r.Readers("shared")
				if i%50 == 0 {
					clk.advance(11 * time.Millisecond)
				}
				if i%100 == 99 {
					r.Leave("shared", node)
				}
			}
		}(g)
	}
	wg.Wait()
}
