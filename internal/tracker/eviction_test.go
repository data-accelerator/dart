package tracker

import (
	"testing"
	"time"
)

// regWithGrace builds a Registry with an explicit idle grace.
func regWithGrace(clk *fakeClock, tick, ttl, grace time.Duration) *Registry {
	return NewRegistry(Options{Tick: tick, LeaseTTL: ttl, IdleGrace: grace, Now: clk.now})
}

// TestIdleEvictionAfterGrace: a file whose readers vanish without Leave is
// forgotten once its leases have expired AND it has been idle past the grace;
// within the grace it is kept (absorbing join/leave churn).
func TestIdleEvictionAfterGrace(t *testing.T) {
	clk := newClock()
	r := regWithGrace(clk, time.Second, 2*time.Second, 10*time.Second)
	r.Join("f1", "A", 0)
	if r.Files() != 1 {
		t.Fatalf("files = %d, want 1", r.Files())
	}

	// Lease expired (2s) but grace (10s) not elapsed: kept.
	clk.advance(5 * time.Second)
	r.Join("other", "B", 0) // drives a sweep
	if r.Files() != 2 {
		t.Fatalf("files = %d, want 2 (f1 within grace)", r.Files())
	}

	// Past the grace: forgotten on the next sweep.
	clk.advance(10 * time.Second) // f1 idle 15s > grace
	r.Join("other", "B", 0)       // drives a sweep (other is exactly at grace: kept)
	if r.Files() != 1 {
		t.Fatalf("files = %d, want 1 (f1 evicted)", r.Files())
	}
	if _, epoch := r.Readers("f1"); epoch != 0 {
		t.Errorf("evicted file still has epoch %d", epoch)
	}
}

// TestIdleEvictionKeepsQueriedFiles: querying a file counts as activity, so a
// file that is still being asked about is never evicted, even with all leases
// long expired.
func TestIdleEvictionKeepsQueriedFiles(t *testing.T) {
	clk := newClock()
	r := regWithGrace(clk, time.Second, 2*time.Second, 10*time.Second)
	r.Join("f1", "A", 0)
	for i := 0; i < 5; i++ {
		clk.advance(5 * time.Second) // each step: lease long dead, idle 5s < grace
		r.Readers("f1")              // query activity; also drives the sweep
	}
	if r.Files() != 1 {
		t.Fatalf("files = %d, want 1 (queried file kept)", r.Files())
	}
	readers, _ := r.Readers("f1")
	if len(readers) != 0 {
		t.Errorf("readers = %v, want empty (lease expired) but file kept", readers)
	}
}

// TestRejoinAfterEviction: a Join after eviction recreates the entry and
// publishes immediately, at the price of an epochS reset.
func TestRejoinAfterEviction(t *testing.T) {
	clk := newClock()
	r := regWithGrace(clk, time.Second, 2*time.Second, 5*time.Second)
	r.Join("f1", "A", 0)
	clk.advance(30 * time.Second)
	r.Files() // drives the sweep: f1 evicted
	if r.Files() != 0 {
		t.Fatalf("files = %d, want 0 after eviction", r.Files())
	}
	resp := r.Join("f1", "C", 0)
	if len(resp.Readers) != 1 || resp.Readers[0] != "C" {
		t.Fatalf("readers = %v, want [C]", resp.Readers)
	}
	if resp.EpochS == 0 {
		t.Error("epochS should be non-zero after re-publish")
	}
}

// TestIdleEvictionDefaultGrace: a zero IdleGrace selects DefaultIdleGrace.
func TestIdleEvictionDefaultGrace(t *testing.T) {
	r := newReg(newClock(), time.Second, 2*time.Second)
	if r.grace != DefaultIdleGrace {
		t.Errorf("grace = %v, want DefaultIdleGrace %v", r.grace, DefaultIdleGrace)
	}
}
