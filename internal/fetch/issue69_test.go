package fetch

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// alwaysFailFetcher counts invocations and never succeeds.
type alwaysFailFetcher struct{ calls atomic.Int64 }

func (f *alwaysFailFetcher) Fetch(context.Context, string, int64, int64) (Range, error) {
	f.calls.Add(1)
	return Range{}, errors.New("origin boom")
}

// TestSequentialFetchNeverJoinsCompletedFlight pins issue #69: group.finish
// used to publish the result (close(done)) before deleting the flight's map
// entry, so a strictly sequential same-key Fetch issued right after the
// leader returned could join the COMPLETED flight and reuse its result — an
// error effectively became cached, which is how the engine's "probe failures
// are never cached" pin (TestSizeHiddenTotalIsProbeFailure) came to flake in
// CI. Every sequential Fetch must reach the inner fetcher.
func TestSequentialFetchNeverJoinsCompletedFlight(t *testing.T) {
	// The pre-fix window is a handful of instructions in the flight worker
	// between publishing the result and removing the map entry; it bites only
	// when the worker loses its P exactly there. Fewer P's than runnable
	// goroutines plus race instrumentation widen it (CI's shape: 4 vCPUs with
	// parallel package tests). Pre-fix the race shows in scheduling bursts — a
	// starved worker leaves its completed entry discoverable across many
	// pairs — so the loop is time-boxed, not count-boxed: a short fixed count
	// can slip between bursts. Measured pre-fix: the first violation lands
	// well inside the budget (~2.7% of pairs in a standalone 2M-pair run,
	// first hit typically within a few thousand pairs); post-fix: zero
	// violations anywhere.
	restore := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(restore)
	var stop atomic.Bool
	for i := 0; i < 12; i++ {
		go func() {
			for !stop.Load() {
				runtime.Gosched()
			}
		}()
	}
	defer stop.Store(true)

	c := &Coalescing{F: &alwaysFailFetcher{}}
	inner := c.F.(*alwaysFailFetcher)
	deadline := time.Now().Add(2 * time.Second)
	for pair := 0; time.Now().Before(deadline); pair++ {
		before := inner.calls.Load()
		for j := 0; j < 2; j++ {
			if _, err := c.Fetch(context.Background(), "http://origin/x", 0, 0); err == nil {
				t.Fatal("the inner fetcher always fails; every call must return its error")
			}
		}
		if got := inner.calls.Load() - before; got != 2 {
			t.Fatalf("pair %d: two sequential fetches reached the inner fetcher %d times — a completed flight was joined and its error served twice (issue #69)", pair, got)
		}
	}
}
