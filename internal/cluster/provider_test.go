package cluster

import (
	"sync"
	"testing"
)

func TestStaticProviderCurrent(t *testing.T) {
	p := NewStaticProvider(
		Member{ID: "a", Weight: 1, State: Ready},
		Member{ID: "b", Weight: 1, State: Ready},
	)
	if p.Current() == nil || p.Current().Len() != 2 {
		t.Fatalf("current view len = %v", p.Current())
	}
}

func TestStaticProviderSetBumpsEpoch(t *testing.T) {
	p := NewStaticProvider(Member{ID: "a", Weight: 1, State: Ready})
	e0 := p.Current().Epoch()
	p.Set([]Member{
		{ID: "a", Weight: 1, State: Ready},
		{ID: "b", Weight: 1, State: Ready},
	})
	if p.Current().Epoch() == e0 {
		t.Error("epoch did not change after adding a member")
	}
	if p.Current().Len() != 2 {
		t.Errorf("len = %d, want 2", p.Current().Len())
	}
}

// TestSubscribeReceivesCurrentThenUpdates: a subscriber gets the current View
// immediately and every subsequent Set.
func TestSubscribeReceivesCurrentThenUpdates(t *testing.T) {
	p := NewStaticProvider(Member{ID: "a", Weight: 1, State: Ready})
	ch, cancel := p.Subscribe()
	defer cancel()

	first := <-ch
	if first.Len() != 1 {
		t.Fatalf("first view len = %d, want 1", first.Len())
	}
	p.Set([]Member{{ID: "a", Weight: 1, State: Ready}, {ID: "b", Weight: 1, State: Ready}})
	second := <-ch
	if second.Len() != 2 {
		t.Fatalf("second view len = %d, want 2", second.Len())
	}
}

// TestSubscribeCoalesces: a slow consumer only sees the latest View, never
// blocks the producer, and the buffer never grows beyond 1.
func TestSubscribeCoalesces(t *testing.T) {
	p := NewStaticProvider(Member{ID: "a", Weight: 1, State: Ready})
	ch, cancel := p.Subscribe()
	defer cancel()
	<-ch // drain the initial view

	// Producer sends several updates without the consumer reading in between.
	for i := 2; i <= 5; i++ {
		ms := make([]Member, i)
		for j := 0; j < i; j++ {
			ms[j] = Member{ID: string(rune('a' + j)), Weight: 1, State: Ready}
		}
		p.Set(ms) // must never block
	}
	// Only the most recent (len 5) should be retained.
	latest := <-ch
	if latest.Len() != 5 {
		t.Errorf("coalesced view len = %d, want 5 (latest)", latest.Len())
	}
	select {
	case extra := <-ch:
		t.Errorf("buffer held more than one view: extra len=%d", extra.Len())
	default:
	}
}

// TestUnsubscribe: after cancel, Set no longer delivers to the channel and
// cancel is idempotent.
func TestUnsubscribe(t *testing.T) {
	p := NewStaticProvider(Member{ID: "a", Weight: 1, State: Ready})
	ch, cancel := p.Subscribe()
	<-ch // initial
	cancel()
	cancel() // idempotent, must not panic
	p.Set([]Member{{ID: "a", Weight: 1, State: Ready}, {ID: "b", Weight: 1, State: Ready}})
	select {
	case v := <-ch:
		if v != nil {
			t.Errorf("received view after unsubscribe: %v", v)
		}
	default:
		// expected: nothing delivered
	}
}

// TestConcurrentSetAndCurrent exercises the lock-free reader path against
// concurrent writers and a draining subscriber; run with -race.
func TestConcurrentSetAndCurrent(t *testing.T) {
	p := NewStaticProvider(Member{ID: "a", Weight: 1, State: Ready})
	var wg sync.WaitGroup

	// Readers (lock-free Current()).
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				if v := p.Current(); v == nil {
					t.Error("Current returned nil")
					return
				}
			}
		}()
	}

	// A subscriber that keeps draining until told to stop.
	ch, cancel := p.Subscribe()
	defer cancel()
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-ch:
			case <-stop:
				return
			}
		}
	}()

	// Writers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				p.Set([]Member{{ID: "a", Weight: float64(base + j + 1), State: Ready}})
			}
		}(i)
	}

	wg.Wait()   // readers + writers done
	close(stop) // stop the drain goroutine
	<-drained
}
