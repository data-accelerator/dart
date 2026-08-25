package cluster

// Regression tests for issue #12 (cluster bundle: K2–K5).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type issue12Seeder []string

func (s issue12Seeder) Seeds(context.Context) ([]string, error) { return []string(s), nil }

// TestHearsayAddrChangeDoesNotRefreshLiveness pins K2: a hearsay-carried
// address change used to reset lastContact, so two survivors flip-flopping
// conflicting address reports about a dead node kept it in membership forever
// (§3.9's invariant says the clock is deliberately NOT updated by hearsay).
func TestHearsayAddrChangeDoesNotRefreshLiveness(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	now := base
	d := NewDynamicProvider(DynamicConfig{
		Self:        Member{ID: "self", Addr: "10.0.0.1:9000"},
		Seeder:      issue12Seeder{},
		ForgetAfter: time.Minute,
		Now:         func() time.Time { return now },
	})
	d.Learn(Member{ID: "x", Addr: "10.0.0.2:9000"})

	// Control: same-address hearsay must not refresh.
	now = base.Add(30 * time.Second)
	before := d.known["x"].lastContact
	d.Learn(Member{ID: "x", Addr: "10.0.0.2:9000"})
	if !d.known["x"].lastContact.Equal(before) {
		t.Fatal("same-address hearsay refreshed the clock")
	}

	// A changed address is adopted (dial the newest one) but does not refresh.
	d.Learn(Member{ID: "x", Addr: "10.0.0.3:9000"})
	if got := d.known["x"].member.Addr; got != "10.0.0.3:9000" {
		t.Fatalf("addr = %s, want the newest address adopted", got)
	}
	if !d.known["x"].lastContact.Equal(before) {
		t.Fatal("address-change hearsay refreshed lastContact")
	}

	// Flip-flopping hearsay must not prevent forgetting: 200 rounds of
	// conflicting address reports at 30s spacing (half of ForgetAfter) with
	// zero direct contact must still expire x. (Hearsay may legitimately
	// re-add it afterwards — adding is immediate and cheap — so assert the
	// oscillation, not the end state: pre-fix, lastContact never lapsed and x
	// was present in every single round.)
	addrs := []string{"10.0.0.2:9000", "10.0.0.3:9000"}
	forgotten := false
	for i := 0; i < 200; i++ {
		now = now.Add(30 * time.Second)
		d.Learn(Member{ID: "x", Addr: addrs[i%2]})
		d.Refresh(context.Background())
		if _, ok := d.Current().Get("x"); !ok {
			forgotten = true
		}
	}
	if !forgotten {
		t.Fatal("x stayed in membership for 100 minutes of flip-flopping hearsay with zero direct contact")
	}
}

// TestConcurrentSetLastReturnedWins pins K3: Set used to Store outside the
// lock, so a slow Set (large NewView) invoked first could overwrite a fast Set
// that had already returned — the final Current() regressed. Now Store+notify
// serialize on the lock: the Set that returns last must own Current().
func TestConcurrentSetLastReturnedWins(t *testing.T) {
	p := NewStaticProvider(Member{ID: "init"})

	big := make([]Member, 300_000)
	for i := range big {
		big[i] = Member{ID: fmt.Sprintf("n%06d", i), Weight: 1}
	}
	small := []Member{{ID: "small", Weight: 1}}

	var slowView, fastView *View
	var slowDone, fastDone atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		slowView = p.Set(big)
		slowDone.Store(time.Now().UnixNano())
	}()
	time.Sleep(20 * time.Millisecond) // let the slow Set get deep into NewView
	fastView = p.Set(small)
	fastDone.Store(time.Now().UnixNano())
	wg.Wait()

	want, other := fastView, slowView
	if slowDone.Load() > fastDone.Load() {
		want, other = other, want
	}
	if p.Current() != want {
		t.Fatal("Current() is the Set that returned FIRST; the later Set's view was lost")
	}
	_ = other
}

// TestSubscribeNeverMissesConcurrentSet pins K4: Subscribe used to Load before
// registering, so a Set landing in the gap was never delivered and the
// subscriber stayed on the old view until the next Set.
func TestSubscribeNeverMissesConcurrentSet(t *testing.T) {
	for r := 0; r < 300; r++ {
		p := NewStaticProvider(Member{ID: "v0"})
		go p.Set([]Member{{ID: "v1"}})

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch, cancel := p.Subscribe()
				defer cancel()
				deadline := time.After(2 * time.Second)
				for {
					select {
					case v := <-ch:
						if _, ok := v.Get("v1"); ok {
							return
						}
					case <-deadline:
						t.Error("subscriber never received the Set")
						return
					}
				}
			}()
		}
		wg.Wait()
		if t.Failed() {
			t.Fatalf("round %d: a subscribe missed the concurrent Set", r)
		}
	}
}

// TestDedupKeepsServingStateOverJoining pins K5: duplicate IDs used to keep
// the lowest (ID, State, Weight) — a Joining duplicate silently suppressed a
// Ready one, dropping the node from placement. The rank order is now
// Ready > Suspect > Joining > Leaving, deterministic in both input orders.
func TestDedupKeepsServingStateOverJoining(t *testing.T) {
	mk := func() []Member {
		return []Member{
			{ID: "x", Weight: 1, State: Ready},
			{ID: "x", Weight: 9, State: Joining}, // duplicate ID, higher weight
			{ID: "y", Weight: 1, State: Ready},
		}
	}
	v1 := NewView(mk())
	rev := mk()
	rev[0], rev[1] = rev[1], rev[0]
	v2 := NewView(rev)

	for i, v := range []*View{v1, v2} {
		m, ok := v.Get("x")
		if !ok {
			t.Fatalf("view %d: x missing entirely", i)
		}
		if m.State != Ready {
			t.Fatalf("view %d: dedup kept %v for x, want Ready (a Joining marker must not suppress a serving node)", i, m.State)
		}
		if m.Weight != 1 {
			t.Fatalf("view %d: kept weight %g, want the Ready entry's", i, m.Weight)
		}
		if len(v.Members()) != 2 {
			t.Fatalf("view %d: %d members, want 2 (dedup by ID)", i, len(v.Members()))
		}
	}
	if v1.Epoch() != v2.Epoch() {
		t.Fatal("epochs differ across input orders; canonicalization is not input-order independent")
	}
}
