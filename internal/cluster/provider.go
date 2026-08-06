package cluster

import (
	"sync"
	"sync/atomic"
)

// Provider is a source of cluster membership. Implementations publish an
// immutable View that changes over time (e.g. from a Kubernetes EndpointSlice
// watch, or a static configuration).
//
// Current must be safe for concurrent use and cheap (lock-free) since it is on
// the hot path of placement. Subscribe delivers subsequent Views for callers
// that must react to membership changes (re-sharding, epoch convergence).
type Provider interface {
	// Current returns the latest View. Never nil after construction.
	Current() *View

	// Subscribe returns a channel that receives the current View immediately
	// and every subsequent View, plus a cancel func that unsubscribes and
	// releases resources. The channel is buffered and coalescing: if the
	// consumer is slow, only the most recent View is retained (intermediate
	// ones are dropped), because only the latest membership matters.
	Subscribe() (<-chan *View, func())
}

// StaticProvider is an in-memory Provider whose View is set explicitly. It is
// used for tests and for non-Kubernetes deployments (static peer lists). It is
// safe for concurrent use.
type StaticProvider struct {
	cur atomic.Pointer[View]

	mu     sync.Mutex
	nextID int
	subs   map[int]chan *View
}

// NewStaticProvider creates a StaticProvider seeded with the given members.
func NewStaticProvider(members ...Member) *StaticProvider {
	p := &StaticProvider{subs: make(map[int]chan *View)}
	p.cur.Store(NewView(members))
	return p
}

// Current returns the latest View. Lock-free.
func (p *StaticProvider) Current() *View { return p.cur.Load() }

// Set replaces the membership with a new View built from members and notifies
// all subscribers. It is safe to call concurrently. If the new membership is
// identical to the current one the epoch is unchanged, but subscribers are
// still notified (they can compare epochs to no-op).
func (p *StaticProvider) Set(members []Member) *View {
	v := NewView(members)
	p.cur.Store(v)

	p.mu.Lock()
	for _, ch := range p.subs {
		notify(ch, v)
	}
	p.mu.Unlock()
	return v
}

// Subscribe implements Provider. The returned channel first receives the
// current View, then every View published by Set.
func (p *StaticProvider) Subscribe() (<-chan *View, func()) {
	ch := make(chan *View, 1)
	ch <- p.cur.Load() // deliver current state immediately

	p.mu.Lock()
	id := p.nextID
	p.nextID++
	p.subs[id] = ch
	p.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			p.mu.Lock()
			delete(p.subs, id)
			p.mu.Unlock()
		})
	}
	return ch, cancel
}

// notify performs a coalescing, non-blocking send on a buffered(1) channel:
// drain any stale value then store the latest, so a slow consumer always sees
// the most recent View without the producer ever blocking. Callers must hold
// p.mu (so no concurrent notify races on the same channel).
func notify(ch chan *View, v *View) {
	for {
		select {
		case ch <- v:
			return
		default:
			// Buffer full: drop the stale value and retry.
			select {
			case <-ch:
			default:
			}
		}
	}
}
