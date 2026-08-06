package peer

import (
	"errors"
	"sync"
	"time"
)

// Circuit breaking keeps one sick peer from taxing every read that routes
// through it.
//
// Without a breaker, a node that is down or persistently erroring is retried on
// every single block: each read pays a connect timeout before falling back, and
// in a distribution tree the cost is multiplied by the subtree below it. The
// breaker remembers the failure so subsequent requests fail immediately (cheaply)
// and the caller can route elsewhere, while still probing periodically so a
// recovered peer is picked back up automatically.
//
// What counts as a failure matters here: a 404 is **not** a failure. It is a
// legitimate answer ("I do not hold that block") and blocks legitimately 404 all
// the time in a distributed cache — counting them would trip the breaker on
// healthy peers. Only transport errors, timeouts, and unexpected statuses count.

// BreakerState is a peer's circuit state.
type BreakerState int

const (
	// BreakerClosed passes requests through (the healthy state).
	BreakerClosed BreakerState = iota
	// BreakerOpen rejects requests immediately.
	BreakerOpen
	// BreakerHalfOpen lets a limited number of probes through to test recovery.
	BreakerHalfOpen
)

// String returns the state name.
func (s BreakerState) String() string {
	switch s {
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// ErrCircuitOpen is returned instead of dialing a peer whose circuit is open.
var ErrCircuitOpen = errors.New("peer: circuit open")

// Breaker defaults.
const (
	DefaultFailureThreshold = 5
	DefaultBreakerCooldown  = 5 * time.Second
	DefaultHalfOpenProbes   = 1
)

// BreakerOptions configures a Breaker. Zero values select the defaults.
type BreakerOptions struct {
	// FailureThreshold is how many consecutive failures open the circuit.
	FailureThreshold int
	// Cooldown is how long the circuit stays open before probing again.
	Cooldown time.Duration
	// HalfOpenProbes is how many concurrent probes half-open admits.
	HalfOpenProbes int
	// Now overrides the clock (tests).
	Now func() time.Time
}

// Breaker tracks per-peer health, keyed by address. It is safe for concurrent
// use.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	probes    int
	now       func() time.Time

	mu    sync.Mutex
	peers map[string]*breakerEntry
}

// breakerEntry is one peer's circuit.
type breakerEntry struct {
	state    BreakerState
	failures int       // consecutive failures
	openedAt time.Time // when the circuit last opened
	inFlight int       // half-open probes currently outstanding
}

// NewBreaker creates a Breaker.
func NewBreaker(opt BreakerOptions) *Breaker {
	b := &Breaker{
		threshold: opt.FailureThreshold,
		cooldown:  opt.Cooldown,
		probes:    opt.HalfOpenProbes,
		now:       opt.Now,
		peers:     make(map[string]*breakerEntry),
	}
	if b.threshold <= 0 {
		b.threshold = DefaultFailureThreshold
	}
	if b.cooldown <= 0 {
		b.cooldown = DefaultBreakerCooldown
	}
	if b.probes <= 0 {
		b.probes = DefaultHalfOpenProbes
	}
	if b.now == nil {
		b.now = time.Now
	}
	return b
}

// entryLocked returns (creating if needed) the peer's circuit, first applying any
// due open -> half-open transition. Caller must hold b.mu.
func (b *Breaker) entryLocked(addr string) *breakerEntry {
	e := b.peers[addr]
	if e == nil {
		e = &breakerEntry{state: BreakerClosed}
		b.peers[addr] = e
	}
	if e.state == BreakerOpen && b.now().Sub(e.openedAt) >= b.cooldown {
		e.state = BreakerHalfOpen
		e.inFlight = 0
	}
	return e
}

// Allow reports whether a request to addr may proceed. When it returns true in
// the half-open state it has reserved a probe slot, so the caller MUST report the
// outcome via RecordSuccess or RecordFailure.
func (b *Breaker) Allow(addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entryLocked(addr)
	switch e.state {
	case BreakerOpen:
		return false
	case BreakerHalfOpen:
		if e.inFlight >= b.probes {
			return false
		}
		e.inFlight++
		return true
	default:
		return true
	}
}

// RecordSuccess reports that addr answered. A success closes the circuit and
// clears the failure count: the peer is demonstrably reachable.
func (b *Breaker) RecordSuccess(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entryLocked(addr)
	if e.inFlight > 0 {
		e.inFlight--
	}
	e.state = BreakerClosed
	e.failures = 0
}

// RecordFailure reports that addr failed to answer (transport error, timeout, or
// an unexpected status). Reaching the threshold opens the circuit; a failure while
// half-open re-opens it immediately, restarting the cooldown.
func (b *Breaker) RecordFailure(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entryLocked(addr)
	if e.inFlight > 0 {
		e.inFlight--
	}
	e.failures++
	if e.state == BreakerHalfOpen || e.failures >= b.threshold {
		e.state = BreakerOpen
		e.openedAt = b.now()
	}
}

// RecordHardFailure reports that addr could not be reached at all (the dial
// failed or timed out). Unlike a slow or erroring peer, that is definitive, so
// the circuit opens immediately instead of spending the whole failure budget
// rediscovering it one dial timeout at a time — which is what made an abruptly
// dead machine take tens of seconds to route around.
//
// Opening on a single observation is safe because opening is cheap and
// reversible: the cooldown plus the half-open probe brings a recovered peer back
// without operator action.
func (b *Breaker) RecordHardFailure(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entryLocked(addr)
	if e.inFlight > 0 {
		e.inFlight--
	}
	e.failures++
	e.state = BreakerOpen
	e.openedAt = b.now()
}

// State returns addr's current circuit state, applying any due transition.
func (b *Breaker) State(addr string) BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entryLocked(addr).state
}

// Healthy reports whether addr is usable right now (closed or probeable). It does
// not reserve a probe slot, so it is safe for routing decisions.
func (b *Breaker) Healthy(addr string) bool {
	return b.State(addr) != BreakerOpen
}

// OpenCount returns how many peers currently have an open circuit (metrics).
func (b *Breaker) OpenCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for addr := range b.peers {
		if b.entryLocked(addr).state == BreakerOpen {
			n++
		}
	}
	return n
}
