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
	// lastActive stamps the last request activity (Allow or a Record*
	// report). It keys LRU eviction under map pressure: an entry nobody
	// routes to anymore is stale state for a departed address. Read-only
	// observers (State/Healthy/OpenCount) do not stamp it — a metrics
	// scrape must not launder every entry into "recently used".
	lastActive time.Time
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

// maxTrackedPeers is a hard bound on the peer map. Past it, a new entry first
// triggers a sweep of information-free entries (closed, no failures, no probes
// in flight); if the sweep frees nothing — every tracked address still carries
// failure state, as under failed-address churn — the least-recently-active
// entry is evicted instead, so the map can never grow past the cap. Evicting a
// dirty entry resets that peer's circuit to closed, which is safe: the victim
// is by construction the address no request has touched for the longest time,
// and if it is genuinely sick and still in rotation, the next request
// re-creates the entry and re-accumulates the failures.
const maxTrackedPeers = 4096

// entryLocked returns (creating if needed) the peer's circuit, first applying any
// due open -> half-open transition. Caller must hold b.mu.
func (b *Breaker) entryLocked(addr string) *breakerEntry {
	e := b.peers[addr]
	if e == nil {
		if len(b.peers) >= maxTrackedPeers {
			b.sweepLocked()
			if len(b.peers) >= maxTrackedPeers {
				b.evictStalestLocked()
			}
		}
		e = &breakerEntry{state: BreakerClosed}
		b.peers[addr] = e
	}
	if e.state == BreakerOpen && b.now().Sub(e.openedAt) >= b.cooldown {
		e.state = BreakerHalfOpen
		e.inFlight = 0
	}
	return e
}

// sweepLocked drops entries that carry no information: a closed circuit with
// zero consecutive failures and no probes outstanding is behaviorally
// identical to never having seen the peer. Caller must hold b.mu.
func (b *Breaker) sweepLocked() {
	for addr, e := range b.peers {
		if e.state == BreakerClosed && e.failures == 0 && e.inFlight == 0 {
			delete(b.peers, addr)
		}
	}
}

// evictStalestLocked drops the entry with the oldest lastActive stamp — the
// address no request has touched for the longest time. It runs only when the
// sweep found nothing to free, i.e. the map is full of stateful entries, so
// the cost of the O(n) scan is paid once per eviction at the cap, never on
// the hot path. Caller must hold b.mu.
func (b *Breaker) evictStalestLocked() {
	var stalest string
	var at time.Time
	first := true
	for addr, e := range b.peers {
		if first || e.lastActive.Before(at) {
			stalest, at, first = addr, e.lastActive, false
		}
	}
	delete(b.peers, stalest)
}

// Allow reports whether a request to addr may proceed. When it returns true in
// the half-open state it has reserved a probe slot, so the caller MUST report the
// outcome via RecordSuccess or RecordFailure.
func (b *Breaker) Allow(addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entryLocked(addr)
	e.lastActive = b.now() // a routing attempt is activity, even when rejected
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
	e.lastActive = b.now()
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
	e.lastActive = b.now()
	if e.inFlight > 0 {
		e.inFlight--
	}
	e.failures++
	switch {
	case e.state == BreakerHalfOpen:
		// A failed probe re-opens with a fresh cooldown.
		e.state = BreakerOpen
		e.openedAt = b.now()
	case e.state != BreakerOpen && e.failures >= b.threshold:
		e.state = BreakerOpen
		e.openedAt = b.now()
	}
	// A failure that arrives while already Open (a late in-flight request dying)
	// must NOT restamp openedAt: the cooldown starts when the circuit opens,
	// otherwise a steady trickle of late completions pins it open forever.
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
	e.lastActive = b.now()
	if e.inFlight > 0 {
		e.inFlight--
	}
	e.failures++
	if e.state != BreakerOpen {
		e.state = BreakerOpen
		e.openedAt = b.now()
	}
	// Already open: keep the original openedAt (see RecordFailure).
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
