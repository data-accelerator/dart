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
	// After the first success, admission pauses until admitted probes finish.
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
	state      BreakerState
	generation uint64    // invalidates results from requests admitted before a transition
	failures   int       // consecutive failures
	openedAt   time.Time // when the circuit last opened
	inFlight   int       // half-open probes currently outstanding
	recovering bool      // freeze admission while a successful probe awaits siblings
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
		e.recovering = false
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
	_, allowed := b.begin(addr, true)
	return allowed
}

// breakerPermit binds a Client result to the entry and generation at request
// admission. Entry identity also protects against eviction/reuse of an address.
type breakerPermit struct {
	breaker    *Breaker
	entry      *breakerEntry
	generation uint64
	probe      bool // this request owns a half-open probe slot
}

// begin can bypass admission for discovery while still scoping its result.
func (b *Breaker) begin(addr string, enforce bool) (breakerPermit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entryLocked(addr)
	e.lastActive = b.now()
	if enforce {
		switch e.state {
		case BreakerOpen:
			return breakerPermit{}, false
		case BreakerHalfOpen:
			if e.recovering || e.inFlight >= b.probes {
				return breakerPermit{}, false
			}
			e.inFlight++
		}
	}
	return breakerPermit{breaker: b, entry: e, generation: e.generation, probe: enforce && e.state == BreakerHalfOpen}, true
}

// RecordSuccess reports that addr answered. After outstanding probes finish,
// success closes the circuit and
// clears the failure count: the peer is demonstrably reachable.
func (b *Breaker) RecordSuccess(addr string) {
	b.record(addr, outcomeAnswered, nil)
}

// RecordFailure reports that addr failed to answer (transport error, timeout, or
// an unexpected status). Reaching the threshold opens the circuit; a failure while
// half-open re-opens it immediately, restarting the cooldown.
func (b *Breaker) RecordFailure(addr string) {
	b.record(addr, outcomeSoftFail, nil)
}

// RecordHardFailure reports a definitive peer failure: a failed dial, or a
// validated block protocol violation despite a completed HTTP exchange. It
// opens immediately rather than spending the whole failure budget rediscovering
// an unusable peer one request at a time.
//
// Opening on a single observation is safe because opening is cheap and
// reversible: the cooldown plus the half-open probe brings a recovered peer back
// without operator action.
func (b *Breaker) RecordHardFailure(addr string) {
	b.record(addr, outcomeHardFail, nil)
}

// record validates the request generation and updates its outcome under one
// lock. An old completion cannot close a newer quarantine, consume a current
// half-open probe, or recreate an entry evicted while that request was flying.
// The public Record* methods retain their explicit, unscoped reporting API;
// Client uses permits so delayed network completions are generation-safe.
func (b *Breaker) record(addr string, o outcome, permit *breakerPermit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if permit != nil {
		e := b.peers[addr]
		if e != permit.entry || e.generation != permit.generation {
			return
		}
	}
	e := b.entryLocked(addr)
	e.lastActive = b.now()
	if e.inFlight > 0 && (permit == nil || permit.probe) {
		e.inFlight--
	}
	if o == outcomeAnswered {
		if e.state == BreakerHalfOpen && e.inFlight > 0 {
			// Keep admitted siblings in the current generation until all
			// report. A failure must still veto a preceding success. Freeze
			// admission so a busy peer cannot grow this cohort forever.
			e.recovering = true
			return
		}
		if e.state != BreakerClosed {
			e.generation++
			e.inFlight = 0
		}
		e.state, e.failures = BreakerClosed, 0
		e.recovering = false
		return
	}
	e.failures++
	if e.state != BreakerOpen && (o == outcomeHardFail || e.state == BreakerHalfOpen || e.failures >= b.threshold) {
		e.state = BreakerOpen
		e.generation++
		e.inFlight = 0
		e.recovering = false
		e.openedAt = b.now()
	}
	// Already-open failures never extend cooldown; old permits are ignored.
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
