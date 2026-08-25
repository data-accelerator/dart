package engine

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
)

// Hedging bounds tail latency on the peer path.
//
// The failure paths already have fallbacks: a 404 or a transport error falls
// straight through to origin. What has no fallback is a peer that is *alive but
// slow* (GC pause, disk hitch, or itself waiting on its own parent). In a
// distribution tree that stall cascades to the entire subtree below it, so one
// unlucky node can hold up many readers.
//
// A hedge sends a duplicate request to an alternative source once the primary
// has exceeded a latency threshold, takes whichever answer arrives first, and
// cancels the loser. Hedging trades duplicate work for latency, so it is
// deliberately constrained:
//
//   - delayed, never immediate: fire only after the estimated p99, so the common
//     case costs nothing;
//   - rate limited: at most HedgeRatio of requests may hedge, because a cluster
//     that is uniformly slow would otherwise double its own load and amplify the
//     very congestion it is reacting to;
//   - the loser is cancelled so its bandwidth is reclaimed.

// hedgeDefaults.
const (
	// defaultHedgeRatio caps hedges at 5% of peer fetches.
	defaultHedgeRatio = 0.05
	// minHedgeDelay floors the trigger so a fast cluster does not hedge on noise.
	minHedgeDelay = 5 * time.Millisecond
	// maxHedgeDelay caps the trigger so a pathological sample cannot disable
	// hedging entirely.
	maxHedgeDelay = 2 * time.Second
	// latencyWindow is how many recent samples the estimator keeps.
	latencyWindow = 256
)

// latencyEstimator keeps a bounded ring of recent peer-fetch durations and
// reports a quantile. It is intentionally simple: an exact quantile over a small
// window is cheaper and easier to reason about than a streaming sketch, and the
// window only needs to track "what does a healthy fetch cost right now".
type latencyEstimator struct {
	mu      sync.Mutex
	samples []time.Duration
	next    int
	filled  bool
}

func newLatencyEstimator() *latencyEstimator {
	return &latencyEstimator{samples: make([]time.Duration, latencyWindow)}
}

// Observe records one completed fetch.
func (l *latencyEstimator) Observe(d time.Duration) {
	l.mu.Lock()
	l.samples[l.next] = d
	l.next = (l.next + 1) % len(l.samples)
	if l.next == 0 {
		l.filled = true
	}
	l.mu.Unlock()
}

// quantile returns the q-quantile (0<q<1) of the window, and whether there were
// enough samples to be meaningful.
func (l *latencyEstimator) quantile(q float64) (time.Duration, bool) {
	l.mu.Lock()
	n := len(l.samples)
	if !l.filled {
		n = l.next
	}
	if n < 16 { // too few samples to trust
		l.mu.Unlock()
		return 0, false
	}
	cp := make([]time.Duration, n)
	copy(cp, l.samples[:n])
	l.mu.Unlock()

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(q * float64(n))
	if idx >= n {
		idx = n - 1
	}
	return cp[idx], true
}

// hedgeLimiter enforces the "at most ratio of requests may hedge" rule using a
// simple token accounting: each attempt earns ratio tokens, each hedge spends
// one. That keeps the long-run hedge rate at the ratio without needing a clock.
type hedgeLimiter struct {
	mu     sync.Mutex
	ratio  float64
	tokens float64
}

func newHedgeLimiter(ratio float64) *hedgeLimiter {
	if ratio <= 0 {
		ratio = defaultHedgeRatio
	}
	if ratio > 1 {
		ratio = 1
	}
	return &hedgeLimiter{ratio: ratio}
}

// allow accounts for one fetch attempt and reports whether it may hedge.
func (h *hedgeLimiter) allow() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tokens += h.ratio
	// Never bank more than one hedge worth of credit, so a long quiet period
	// cannot unleash a burst of duplicates.
	if h.tokens > 1 {
		h.tokens = 1
	}
	if h.tokens >= 1 {
		h.tokens -= 1
		return true
	}
	return false
}

// hedgeDelay returns how long to wait before hedging, or ok=false when hedging
// is disabled or the estimator has too little data.
func (e *Engine) hedgeDelay() (time.Duration, bool) {
	if !e.hedgeEnabled {
		return 0, false
	}
	d, ok := e.latency.quantile(0.99)
	if !ok {
		return 0, false
	}
	if d < minHedgeDelay {
		d = minHedgeDelay
	}
	if d > maxHedgeDelay {
		d = maxHedgeDelay
	}
	return d, true
}

// peerResult is one contender's outcome in a hedged fetch.
type peerResult struct {
	data []byte
	held bool
	err  error
	from string
}

// fetchHedged asks primary for the block, with two distinct escalations to
// backup:
//
//   - **failover** — primary definitively failed (error, or it does not hold the
//     block). Try backup immediately. This is *not* rate limited: it is a reaction
//     to a known-bad peer, not speculation, and throttling it would only push the
//     read to origin while a peer that almost certainly has the block sits idle.
//   - **hedge** — primary is merely slow (past the estimated p99). Race a
//     duplicate. This *is* rate limited, because when a whole cluster is slow,
//     unthrottled duplicates double its load and amplify the congestion.
//
// The first answer with held=true wins and the loser is cancelled. If neither
// contender can serve the block, ok is false and the caller falls back to origin.
func (e *Engine) fetchHedged(ctx context.Context, primary, backup string, req peer.BlockRequest) ([]byte, bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancels the loser (and any straggler) on return

	results := make(chan peerResult, 2)
	launch := func(addr string) {
		go func() {
			start := time.Now()
			data, held, err := e.peer.Get(ctx, addr, req)
			if err == nil && held {
				// Only a real fetch informs the latency estimate: a fast 404
				// miss would otherwise collapse the p99 to the floor and arm
				// hedges a few milliseconds into genuine fetches.
				e.latency.Observe(time.Since(start))
			}
			results <- peerResult{data: data, held: held, err: err, from: addr}
		}()
	}

	launch(primary)
	contenders := 1
	backupUsable := backup != "" && backup != primary
	backupLaunched := false

	// Arm the speculative hedge only if the estimator has data and the rate limit
	// allows it. Failover below does not consult either.
	var timerC <-chan time.Time
	hedgeFired := false
	if backupUsable {
		if delay, ok := e.hedgeDelay(); ok && e.hedges.allow() {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			timerC = timer.C
		}
	}

	for contenders > 0 {
		select {
		case <-timerC:
			// The primary is slower than p99: race a duplicate against it.
			timerC = nil
			if !backupLaunched {
				launch(backup)
				backupLaunched = true
				hedgeFired = true
				contenders++
				e.mx.recordHedge()
			}
		case r := <-results:
			contenders--
			if r.err == nil && r.held {
				// Hedge-win metrics only mean something when a hedge actually
				// fired; recording a win for every fetch (or for a failover)
				// makes the prescribed backup_won/fired comparison
				// uninterpretable exactly when hedging is off.
				if hedgeFired {
					e.mx.recordHedgeWin(r.from == primary)
				}
				return r.data, true
			}
			// Definite failure: fail over now, regardless of the hedge budget.
			if backupUsable && !backupLaunched {
				timerC = nil
				launch(backup)
				backupLaunched = true
				contenders++
				e.mx.recordFailover()
			}
		case <-ctx.Done():
			return nil, false
		}
	}
	return nil, false
}

// hedgeTargets picks the primary (tree parent) and a backup for a chunk. The
// backup is the grandparent when one exists, else the tree root (owner): both sit
// closer to the source, so they are more likely to already hold the block.
//
// Peers whose circuit is open are skipped: routing to a known-sick peer would
// only spend a request to learn what the breaker already knows. If the parent is
// open we walk further up the tree, so a dead branch is routed around rather than
// forcing every reader beneath it to fall back to origin.
func (e *Engine) hedgeTargets(ranked []hashring.Node, view *cluster.View) (primary, backup string) {
	if len(ranked) == 0 {
		return "", ""
	}
	selfIdx := -1
	for i, n := range ranked {
		if n.ID == e.selfID {
			selfIdx = i
			break
		}
	}
	// addrOf resolves a rank to a usable peer address, or "" when it has none or
	// its circuit is open.
	addrOf := func(i int) string {
		if i < 0 || i >= len(ranked) {
			return ""
		}
		m, ok := view.Get(ranked[i].ID)
		if !ok || m.Addr == "" {
			return ""
		}
		if e.peer != nil && e.peer.Breaker != nil && !e.peer.Breaker.Healthy(m.Addr) {
			return ""
		}
		return m.Addr
	}

	if selfIdx < 0 {
		// Not a member of this tree: the owner is our upstream, no backup above it.
		return addrOf(0), ""
	}

	// Walk up the ancestor chain collecting the first two usable addresses, so an
	// open circuit anywhere above us is skipped instead of stalling the branch.
	var usable []string
	for idx := hashring.Parent(selfIdx, len(ranked), e.fanout); idx >= 0 && len(usable) < 2; idx = hashring.Parent(idx, len(ranked), e.fanout) {
		if a := addrOf(idx); a != "" {
			usable = append(usable, a)
		}
		if idx == 0 {
			break // reached the root
		}
	}
	switch len(usable) {
	case 0:
		return "", "" // we are the root, or every ancestor is unusable
	case 1:
		return usable[0], ""
	default:
		return usable[0], usable[1]
	}
}
