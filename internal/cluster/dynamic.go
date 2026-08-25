package cluster

import (
	"context"
	"sort"
	"sync"
	"time"
)

// RosterFetcher asks the node at addr for its membership view.
//
// The returned slice MUST include the responder itself, since that entry is how
// its stable ID becomes known — a seed gives an address, and an address is never
// an identity. State on the returned members is ignored: liveness is local.
//
// responderID is the stable ID of the member that actually answered, from its
// own self-identification. It is the *only* identity direct contact may be
// credited to: a dialed address proves nothing about which member answered (a
// recycled pod IP answers for a member that is gone), so an empty responderID
// earns no liveness credit at all.
//
// This is an interface rather than a concrete transport so that membership does
// not depend on the peer package, and so tests can drive convergence without
// sockets.
type RosterFetcher interface {
	FetchRoster(ctx context.Context, addr string) (members []Member, responderID string, err error)
}

// Defaults for DynamicProvider.
const (
	// DefaultRefreshInterval is how often seeds are re-resolved and rosters
	// re-fetched. A few seconds is plenty: the dominant term in how long a new
	// node takes to be discovered is its own image pull and startup, not this.
	DefaultRefreshInterval = 5 * time.Second

	// DefaultForgetAfter is how long a member must stay both unseen and
	// unreachable before it is dropped from membership.
	//
	// It is deliberately much larger than the refresh interval, because adding and
	// removing a member are not symmetric operations. Adding is cheap. Removing
	// re-runs placement, moves ownership of ~1/N of the keyspace, and re-forms the
	// distribution tree; if a flapping node were added and dropped repeatedly, the
	// whole cluster would keep paying that cost. Routing around an unreachable peer
	// is already handled in seconds, locally, by the circuit breaker — so removal
	// can afford to be slow, and should be.
	DefaultForgetAfter = 60 * time.Second
)

// DynamicConfig configures a DynamicProvider. It is plain data, separate from the
// provider itself so a provider (which owns a mutex) is never copied.
type DynamicConfig struct {
	// Self is this node. It is always present in the published View, even before
	// any seed resolves — a node that did not know itself would compute placement
	// over a set it is not in and conclude it owns nothing.
	Self Member

	// Seeder supplies candidate addresses. Required.
	Seeder Seeder

	// Fetcher exchanges rosters. Optional: without it, seeds still provide
	// addresses but their identities can never be learned, so no peer can join
	// membership. Effectively required for P2P.
	Fetcher RosterFetcher

	// RefreshInterval defaults to DefaultRefreshInterval.
	RefreshInterval time.Duration
	// ForgetAfter defaults to DefaultForgetAfter.
	ForgetAfter time.Duration

	// Now overrides the clock (tests).
	Now func() time.Time

	// OnError, if set, receives non-fatal refresh errors (resolution failures,
	// unreachable seeds). Refresh continues regardless.
	OnError func(error)
}

// DynamicProvider maintains membership from a Seeder plus roster exchange.
//
// The split is deliberate. The Seeder answers the environment-specific question
// ("what addresses might be peers?") and can be DNS, a static list or anything
// else. Roster exchange then answers the environment-independent questions ("what
// are their stable identities, and who else do they know?") over DART's own
// connections, so the hard half of discovery does not depend on the platform.
//
// Liveness is *not* part of this. A peer that cannot be reached keeps its place in
// membership until ForgetAfter elapses; requests route around it immediately via
// the circuit breaker. That separation is what keeps a transient failure from
// triggering cluster-wide re-placement.
//
// A DynamicProvider is safe for concurrent use. Construct one with
// NewDynamicProvider.
type DynamicProvider struct {
	cfg   DynamicConfig
	inner *StaticProvider

	mu    sync.Mutex
	known map[string]*tracked // by member ID
}

// tracked is a known peer plus the bookkeeping that decides when to forget it.
type tracked struct {
	member Member
	// lastContact is when we last had *direct* evidence this member exists: a
	// successful roster fetch from it, or a request it made to us.
	//
	// It is deliberately not refreshed by hearsay. Members learned from a peer's
	// roster are added, but their clock is not reset, because every surviving peer
	// keeps listing a dead node in its own roster for as long as it remembers it —
	// so if hearsay refreshed the clock, two survivors would refresh each other's
	// memory of the dead node indefinitely and it would never be forgotten. Only
	// something we observed ourselves counts as evidence of life.
	lastContact time.Time
}

// NewDynamicProvider creates a provider whose initial View contains only self.
func NewDynamicProvider(cfg DynamicConfig) *DynamicProvider {
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = DefaultRefreshInterval
	}
	if cfg.ForgetAfter <= 0 {
		cfg.ForgetAfter = DefaultForgetAfter
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	// NaN/±Inf need no guard: weights arrive via JSON (which cannot represent
	// them) or are hardcoded — see A4 of docs/design-assumptions.md.
	if cfg.Self.Weight <= 0 {
		cfg.Self.Weight = 1
	}
	cfg.Self.State = Ready
	return &DynamicProvider{
		cfg:   cfg,
		known: make(map[string]*tracked),
		inner: NewStaticProvider(cfg.Self),
	}
}

// Self returns this node's member entry.
func (d *DynamicProvider) Self() Member { return d.cfg.Self }

// Current implements Provider. Lock-free.
func (d *DynamicProvider) Current() *View { return d.inner.Current() }

// Subscribe implements Provider.
func (d *DynamicProvider) Subscribe() (<-chan *View, func()) { return d.inner.Subscribe() }

// Run refreshes membership until ctx is cancelled. It performs one refresh
// immediately so a caller does not have to wait a full interval for the first
// view.
func (d *DynamicProvider) Run(ctx context.Context) {
	d.Refresh(ctx)
	t := time.NewTicker(d.cfg.RefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.Refresh(ctx)
		}
	}
}

// Refresh performs one discovery pass: resolve seeds, fetch rosters, merge, expire
// and publish. It is exported so tests (and an admin trigger) can step it
// deterministically instead of waiting on a ticker.
func (d *DynamicProvider) Refresh(ctx context.Context) *View {
	addrs, err := d.cfg.Seeder.Seeds(ctx)
	if err != nil {
		d.reportError(err)
		// Keep going with the addresses already known: a DNS outage must not empty
		// membership, since the peers themselves are probably fine.
	}

	// Ask both the seeds and the peers already known. Including known peers is what
	// keeps the cluster together when DNS is failing or its answer was truncated.
	targets := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		targets[a] = struct{}{}
	}
	for _, t := range d.snapshot() {
		if t.member.Addr != "" {
			targets[t.member.Addr] = struct{}{}
		}
	}
	delete(targets, d.cfg.Self.Addr) // no need to ask ourselves

	if d.cfg.Fetcher != nil && len(targets) > 0 {
		d.gather(ctx, targets)
	}

	return d.publish()
}

// gather fetches rosters from every target concurrently and learns what comes
// back. Failures are reported and otherwise ignored: an unreachable peer is not a
// reason to change membership.
func (d *DynamicProvider) gather(ctx context.Context, targets map[string]struct{}) {
	type result struct {
		responder string
		members   []Member
		err       error
	}
	results := make(chan result, len(targets))
	for addr := range targets {
		go func(addr string) {
			ms, responder, err := d.cfg.Fetcher.FetchRoster(ctx, addr)
			results <- result{responder, ms, err}
		}(addr)
	}
	for i := 0; i < cap(results); i++ {
		select {
		case r := <-results:
			if r.err != nil {
				d.reportError(r.err)
				continue
			}
			// A reply is direct evidence that the member that answered is alive;
			// the rest of the roster is hearsay.
			d.Learn(r.members...)
			d.confirm(r.responder)
		case <-ctx.Done():
			return
		}
	}
}

// confirm records direct contact with the member whose stable ID is
// responderID. Crediting by identity — never by dialed address — is
// load-bearing: an address can be recycled to a different member (pod-IP
// reuse), and crediting by address would keep the previous owner's liveness
// clock alive indefinitely, pinning a dead member in placement. A responder
// that does not identify itself earns no credit.
func (d *DynamicProvider) confirm(responderID string) {
	if responderID == "" {
		return
	}
	now := d.cfg.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.known[responderID]; ok {
		t.lastContact = now
	}
}

// Learn records members reported by a peer's roster. This is *hearsay*: it is
// enough to add a member, which is how one reachable neighbour is enough to find
// the whole cluster, but it does not refresh the liveness clock of a member we
// already know. See tracked.lastContact for why that distinction is load-bearing.
//
// Adding is immediate and unconditional, in contrast to forgetting. Adding costs
// one placement recomputation; the risk of being wrong is a request sent to a node
// that turns out not to hold the block, which is safe.
func (d *DynamicProvider) Learn(members ...Member) {
	if len(members) == 0 {
		return
	}
	now := d.cfg.Now()
	d.mu.Lock()
	for _, m := range members {
		if m.ID == "" || m.ID == d.cfg.Self.ID {
			continue // self is always present; an anonymous member is unusable
		}
		// NaN cannot arrive here (JSON cannot represent it; A4).
		if m.Weight <= 0 {
			m.Weight = 1
		}
		// State is never taken from the wire: it is a local judgement.
		m.State = Ready
		t, ok := d.known[m.ID]
		if !ok {
			// New to us. Give it a full grace period to be confirmed directly.
			d.known[m.ID] = &tracked{member: m, lastContact: now}
			continue
		}
		// A member's address can legitimately change — a pod is recreated with a new
		// IP while keeping its node identity. Take the newest address, or we would
		// keep dialing an address nobody is listening on. But hearsay is NOT
		// liveness evidence: the address change updates Addr only and deliberately
		// does not touch lastContact (§3.9 of docs/cluster.md). Otherwise two
		// survivors flip-flopping conflicting address reports about a dead node
		// would refresh each other's memory of it forever.
		t.member = m
	}
	d.mu.Unlock()
}

// LearnPeer records an inbound contact: a peer asked us for our roster, which is
// direct evidence it is alive and therefore does refresh its liveness clock.
//
// This is also the inbound half of the exchange, which keeps the node that started
// first — and so had nothing to seed from — from staying isolated. It additionally
// covers asymmetric reachability: a peer we cannot dial but that can dial us stays
// in membership.
func (d *DynamicProvider) LearnPeer(id, addr string) {
	if id == "" || id == d.cfg.Self.ID {
		return
	}
	d.Learn(Member{ID: id, Addr: addr, Weight: 1})
	now := d.cfg.Now()
	d.mu.Lock()
	if t, ok := d.known[id]; ok {
		t.lastContact = now
	}
	d.mu.Unlock()
}

// Roster returns the current membership as a plain slice, for serving to peers.
func (d *DynamicProvider) Roster() []Member {
	v := d.Current()
	return v.Members()
}

// publish expires stale members and installs a new View. It returns the View.
func (d *DynamicProvider) publish() *View {
	now := d.cfg.Now()
	members := []Member{d.cfg.Self}

	d.mu.Lock()
	for id, t := range d.known {
		if now.Sub(t.lastContact) > d.cfg.ForgetAfter {
			delete(d.known, id)
			continue
		}
		members = append(members, t.member)
	}
	d.mu.Unlock()

	// Sorting is not required by NewView (it canonicalizes) but makes the published
	// order stable for anything that logs or diffs it.
	sort.Slice(members, func(a, b int) bool { return members[a].ID < members[b].ID })
	return d.inner.Set(members)
}

// snapshot copies the tracked set under lock.
func (d *DynamicProvider) snapshot() []tracked {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]tracked, 0, len(d.known))
	for _, t := range d.known {
		out = append(out, *t)
	}
	return out
}

func (d *DynamicProvider) reportError(err error) {
	if d.cfg.OnError != nil {
		d.cfg.OnError(err)
	}
}
