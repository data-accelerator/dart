package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeFetcher answers roster requests from a scripted address -> members map.
type fakeFetcher struct {
	mu      sync.Mutex
	rosters map[string][]Member
	fail    map[string]bool
	calls   int64
}

func (f *fakeFetcher) FetchRoster(_ context.Context, addr string) ([]Member, error) {
	atomic.AddInt64(&f.calls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[addr] {
		return nil, fmt.Errorf("unreachable: %s", addr)
	}
	ms, ok := f.rosters[addr]
	if !ok {
		return nil, fmt.Errorf("no such peer: %s", addr)
	}
	return ms, nil
}

func (f *fakeFetcher) set(addr string, ms ...Member) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rosters == nil {
		f.rosters = map[string][]Member{}
	}
	f.rosters[addr] = ms
}

func (f *fakeFetcher) breaks(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail == nil {
		f.fail = map[string]bool{}
	}
	f.fail[addr] = true
}

// clock is a manual clock so expiry is tested without sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1700000000, 0)} }
func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func ids(v *View) []string {
	out := make([]string, 0, v.Len())
	for _, m := range v.Members() {
		out = append(out, m.ID)
	}
	return out
}

func hasID(v *View, id string) bool {
	_, ok := v.Get(id)
	return ok
}

// TestDynamicConvergesAfterSeedStartsLate is the regression test for a race that a
// live 3-node run actually hit.
//
// Nodes in a cluster start together, so a node routinely dials a seed that is not
// listening yet. If the first failure were allowed to stop further attempts, the
// node would never learn about that peer; with a cyclic seed list (A seeds B, B
// seeds C, C seeds A) every node fails its first attempt and the whole cluster
// stays partitioned. Discovery must keep asking.
func TestDynamicConvergesAfterSeedStartsLate(t *testing.T) {
	f := &fakeFetcher{}
	f.breaks("10.0.0.2:9000") // the seed is not up yet

	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.2:9000"},
		Fetcher: f,
	})

	// Several refreshes while the peer is down: membership is just us, no error state
	// that would prevent recovery.
	for i := 0; i < 3; i++ {
		if v := d.Refresh(context.Background()); v.Len() != 1 {
			t.Fatalf("refresh %d: view %v, want only self", i, ids(v))
		}
	}

	// The peer finishes starting.
	f.mu.Lock()
	delete(f.fail, "10.0.0.2:9000")
	f.mu.Unlock()
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000"})

	v := d.Refresh(context.Background())
	if !hasID(v, "node-b") {
		t.Fatalf("did not converge once the seed came up: %v "+
			"(discovery must keep retrying a seed that failed earlier)", ids(v))
	}
}

// TestDynamicCyclicSeedsConverge: the shape a real deployment produces, where each
// node is seeded with exactly one other and the seeds form a cycle. One reachable
// neighbour has to be enough, in both directions.
func TestDynamicCyclicSeedsConverge(t *testing.T) {
	all := []Member{
		{ID: "A", Addr: "10.0.0.1:9000"},
		{ID: "B", Addr: "10.0.0.2:9000"},
		{ID: "C", Addr: "10.0.0.3:9000"},
	}
	// Seed cycle: A->B, B->C, C->A.
	seeds := map[string]string{"A": all[1].Addr, "B": all[2].Addr, "C": all[0].Addr}

	// One shared fetcher whose rosters reflect what each node currently knows.
	provs := map[string]*DynamicProvider{}
	shared := &liveFetcher{provs: provs}
	for _, self := range all {
		provs[self.Addr] = NewDynamicProvider(DynamicConfig{
			Self:    self,
			Seeder:  StaticSeeder{seeds[self.ID]},
			Fetcher: shared,
		})
	}

	// A few rounds, as the real refresh loop would do.
	for round := 0; round < 4; round++ {
		for _, self := range all {
			provs[self.Addr].Refresh(context.Background())
		}
	}

	var epochs []uint64
	for _, self := range all {
		v := provs[self.Addr].Current()
		if v.Len() != 3 {
			t.Errorf("%s converged to %v, want all 3", self.ID, ids(v))
		}
		epochs = append(epochs, v.Epoch())
	}
	for i := range epochs {
		if epochs[i] != epochs[0] {
			t.Errorf("nodes disagree on epoch after convergence: %v", epochs)
			break
		}
	}
}

// liveFetcher answers from the providers' actual current views, so a test exercises
// real propagation rather than a fixed script. It also performs the inbound half of
// the exchange, as the roster server does.
type liveFetcher struct {
	provs map[string]*DynamicProvider
}

func (l *liveFetcher) FetchRoster(_ context.Context, addr string) ([]Member, error) {
	p, ok := l.provs[addr]
	if !ok {
		return nil, fmt.Errorf("unreachable: %s", addr)
	}
	return p.Current().Members(), nil
}

// TestDynamicHearsayDoesNotKeepDeadMemberAlive is the regression test for a bug a
// live 3-node run exposed, which unit tests with a scripted fetcher had missed.
//
// A, B and C all know each other, then C dies. A asks B for a roster; B still lists
// C, because B has not forgotten it yet either. If hearsay refreshed C's liveness
// clock, A would keep C alive on B's word while B keeps C alive on A's word, and a
// dead node would never be removed by anyone. Only direct contact may refresh the
// clock.
func TestDynamicHearsayDoesNotKeepDeadMemberAlive(t *testing.T) {
	c := newClock()
	all := []Member{
		{ID: "A", Addr: "10.0.0.1:9000"},
		{ID: "B", Addr: "10.0.0.2:9000"},
		{ID: "C", Addr: "10.0.0.3:9000"},
	}
	provs := map[string]*DynamicProvider{}
	shared := &liveFetcher{provs: provs}
	for _, self := range all {
		provs[self.Addr] = NewDynamicProvider(DynamicConfig{
			Self:        self,
			Seeder:      StaticSeeder{all[0].Addr, all[1].Addr, all[2].Addr},
			Fetcher:     shared,
			ForgetAfter: 30 * time.Second,
			Now:         c.now,
		})
	}
	for round := 0; round < 3; round++ {
		for _, self := range all {
			provs[self.Addr].Refresh(context.Background())
		}
	}
	for _, self := range all {
		if v := provs[self.Addr].Current(); v.Len() != 3 {
			t.Fatalf("setup: %s sees %v", self.ID, ids(v))
		}
	}

	// C dies. A and B remain, and each still lists C in the roster it serves.
	delete(provs, all[2].Addr)

	// Well past ForgetAfter, with A and B repeatedly exchanging rosters that both
	// still mention C.
	for round := 0; round < 5; round++ {
		c.advance(10 * time.Second)
		provs[all[0].Addr].Refresh(context.Background())
		provs[all[1].Addr].Refresh(context.Background())
	}

	for _, self := range all[:2] {
		v := provs[self.Addr].Current()
		if hasID(v, "C") {
			t.Errorf("%s still lists the dead node C after 50s with ForgetAfter=30s: %v "+
				"(survivors are refreshing each other's memory of it)", self.ID, ids(v))
		}
		if v.Len() != 2 {
			t.Errorf("%s sees %v, want just A and B", self.ID, ids(v))
		}
	}
}

// TestDynamicDirectContactRefreshesClock is the other half: a peer we keep talking
// to must never be expired, however long the process runs.
func TestDynamicDirectContactRefreshesClock(t *testing.T) {
	c := newClock()
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:        Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:      StaticSeeder{"10.0.0.2:9000"},
		Fetcher:     f,
		ForgetAfter: 30 * time.Second,
		Now:         c.now,
	})
	for i := 0; i < 10; i++ {
		d.Refresh(context.Background())
		c.advance(20 * time.Second) // less than ForgetAfter per step, far more in total
	}
	if v := d.Current(); !hasID(v, "node-b") {
		t.Errorf("a continuously reachable peer was expired after 200s: %v", ids(v))
	}
}

// TestDynamicInboundContactCountsAsEvidence: a peer we cannot dial but that dials us
// is alive and useful, and must not be expired. Reachability can be asymmetric.
func TestDynamicInboundContactCountsAsEvidence(t *testing.T) {
	c := newClock()
	f := &fakeFetcher{}
	f.breaks("10.0.0.2:9000") // we can never reach it

	d := NewDynamicProvider(DynamicConfig{
		Self:        Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:      StaticSeeder{"10.0.0.2:9000"},
		Fetcher:     f,
		ForgetAfter: 30 * time.Second,
		Now:         c.now,
	})

	for i := 0; i < 5; i++ {
		d.LearnPeer("node-b", "10.0.0.2:9000") // it keeps calling us
		c.advance(20 * time.Second)
		d.Refresh(context.Background())
	}
	if v := d.Current(); !hasID(v, "node-b") {
		t.Errorf("a peer that keeps contacting us was expired: %v", ids(v))
	}
}

// TestDynamicSelfAlwaysPresent: before anything resolves, a node must still see
// itself. A node missing from its own view would compute placement over a set it
// is not in and conclude it owns nothing, so it would never cache as an owner.
func TestDynamicSelfAlwaysPresent(t *testing.T) {
	d := NewDynamicProvider(DynamicConfig{
		Self:   Member{ID: "self", Addr: "10.0.0.1:9000"},
		Seeder: StaticSeeder{},
	})
	if !hasID(d.Current(), "self") {
		t.Fatalf("initial view %v lacks self", ids(d.Current()))
	}

	// A failing seeder must not remove it either.
	d2 := NewDynamicProvider(DynamicConfig{
		Self:   Member{ID: "self", Addr: "10.0.0.1:9000"},
		Seeder: errSeeder{},
	})
	d2.Refresh(context.Background())
	if !hasID(d2.Current(), "self") {
		t.Errorf("view %v lacks self after a seeder error", ids(d2.Current()))
	}
}

type errSeeder struct{}

func (errSeeder) Seeds(context.Context) ([]string, error) { return nil, errors.New("resolve failed") }

// TestDynamicLearnsIdentityFromRoster is the core of discovery: a seed provides an
// address, and only the roster exchange can turn that into the stable ID that
// placement requires.
func TestDynamicLearnsIdentityFromRoster(t *testing.T) {
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000", Weight: 1})

	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.2:9000"},
		Fetcher: f,
	})
	v := d.Refresh(context.Background())

	if !hasID(v, "node-b") {
		t.Fatalf("view %v did not learn node-b", ids(v))
	}
	m, _ := v.Get("node-b")
	if m.Addr != "10.0.0.2:9000" {
		t.Errorf("node-b addr = %q", m.Addr)
	}
	// The address itself must never become the identity.
	if hasID(v, "10.0.0.2:9000") {
		t.Error("an address was used as a member ID")
	}
}

// TestDynamicTransitiveDiscovery: a node reached through a seed also reports who
// *it* knows, so one reachable peer is enough to find the rest. This is what makes
// DNS truncation and a partial seed list survivable.
func TestDynamicTransitiveDiscovery(t *testing.T) {
	f := &fakeFetcher{}
	// Only b is seeded, but b knows c and d.
	f.set("10.0.0.2:9000",
		Member{ID: "node-b", Addr: "10.0.0.2:9000"},
		Member{ID: "node-c", Addr: "10.0.0.3:9000"},
		Member{ID: "node-d", Addr: "10.0.0.4:9000"},
	)
	f.set("10.0.0.3:9000", Member{ID: "node-c", Addr: "10.0.0.3:9000"})
	f.set("10.0.0.4:9000", Member{ID: "node-d", Addr: "10.0.0.4:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.2:9000"},
		Fetcher: f,
	})
	v := d.Refresh(context.Background())
	for _, id := range []string{"node-a", "node-b", "node-c", "node-d"} {
		if !hasID(v, id) {
			t.Errorf("view %v is missing %s after one refresh", ids(v), id)
		}
	}
}

// TestDynamicKeepsUnreachablePeer is the asymmetry that protects the cluster: an
// unreachable peer stays in membership. Dropping it immediately would re-run
// placement and move ownership of ~1/N of the keyspace for what may be a
// two-second blip; routing around it is the circuit breaker's job, not
// membership's.
func TestDynamicKeepsUnreachablePeer(t *testing.T) {
	c := newClock()
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:        Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:      StaticSeeder{"10.0.0.2:9000"},
		Fetcher:     f,
		ForgetAfter: 60 * time.Second,
		Now:         c.now,
	})
	if v := d.Refresh(context.Background()); !hasID(v, "node-b") {
		t.Fatalf("setup: %v lacks node-b", ids(v))
	}

	// node-b goes away entirely: unreachable *and* no longer resolved.
	f.breaks("10.0.0.2:9000")
	d = rebindSeeder(d, StaticSeeder{})

	c.advance(30 * time.Second)
	if v := d.Refresh(context.Background()); !hasID(v, "node-b") {
		t.Errorf("node-b dropped after 30s, before ForgetAfter=60s: %v", ids(v))
	}

	c.advance(31 * time.Second) // now past ForgetAfter
	if v := d.Refresh(context.Background()); hasID(v, "node-b") {
		t.Errorf("node-b still present after ForgetAfter elapsed: %v", ids(v))
	}
}

// rebindSeeder returns a provider sharing d's learned state but with a new seeder,
// which is how the tests simulate a peer disappearing from DNS.
func rebindSeeder(d *DynamicProvider, s Seeder) *DynamicProvider {
	cfg := d.cfg
	cfg.Seeder = s
	nd := NewDynamicProvider(cfg)
	nd.known = d.known
	return nd
}

// TestDynamicReaddResetsExpiry: a peer that comes back before ForgetAfter must not
// be dropped later on account of its earlier absence.
func TestDynamicReaddResetsExpiry(t *testing.T) {
	c := newClock()
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:        Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:      StaticSeeder{"10.0.0.2:9000"},
		Fetcher:     f,
		ForgetAfter: 60 * time.Second,
		Now:         c.now,
	})
	d.Refresh(context.Background())

	c.advance(50 * time.Second)
	d.Refresh(context.Background()) // still reachable: lastSeen refreshed
	c.advance(50 * time.Second)
	if v := d.Refresh(context.Background()); !hasID(v, "node-b") {
		t.Errorf("a continuously reachable peer was expired: %v", ids(v))
	}
}

// TestDynamicAddressChangeFollowed: a pod recreated with a new IP keeps its node
// identity. Membership must follow the new address, or every request would go to
// an address nothing is listening on.
func TestDynamicAddressChangeFollowed(t *testing.T) {
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.2:9000"},
		Fetcher: f,
	})
	d.Refresh(context.Background())

	// Same ID, new address. The old address stops answering, which is what a pod
	// recreation actually looks like: the previous IP is gone. Leaving it
	// answering would model two live endpoints claiming one identity, and the
	// merged address would then depend on which reply arrived last (Refresh asks
	// seeds *and* known peers, concurrently) rather than on the code under test.
	f.set("10.0.0.9:9000", Member{ID: "node-b", Addr: "10.0.0.9:9000"})
	f.breaks("10.0.0.2:9000")
	d = rebindSeeder(d, StaticSeeder{"10.0.0.9:9000"})
	v := d.Refresh(context.Background())

	m, ok := v.Get("node-b")
	if !ok {
		t.Fatalf("node-b vanished: %v", ids(v))
	}
	if m.Addr != "10.0.0.9:9000" {
		t.Errorf("node-b addr = %q, want the new address 10.0.0.9:9000", m.Addr)
	}
	if v.Len() != 2 {
		t.Errorf("view has %d members (%v), want 2: an address change must not duplicate a node", v.Len(), ids(v))
	}
}

// TestDynamicLearnPeerBidirectional: a node with nothing to seed from must still
// end up in the cluster. Whoever contacts it teaches it, which is the inbound half
// of the exchange; without it the first node to start could stay isolated forever.
func TestDynamicLearnPeerBidirectional(t *testing.T) {
	d := NewDynamicProvider(DynamicConfig{
		Self:   Member{ID: "first", Addr: "10.0.0.1:9000"},
		Seeder: StaticSeeder{}, // nothing to seed from
	})
	if v := d.Refresh(context.Background()); v.Len() != 1 {
		t.Fatalf("expected a lone view, got %v", ids(v))
	}

	// Someone calls our roster endpoint; the server hands us their identity.
	d.LearnPeer("late", "10.0.0.2:9000")
	v := d.Refresh(context.Background())
	if !hasID(v, "late") {
		t.Errorf("view %v did not learn the inbound peer", ids(v))
	}
}

// TestDynamicWireStateIgnored: State must never be taken from a peer. It is a
// local judgement, and importing a peer's opinion would let one node's transient
// failure propagate as cluster-wide membership churn.
func TestDynamicWireStateIgnored(t *testing.T) {
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000",
		Member{ID: "node-b", Addr: "10.0.0.2:9000", State: Leaving},
		Member{ID: "node-c", Addr: "10.0.0.3:9000", State: Suspect},
	)
	f.set("10.0.0.3:9000", Member{ID: "node-c", Addr: "10.0.0.3:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.2:9000"},
		Fetcher: f,
	})
	v := d.Refresh(context.Background())

	for _, id := range []string{"node-b", "node-c"} {
		m, ok := v.Get(id)
		if !ok {
			t.Fatalf("%s missing: %v", id, ids(v))
		}
		if m.State != Ready {
			t.Errorf("%s state = %v, want Ready: state must not come from the wire", id, m.State)
		}
	}
	// All members Ready means all participate in placement.
	if len(v.Ready()) != 3 {
		t.Errorf("Ready() has %d members, want 3", len(v.Ready()))
	}
}

// TestDynamicEpochStableAcrossNodes ties discovery back to the epoch's purpose:
// two nodes that have converged on the same membership must agree on the epoch,
// even though each considers *itself* the local node.
func TestDynamicEpochStableAcrossNodes(t *testing.T) {
	all := []Member{
		{ID: "node-a", Addr: "10.0.0.1:9000"},
		{ID: "node-b", Addr: "10.0.0.2:9000"},
		{ID: "node-c", Addr: "10.0.0.3:9000"},
	}
	f := &fakeFetcher{}
	for _, m := range all {
		f.set(m.Addr, all...)
	}

	var epochs []uint64
	for _, self := range all {
		d := NewDynamicProvider(DynamicConfig{
			Self:    self,
			Seeder:  StaticSeeder{all[0].Addr, all[1].Addr, all[2].Addr},
			Fetcher: f,
		})
		v := d.Refresh(context.Background())
		if v.Len() != 3 {
			t.Fatalf("%s converged to %v, want 3 members", self.ID, ids(v))
		}
		epochs = append(epochs, v.Epoch())
	}
	for i := 1; i < len(epochs); i++ {
		if epochs[i] != epochs[0] {
			t.Errorf("epoch mismatch across converged nodes: %v", epochs)
			break
		}
	}
}

// TestDynamicSurvivesSeederOutage: when DNS fails, the peers already known are
// still asked, so the cluster does not disintegrate because of a resolver problem.
func TestDynamicSurvivesSeederOutage(t *testing.T) {
	f := &fakeFetcher{}
	f.set("10.0.0.2:9000", Member{ID: "node-b", Addr: "10.0.0.2:9000"})

	var errs int
	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.2:9000"},
		Fetcher: f,
		OnError: func(error) { errs++ },
	})
	d.Refresh(context.Background())

	// DNS breaks entirely.
	d = rebindSeeder(d, errSeeder{})
	d.cfg.OnError = func(error) { errs++ }
	v := d.Refresh(context.Background())

	if !hasID(v, "node-b") {
		t.Errorf("node-b lost during a seeder outage: %v", ids(v))
	}
	if errs == 0 {
		t.Error("the seeder error was not reported")
	}
	if atomic.LoadInt64(&f.calls) < 2 {
		t.Error("known peers were not re-contacted during the outage")
	}
}

// TestDynamicDoesNotFetchItself: asking ourselves for a roster would be a wasted
// round trip on every refresh.
func TestDynamicDoesNotFetchItself(t *testing.T) {
	f := &fakeFetcher{}
	f.set("10.0.0.1:9000", Member{ID: "node-a", Addr: "10.0.0.1:9000"})

	d := NewDynamicProvider(DynamicConfig{
		Self:    Member{ID: "node-a", Addr: "10.0.0.1:9000"},
		Seeder:  StaticSeeder{"10.0.0.1:9000"},
		Fetcher: f,
	})
	d.Refresh(context.Background())
	if n := atomic.LoadInt64(&f.calls); n != 0 {
		t.Errorf("fetched %d rosters, want 0 (only self was seeded)", n)
	}
}

func TestDynamicRunStopsOnContextCancel(t *testing.T) {
	d := NewDynamicProvider(DynamicConfig{
		Self:            Member{ID: "a", Addr: "10.0.0.1:9000"},
		Seeder:          StaticSeeder{},
		RefreshInterval: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
