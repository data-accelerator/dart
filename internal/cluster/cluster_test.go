package cluster

import (
	"testing"

	"github.com/data-accelerator/dart/internal/hashring"
)

// TestEpochGolden pins the epoch serialization against an independent Python
// reference (FNV-1a over ID,0x1F,big-endian float64 weight bits,0x1E, then
// fmix64). The epoch is part of the wire protocol (X-DART-Epoch); if this
// breaks, epoch computation drifted and must be treated as a protocol change.
//
// These values changed once, in 2026-08, when State was removed from the hash
// (see TestEpochExcludesState for why).
func TestEpochGolden(t *testing.T) {
	v := NewView([]Member{
		{ID: "node-a", Weight: 1.0, State: Ready},
		{ID: "node-b", Weight: 2.0, State: Suspect},
		{ID: "node-c", Weight: 1.0, State: Ready},
	})
	const want = uint64(448884658194799021)
	if v.Epoch() != want {
		t.Errorf("epoch = %d (%#016x), want %d (%#016x)", v.Epoch(), v.Epoch(), want, want)
	}
	if e := NewView(nil).Epoch(); e != 17280346270528514342 {
		t.Errorf("empty epoch = %d (%#016x), want 17280346270528514342", e, e)
	}
}

// TestEpochExcludesState is the invariant that makes the epoch usable as a
// convergence token at all.
//
// Liveness is derived locally: a node marks a peer Suspect because *it* cannot
// reach it, and two nodes may disagree in good faith. Were State part of the hash,
// nodes holding an identical membership list would still compute different epochs,
// so a mismatch would no longer distinguish "your view is stale" from "we disagree
// about who is up", and the epoch would carry no information.
func TestEpochExcludesState(t *testing.T) {
	ids := []string{"node-a", "node-b", "node-c"}
	weights := []float64{1.0, 2.0, 1.0}

	mk := func(states ...State) *View {
		ms := make([]Member, len(ids))
		for i := range ids {
			ms[i] = Member{ID: ids[i], Weight: weights[i], State: states[i]}
		}
		return NewView(ms)
	}

	base := mk(Ready, Suspect, Ready).Epoch()
	for _, sts := range [][]State{
		{Ready, Ready, Ready},
		{Suspect, Suspect, Suspect},
		{Leaving, Ready, Suspect},
		{Joining, Leaving, Joining},
	} {
		if got := mk(sts...).Epoch(); got != base {
			t.Errorf("states %v gave epoch %d, want %d: State must not affect the epoch", sts, got, base)
		}
	}

	// Weight and ID must still be covered, or the epoch would detect nothing.
	if got := NewView([]Member{
		{ID: "node-a", Weight: 1.0}, {ID: "node-b", Weight: 2.5}, {ID: "node-c", Weight: 1.0},
	}).Epoch(); got == base {
		t.Error("a weight change did not bump the epoch")
	}
	if got := NewView([]Member{
		{ID: "node-a", Weight: 1.0}, {ID: "node-z", Weight: 2.0}, {ID: "node-c", Weight: 1.0},
	}).Epoch(); got == base {
		t.Error("an ID change did not bump the epoch")
	}
}

// TestEpochDeterministicUnderShuffle: epoch (and canonical order) must not
// depend on the input order — the property cross-node convergence relies on.
func TestEpochDeterministicUnderShuffle(t *testing.T) {
	a := []Member{
		{ID: "n1", Weight: 1, State: Ready},
		{ID: "n2", Weight: 3, State: Suspect},
		{ID: "n3", Weight: 1, State: Ready},
		{ID: "n4", Weight: 2, State: Leaving},
	}
	b := []Member{
		{ID: "n4", Weight: 2, State: Leaving},
		{ID: "n1", Weight: 1, State: Ready},
		{ID: "n3", Weight: 1, State: Ready},
		{ID: "n2", Weight: 3, State: Suspect},
	}
	if NewView(a).Epoch() != NewView(b).Epoch() {
		t.Fatal("epoch depends on input order")
	}
}

// TestEpochChangesOnMutation: a change to id or weight changes the epoch. State
// is excluded by design — see TestEpochExcludesState.
func TestEpochChangesOnMutation(t *testing.T) {
	base := NewView([]Member{{ID: "a", Weight: 1, State: Ready}}).Epoch()
	for _, c := range []struct {
		name string
		m    Member
	}{
		{"weight", Member{ID: "a", Weight: 2, State: Ready}},
		{"id", Member{ID: "b", Weight: 1, State: Ready}},
	} {
		if got := NewView([]Member{c.m}).Epoch(); got == base {
			t.Errorf("%s change did not bump epoch", c.name)
		}
	}
	if got := NewView([]Member{{ID: "a", Weight: 1, State: Suspect}}).Epoch(); got != base {
		t.Errorf("state change bumped the epoch (%d != %d)", got, base)
	}
}

// TestReadyLiveFilters: Ready() = Ready only; Live() = Ready+Suspect; both
// sorted by ID with correct weights; Joining/Leaving excluded.
func TestReadyLiveFilters(t *testing.T) {
	v := NewView([]Member{
		{ID: "d", Weight: 4, State: Leaving},
		{ID: "a", Weight: 1, State: Ready},
		{ID: "c", Weight: 3, State: Suspect},
		{ID: "b", Weight: 2, State: Ready},
		{ID: "e", Weight: 5, State: Joining},
	})
	if got := nodeIDs(v.Ready()); !eq(got, []string{"a", "b"}) {
		t.Errorf("Ready ids = %v, want [a b]", got)
	}
	if got := nodeIDs(v.Live()); !eq(got, []string{"a", "b", "c"}) {
		t.Errorf("Live ids = %v, want [a b c]", got)
	}
	for _, n := range v.Live() {
		want := map[string]float64{"a": 1, "b": 2, "c": 3}[n.ID]
		if n.Weight != want {
			t.Errorf("weight for %s = %v, want %v", n.ID, n.Weight, want)
		}
	}
}

// TestDedupDeterministic: duplicate IDs are collapsed to one, deterministically
// regardless of input order.
func TestDedupDeterministic(t *testing.T) {
	v1 := NewView([]Member{
		{ID: "x", Weight: 1, State: Ready},
		{ID: "x", Weight: 2, State: Suspect},
	})
	v2 := NewView([]Member{
		{ID: "x", Weight: 2, State: Suspect},
		{ID: "x", Weight: 1, State: Ready},
	})
	if v1.Len() != 1 || v2.Len() != 1 {
		t.Fatalf("dedup failed: len v1=%d v2=%d", v1.Len(), v2.Len())
	}
	if v1.Epoch() != v2.Epoch() {
		t.Fatal("dedup not deterministic across input order")
	}
}

// TestGet: presence + absence via binary search.
func TestGet(t *testing.T) {
	v := NewView([]Member{
		{ID: "a", Weight: 1, State: Ready},
		{ID: "m", Weight: 1, State: Suspect},
		{ID: "z", Weight: 1, State: Leaving},
	})
	if m, ok := v.Get("m"); !ok || m.State != Suspect {
		t.Errorf("Get(m) = %+v, %v", m, ok)
	}
	if _, ok := v.Get("absent"); ok {
		t.Error("Get(absent) should be false")
	}
}

// TestMembersCopy: mutating the returned slice must not affect the View.
func TestMembersCopy(t *testing.T) {
	v := NewView([]Member{{ID: "a", Weight: 1, State: Ready}})
	ms := v.Members()
	ms[0].Weight = 999
	if got, _ := v.Get("a"); got.Weight != 1 {
		t.Errorf("View mutated via Members() copy: weight=%v", got.Weight)
	}
}

// TestInputNotMutated: NewView must not reorder/modify the caller's slice.
func TestInputNotMutated(t *testing.T) {
	in := []Member{
		{ID: "b", Weight: 1, State: Ready},
		{ID: "a", Weight: 1, State: Ready},
	}
	_ = NewView(in)
	if in[0].ID != "b" || in[1].ID != "a" {
		t.Errorf("NewView reordered the input slice: %v", nodeIDsM(in))
	}
}

// TestFeedsHashring: the Ready() output ranks cleanly through hashring
// (Suspect excluded from placement).
func TestFeedsHashring(t *testing.T) {
	v := NewView([]Member{
		{ID: "a", Weight: 1, State: Ready},
		{ID: "b", Weight: 1, State: Ready},
		{ID: "c", Weight: 1, State: Suspect},
	})
	r := hashring.Rank(42, v.Ready())
	if len(r) != 2 {
		t.Fatalf("placement rank len = %d, want 2 (Ready only)", len(r))
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{
		Joining: "Joining", Ready: "Ready", Suspect: "Suspect",
		Leaving: "Leaving", State(9): "Unknown",
	}
	for s, want := range cases {
		if s.String() != want {
			t.Errorf("State(%d).String() = %q, want %q", s, s.String(), want)
		}
	}
}

// TestAddrCarriedButNotInEpoch: Addr is preserved on the Member (retrievable via
// Get) but does not affect the epoch (it is auxiliary routing metadata).
func TestAddrCarriedButNotInEpoch(t *testing.T) {
	v1 := NewView([]Member{{ID: "a", Addr: "10.0.0.1:9000", Weight: 1, State: Ready}})
	v2 := NewView([]Member{{ID: "a", Addr: "10.0.0.2:9000", Weight: 1, State: Ready}})
	if v1.Epoch() != v2.Epoch() {
		t.Error("Addr must not change the epoch")
	}
	if m, ok := v1.Get("a"); !ok || m.Addr != "10.0.0.1:9000" {
		t.Errorf("Addr not carried: %+v ok=%v", m, ok)
	}
}

// --- helpers ---

func nodeIDs(ns []hashring.Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}

func nodeIDsM(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
