package hashring

import (
	"math"
	"math/rand"
	"testing"
)

// --- Golden values, computed by the independent Python reference
// implementation contrib/golden/golden_reference.py (tracked in this repo; CI
// diffs it against the live Go code under DART_GOLDEN_REF=1). These are the
// real determinism guard: if the Go hash ever drifts from the specified
// construction, these break. Do NOT regenerate them with the Go code itself
// (that would be a tautology) — recompute with the Python reference and treat
// any change as a ring-breaking event.

func TestHash64Golden(t *testing.T) {
	cases := []struct {
		key  uint64
		id   string
		want uint64
	}{
		{0, "node-a", 16511540948091628860},
		{1, "node-a", 16141481610525751987},
		{0, "node-b", 16852417870494220010},
		{0xDEADBEEFCAFEBABE, "node-0", 15394106576409441172},
		{42, "kube-node-01", 16063926426682601568},
	}
	for _, c := range cases {
		if got := Hash64(c.key, c.id); got != c.want {
			t.Errorf("Hash64(%#x, %q) = %d (%#016x), want %d (%#016x)",
				c.key, c.id, got, got, c.want, c.want)
		}
	}
}

// TestRankGolden pins a full ordering against the independent reference. For
// equal weights the HRW order is a strict function of the 64-bit hashes
// (descending, ties by ID), so the expected order below is float-free and was
// produced by the Python reference. This also proves the Go float score path
// yields the identical order to the integer reference.
func TestRankGolden(t *testing.T) {
	nodes := makeNodes(10, 1) // node-00 .. node-09, weight 1
	const key = uint64(0xDEADBEEFCAFEBABE)
	want := []string{
		"node-03", "node-04", "node-05", "node-07", "node-08",
		"node-00", "node-09", "node-06", "node-01", "node-02",
	}
	got := ids(Rank(key, nodes))
	if !equalStrings(got, want) {
		t.Errorf("Rank order mismatch\n got: %v\nwant: %v", got, want)
	}
}

// TestRankShuffleInvariant is the core coordination-free guarantee: the ranking
// must be independent of the order members are supplied in. We shuffle the
// input many times and require byte-identical output every time.
func TestRankShuffleInvariant(t *testing.T) {
	base := makeNodes(50, 1)
	keys := []uint64{0, 1, 7, 1024, 0xFFFFFFFFFFFFFFFF, 0xDEADBEEF}
	rng := rand.New(rand.NewSource(12345))
	for _, key := range keys {
		want := ids(Rank(key, base))
		for trial := 0; trial < 200; trial++ {
			shuf := make([]Node, len(base))
			copy(shuf, base)
			rng.Shuffle(len(shuf), func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })
			if got := ids(Rank(key, shuf)); !equalStrings(got, want) {
				t.Fatalf("key %#x trial %d: order depends on input order\n got: %v\nwant: %v",
					key, trial, got, want)
			}
		}
	}
}

// TestRankDoesNotMutateInput guards that callers can safely reuse their slice.
func TestRankDoesNotMutateInput(t *testing.T) {
	nodes := makeNodes(8, 1)
	before := ids(nodes)
	_ = Rank(99, nodes)
	if after := ids(nodes); !equalStrings(before, after) {
		t.Errorf("Rank mutated input order\nbefore: %v\n after: %v", before, after)
	}
}

// TestRankIsPermutation checks every member appears exactly once.
func TestRankIsPermutation(t *testing.T) {
	nodes := makeNodes(37, 1)
	r := Rank(0xABCDEF, nodes)
	if len(r) != len(nodes) {
		t.Fatalf("len = %d, want %d", len(r), len(nodes))
	}
	seen := map[string]bool{}
	for _, n := range r {
		if seen[n.ID] {
			t.Fatalf("duplicate %q in ranking", n.ID)
		}
		seen[n.ID] = true
	}
}

// TestTieBreakByID: when two nodes hash to the same score they must order by
// ascending ID deterministically. We can't easily force a hash collision, so
// instead we assert the total-order property holds by construction: for equal
// weights, whenever Go would consider scores equal it falls back to ID, and the
// shuffle-invariant test already exercises that path. Here we at least verify
// that identical-ID-prefix nodes keep a stable, ID-sorted relative order across
// shuffles (a proxy for the tie-break comparator being a total order).
func TestTieBreakStableAcrossShuffle(t *testing.T) {
	nodes := makeNodes(20, 1)
	rng := rand.New(rand.NewSource(7))
	want := ids(Rank(555, nodes))
	for i := 0; i < 100; i++ {
		shuf := make([]Node, len(nodes))
		copy(shuf, nodes)
		rng.Shuffle(len(shuf), func(a, b int) { shuf[a], shuf[b] = shuf[b], shuf[a] })
		if got := ids(Rank(555, shuf)); !equalStrings(got, want) {
			t.Fatalf("tie-break not a total order: %v vs %v", got, want)
		}
	}
}

// TestUnweightedBalance: with equal weights, ownership (Top-1) should be spread
// roughly evenly across nodes over many keys.
func TestUnweightedBalance(t *testing.T) {
	const m = 10
	const numKeys = 200000
	nodes := makeNodes(m, 1)
	count := map[string]int{}
	for k := uint64(0); k < numKeys; k++ {
		count[Rank(k, nodes)[0].ID]++
	}
	expected := float64(numKeys) / float64(m)
	for _, n := range nodes {
		got := float64(count[n.ID])
		if rel := math.Abs(got-expected) / expected; rel > 0.10 {
			t.Errorf("owner share for %s = %.0f, expected ~%.0f (rel dev %.3f > 0.10)",
				n.ID, got, expected, rel)
		}
	}
}

// TestWeightedDistribution: ownership share must be proportional to weight.
func TestWeightedDistribution(t *testing.T) {
	nodes := []Node{
		{ID: "w1a", Weight: 1},
		{ID: "w1b", Weight: 1},
		{ID: "w2", Weight: 2},
		{ID: "w4", Weight: 4},
	}
	var sumW float64
	for _, n := range nodes {
		sumW += n.Weight
	}
	const numKeys = 400000
	count := map[string]int{}
	for k := uint64(0); k < numKeys; k++ {
		count[Rank(k, nodes)[0].ID]++
	}
	for _, n := range nodes {
		got := float64(count[n.ID]) / float64(numKeys)
		want := n.Weight / sumW
		if rel := math.Abs(got-want) / want; rel > 0.08 {
			t.Errorf("share for %s (w=%.0f) = %.4f, want ~%.4f (rel dev %.3f > 0.08)",
				n.ID, n.Weight, got, want, rel)
		}
	}
}

// TestMinimalDisruption: HRW's defining property — adding one node reassigns
// only ~1/(m+1) of keys, and never moves a key between two pre-existing nodes.
func TestMinimalDisruption(t *testing.T) {
	const m = 20
	const numKeys = 100000
	before := makeNodes(m, 1)
	after := makeNodes(m+1, 1) // adds node-20
	moved := 0
	for k := uint64(0); k < numKeys; k++ {
		o1 := Rank(k, before)[0].ID
		o2 := Rank(k, after)[0].ID
		if o1 != o2 {
			moved++
			if o2 != "node-20" {
				t.Fatalf("key %d moved %s->%s but not onto the new node", k, o1, o2)
			}
		}
	}
	frac := float64(moved) / float64(numKeys)
	exp := 1.0 / float64(m+1)
	if math.Abs(frac-exp)/exp > 0.15 {
		t.Errorf("moved fraction %.4f, expected ~%.4f", frac, exp)
	}
}

func TestTopAndEdgeCases(t *testing.T) {
	nodes := makeNodes(5, 1)
	if Top(0, nodes, 0) != nil {
		t.Error("Top n=0 should be nil")
	}
	if got := Top(0, nodes, 3); len(got) != 3 {
		t.Errorf("Top n=3 len = %d", len(got))
	}
	if got := Top(0, nodes, 99); len(got) != 5 {
		t.Errorf("Top n>len len = %d, want 5", len(got))
	}
	if got := Rank(0, nil); got != nil && len(got) != 0 {
		t.Errorf("Rank(nil) = %v", got)
	}
	// Zero/negative weight treated as 1: a zero-weight node must still be ranked
	// identically to a unit-weight one for the same ID.
	z := []Node{{ID: "x", Weight: 0}}
	u := []Node{{ID: "x", Weight: 1}}
	if score(7, z[0]) != score(7, u[0]) {
		t.Error("zero weight not treated as 1")
	}
}

// --- helpers ---

func makeNodes(m int, w float64) []Node {
	out := make([]Node, m)
	for i := range out {
		out[i] = Node{ID: nodeID(i), Weight: w}
	}
	return out
}

func nodeID(i int) string {
	const digits = "0123456789"
	return "node-" + string([]byte{digits[i/10], digits[i%10]})
}

func ids(nodes []Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID
	}
	return out
}

func equalStrings(a, b []string) bool {
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
