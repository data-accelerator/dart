package hashring

// Issue #14: weighted-rank golden (H3) and the Python-reference cross-check.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRankWeightedGolden pins the weighted HRW ordering (the w / -ln(u) path)
// against the independent Python reference (contrib/golden/golden_reference.py).
// TestRankGolden covers equal weights only, where the order reduces to integer
// hash order; these cases genuinely exercise the float score. Only the ORDER
// is pinned, never raw floats: Python's math.log (C libm) and Go's math.Log
// (assembly on amd64/s390x) are bit-consistent over the score domain today,
// and a future 1-ulp toolchain drift would flip an order only at an exact tie.
func TestRankWeightedGolden(t *testing.T) {
	nodes := []Node{
		{ID: "wn-1", Weight: 1},
		{ID: "wn-2", Weight: 1},
		{ID: "wn-3", Weight: 2},
		{ID: "wn-4", Weight: 4},
	}
	cases := []struct {
		key  uint64
		want []string
	}{
		{0xDEADBEEFCAFEBABE, []string{"wn-4", "wn-3", "wn-2", "wn-1"}},
		{42, []string{"wn-1", "wn-3", "wn-4", "wn-2"}},
		{7, []string{"wn-4", "wn-2", "wn-1", "wn-3"}},
		{1024, []string{"wn-2", "wn-3", "wn-4", "wn-1"}},
	}
	for _, c := range cases {
		if got := ids(Rank(c.key, nodes)); !equalStrings(got, c.want) {
			t.Errorf("Rank(%#x, weighted) = %v, want %v (python reference)", c.key, got, c.want)
		}
	}
}

// TestHash64AgainstPythonReference re-runs the independent Python reference
// and diffs its hash64 and rank records against the live Go implementation —
// the "independent implementation" claim becomes executable. Runs only when
// DART_GOLDEN_REF=1 is set (CI); skipped otherwise so contributors need no
// Python.
func TestHash64AgainstPythonReference(t *testing.T) {
	nHash, nRank := 0, 0
	for _, rec := range goldenRefRecords(t) {
		f := strings.Split(rec, "|")
		switch f[0] {
		case "hash64":
			if len(f) != 4 {
				t.Fatalf("malformed hash64 record %q", rec)
			}
			nHash++
			key, _ := strconv.ParseUint(f[1], 10, 64)
			want, _ := strconv.ParseUint(f[3], 10, 64)
			if got := Hash64(key, f[2]); got != want {
				t.Errorf("Hash64(%s, %q) = %d, python reference says %d", f[1], f[2], got, want)
			}
		case "rank":
			if len(f) != 4 {
				t.Fatalf("malformed rank record %q", rec)
			}
			nRank++
			key, _ := strconv.ParseUint(f[1], 16, 64)
			var nodes []Node
			for _, m := range strings.Split(f[2], ",") {
				id, ws, ok := strings.Cut(m, "=")
				if !ok {
					t.Fatalf("malformed member %q in %q", m, rec)
				}
				w, err := strconv.ParseFloat(ws, 64)
				if err != nil {
					t.Fatalf("malformed weight in %q: %v", m, err)
				}
				nodes = append(nodes, Node{ID: id, Weight: w})
			}
			want := strings.Split(f[3], ",")
			if got := ids(Rank(key, nodes)); !equalStrings(got, want) {
				t.Errorf("Rank(%s, %s) = %v, python reference says %v", f[1], f[2], got, want)
			}
		case "chunkkey", "epoch":
			// owned by the chunk/cluster cross-checks
		default:
			t.Fatalf("unknown record kind in %q", rec)
		}
	}
	// Guard against the reference silently shrinking: the tables below must
	// not drift apart from what the script emits.
	if nHash != 5 || nRank != 5 {
		t.Fatalf("reference emitted %d hash64 and %d rank records, want 5 and 5", nHash, nRank)
	}
}

// goldenRefRecords runs contrib/golden/golden_reference.py and returns its
// records. Skips the test when DART_GOLDEN_REF is unset.
func goldenRefRecords(t *testing.T) []string {
	t.Helper()
	if os.Getenv("DART_GOLDEN_REF") != "1" {
		t.Skip("set DART_GOLDEN_REF=1 to diff against the Python reference (CI)")
	}
	script := filepath.Join("..", "..", "contrib", "golden", "golden_reference.py")
	out, err := exec.Command("python3", script).Output()
	if err != nil {
		t.Fatalf("python3 %s: %v", script, err)
	}
	var recs []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		recs = append(recs, strings.TrimSpace(line))
	}
	return recs
}
