package hashring

// Regression tests for issue #14 H4: Parent/Depth must not allocate — descend
// locates the containing child segment arithmetically per level (the audit
// measured 512 B / 8 allocs per call at depth 8).

import "testing"

func TestParentZeroAllocs(t *testing.T) {
	const n, k = 100000, 4
	allocs := testing.AllocsPerRun(1000, func() {
		if p := Parent(99999, n, k); p < 0 {
			t.Fatal("parent of a leaf must exist")
		}
	})
	if allocs != 0 {
		t.Fatalf("Parent allocates %.1f per call, want 0", allocs)
	}
}

func TestDepthZeroAllocs(t *testing.T) {
	const n, k = 100000, 4
	allocs := testing.AllocsPerRun(1000, func() {
		if d := Depth(99999, n, k); d < 0 {
			t.Fatal("depth of a leaf must exist")
		}
	})
	if allocs != 0 {
		t.Fatalf("Depth allocates %.1f per call, want 0", allocs)
	}
}

// TestChildSegmentMatchesChildSegments cross-checks the arithmetic locator
// against the materializing childSegments over an exhaustive grid — the two
// must agree on every level of every tree shape.
func TestChildSegmentMatchesChildSegments(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 7, 10, 33, 100, 1000} {
		for _, k := range []int{1, 2, 3, 4, 5, 8, 64} {
			for i := 0; i < n; i++ {
				lo, hi := 0, n-1
				for {
					if i == lo {
						break
					}
					gots, gote, ok := childSegment(lo, hi, k, i)
					if !ok {
						t.Fatalf("childSegment(%d,%d,%d,%d): not found", lo, hi, k, i)
					}
					found := false
					for _, s := range childSegments(lo, hi, k) {
						if i >= s[0] && i <= s[1] {
							found = true
							if s[0] != gots || s[1] != gote {
								t.Fatalf("childSegment=(%d,%d), childSegments=(%d,%d) for lo=%d hi=%d k=%d i=%d",
									gots, gote, s[0], s[1], lo, hi, k, i)
							}
							break
						}
					}
					if !found {
						t.Fatalf("childSegments found no segment for lo=%d hi=%d k=%d i=%d", lo, hi, k, i)
					}
					lo, hi = gots, gote
				}
			}
		}
	}
}
