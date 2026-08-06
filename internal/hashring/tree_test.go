package hashring

import (
	"testing"
)

// TestTreeGolden pins the exact shape of a small preorder binary tree that is
// easy to verify by hand. Positions 0..6, fanout 2:
//
//	     0
//	   /   \
//	  1     4
//	 / \   / \
//	2   3 5   6
//
// (root 0 splits {1..6} into [1,3] and [4,6]; each splits its two descendants.)
func TestTreeGolden(t *testing.T) {
	const n, k = 7, 2
	wantParent := []int{-1, 0, 1, 1, 0, 4, 4}
	for i := 0; i < n; i++ {
		if got := Parent(i, n, k); got != wantParent[i] {
			t.Errorf("Parent(%d) = %d, want %d", i, got, wantParent[i])
		}
	}
	wantChildren := map[int][]int{
		0: {1, 4}, 1: {2, 3}, 2: nil, 3: nil, 4: {5, 6}, 5: nil, 6: nil,
	}
	for i := 0; i < n; i++ {
		if got := Children(i, n, k); !equalInts(got, wantChildren[i]) {
			t.Errorf("Children(%d) = %v, want %v", i, got, wantChildren[i])
		}
	}
	wantDepth := []int{0, 1, 2, 2, 1, 2, 2}
	for i := 0; i < n; i++ {
		if got := Depth(i, n, k); got != wantDepth[i] {
			t.Errorf("Depth(%d) = %d, want %d", i, got, wantDepth[i])
		}
	}
}

// TestChainWhenK1: fanout 1 degenerates to a linked list, Parent(i) = i-1.
func TestChainWhenK1(t *testing.T) {
	const n = 8
	for i := 0; i < n; i++ {
		want := i - 1 // -1 for root
		if got := Parent(i, n, 1); got != want {
			t.Errorf("k=1 Parent(%d) = %d, want %d", i, got, want)
		}
		if i < n-1 {
			if got := Children(i, n, 1); !equalInts(got, []int{i + 1}) {
				t.Errorf("k=1 Children(%d) = %v, want [%d]", i, got, i+1)
			}
		} else if got := Children(i, n, 1); got != nil {
			t.Errorf("k=1 Children(last) = %v, want nil", got)
		}
	}
}

// TestParentChildrenInverse: Parent and Children must be exact inverses over a
// broad matrix of sizes and fanouts.
func TestParentChildrenInverse(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 100, 257, 1000} {
		for _, k := range []int{1, 2, 3, 4, 8, 16} {
			for i := 0; i < n; i++ {
				// Every child points back to i.
				for _, c := range Children(i, n, k) {
					if c <= i {
						t.Fatalf("n=%d k=%d: child %d of %d does not rank later", n, k, c, i)
					}
					if p := Parent(c, n, k); p != i {
						t.Fatalf("n=%d k=%d: Parent(%d)=%d, want %d", n, k, c, p, i)
					}
				}
				// i is listed among its parent's children.
				if i != 0 {
					p := Parent(i, n, k)
					if p < 0 || p >= i {
						t.Fatalf("n=%d k=%d: Parent(%d)=%d must be in [0,%d)", n, k, i, p, i)
					}
					if !contains(Children(p, n, k), i) {
						t.Fatalf("n=%d k=%d: %d not in Children(%d)", n, k, i, p)
					}
				}
			}
		}
	}
}

// TestFanoutDegree: no node has more than k children.
func TestFanoutDegree(t *testing.T) {
	for _, n := range []int{1, 5, 16, 100, 1000} {
		for _, k := range []int{1, 2, 3, 4, 8} {
			for i := 0; i < n; i++ {
				if d := len(Children(i, n, k)); d > k {
					t.Fatalf("n=%d k=%d: node %d has %d children > k", n, k, i, d)
				}
			}
		}
	}
}

// TestNoCyclesReachRoot: following Parent from any node reaches the root (0) in
// at most n steps — proves acyclicity and full connectivity.
func TestNoCyclesReachRoot(t *testing.T) {
	for _, n := range []int{1, 2, 7, 16, 100, 999} {
		for _, k := range []int{1, 2, 3, 8} {
			for i := 0; i < n; i++ {
				cur, steps := i, 0
				for cur != 0 {
					cur = Parent(cur, n, k)
					if cur < 0 {
						t.Fatalf("n=%d k=%d: Parent chain from %d hit -1 before root", n, k, i)
					}
					if steps++; steps > n {
						t.Fatalf("n=%d k=%d: cycle in Parent chain from %d", n, k, i)
					}
				}
			}
		}
	}
}

// TestPartitionExactlyOnce: every non-root position is a child of exactly one
// node — Children over all nodes forms a partition of [1,n).
func TestPartitionExactlyOnce(t *testing.T) {
	for _, n := range []int{1, 4, 7, 16, 100, 513} {
		for _, k := range []int{1, 2, 3, 5, 8} {
			seen := make([]int, n)
			for i := 0; i < n; i++ {
				for _, c := range Children(i, n, k) {
					seen[c]++
				}
			}
			if seen[0] != 0 {
				t.Fatalf("n=%d k=%d: root listed as a child", n, k)
			}
			for c := 1; c < n; c++ {
				if seen[c] != 1 {
					t.Fatalf("n=%d k=%d: position %d appears as child %d times, want 1", n, k, c, seen[c])
				}
			}
		}
	}
}

// TestPrefixConnectivity: for every m, the prefix {0..m} is closed under Parent
// (each element's parent is also in the prefix) — i.e. the joined-so-far set is
// always a connected subtree containing the root.
func TestPrefixConnectivity(t *testing.T) {
	for _, n := range []int{1, 7, 16, 100, 400} {
		for _, k := range []int{1, 2, 4, 8} {
			for m := 0; m < n; m++ {
				for j := 1; j <= m; j++ {
					if p := Parent(j, n, k); p > m {
						t.Fatalf("n=%d k=%d: prefix [0..%d] not connected: Parent(%d)=%d outside", n, k, m, j, p)
					}
				}
			}
		}
	}
}

func TestTreeEdgeCases(t *testing.T) {
	if Parent(0, 1, 2) != -1 {
		t.Error("Parent(root) must be -1")
	}
	if Parent(-1, 5, 2) != -1 || Parent(5, 5, 2) != -1 {
		t.Error("out-of-range Parent must be -1")
	}
	if Parent(1, 5, 0) != -1 {
		t.Error("k<1 Parent must be -1")
	}
	if Children(0, 1, 2) != nil {
		t.Error("single-node tree root has no children")
	}
	if Children(3, 3, 2) != nil {
		t.Error("out-of-range Children must be nil")
	}
	if Depth(-1, 5, 2) != -1 {
		t.Error("out-of-range Depth must be -1")
	}
}

// --- helpers ---

func equalInts(a, b []int) bool {
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

func contains(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
