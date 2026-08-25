package hashring

// The HRW ordering produced by Rank is interpreted as the preorder traversal
// of a full k-ary tree used for P2P distribution:
//
//   - position 0 is the root (the chunk owner / stream source);
//   - the remaining positions (0, hi] form the root's child subtrees, split
//     into up to k contiguous, as-balanced-as-possible segments, applied
//     recursively.
//
// Two properties follow and are relied upon by the distribution layer:
//
//  1. Any prefix [0..m] is a connected subtree containing the root — so the
//     set of nodes that have joined so far always forms a valid tree.
//  2. Every node's parent ranks strictly earlier than it, i.e. the parent's
//     HRW score is higher, so the parent entered the stream earlier and is
//     more likely to already hold the data (good for cut-through relay).
//
// Parent and Children are exact mutual inverses because both derive their
// structure from the single childSegments helper.

// childSegments partitions the descendants (lo, hi] of the subtree rooted at
// lo into up to k contiguous child segments, as balanced as possible: the
// first (remaining mod k) segments get one extra element. Empty segments (when
// there are fewer than k descendants) are omitted. Each returned segment's
// first index is the position of a direct child of lo.
func childSegments(lo, hi, k int) [][2]int {
	childLo := lo + 1
	if k < 1 || childLo > hi {
		return nil
	}
	remaining := hi - childLo + 1
	base := remaining / k
	extra := remaining % k
	segs := make([][2]int, 0, k)
	s := childLo
	for c := 0; c < k && s <= hi; c++ {
		size := base
		if c < extra {
			size++
		}
		if size == 0 {
			continue
		}
		e := s + size - 1
		segs = append(segs, [2]int{s, e})
		s = e + 1
	}
	return segs
}

// childSegment locates, within the children of [lo,hi], the segment containing
// position i — the same partition childSegments materializes, computed
// arithmetically with no allocation.
func childSegment(lo, hi, k, i int) (int, int, bool) {
	childLo := lo + 1
	if k < 1 || childLo > hi || i < childLo || i > hi {
		return -1, -1, false
	}
	remaining := hi - childLo + 1
	base := remaining / k
	extra := remaining % k
	// Segments 0..extra-1 have size base+1, the rest base; segment c starts at
	// childLo + c*base + min(c, extra).
	boundary := childLo + extra*(base+1)
	if i < boundary {
		c := (i - childLo) / (base + 1)
		s := childLo + c*(base+1)
		return s, s + base, true // size base+1
	}
	if base == 0 {
		return -1, -1, false // unreachable: boundary == hi+1 when base == 0
	}
	c := extra + (i-boundary)/base
	s := boundary + (c-extra)*base
	return s, s + base - 1, true // size base
}

// descend walks from the whole range [0, n-1] down to the subtree whose root
// is position i. It calls visit(root) at every ancestor root encountered,
// including i's own position last, and returns i's subtree range [lo, hi]. For
// an out-of-range i (or k < 1) it returns (-1, -1) and does not call visit.
// Zero-allocation: each level locates the containing child segment
// arithmetically (childSegment).
func descend(i, n, k int, visit func(root int)) (int, int) {
	if i < 0 || i >= n || k < 1 {
		return -1, -1
	}
	lo, hi := 0, n-1
	for {
		if visit != nil {
			visit(lo)
		}
		if i == lo {
			return lo, hi
		}
		nlo, nhi, ok := childSegment(lo, hi, k, i)
		if !ok {
			// Unreachable for a valid i in [0,n): every descendant lands in
			// exactly one child segment.
			return -1, -1
		}
		lo, hi = nlo, nhi
	}
}

// Parent returns the preorder-tree parent of position i over n positions with
// fanout k. The root (i == 0) returns -1; out-of-range i (or k < 1) returns -1.
// Runs in O(depth * k) with no allocation beyond childSegments.
func Parent(i, n, k int) int {
	if i <= 0 || i >= n || k < 1 {
		return -1
	}
	prev := -1
	descend(i, n, k, func(root int) {
		if root != i {
			prev = root
		}
	})
	return prev
}

// Children returns the positions of i's direct children in the preorder tree,
// in ascending (preorder) order. A leaf returns nil. Out-of-range i (or k < 1)
// returns nil.
func Children(i, n, k int) []int {
	lo, hi := descend(i, n, k, nil)
	if lo < 0 {
		return nil
	}
	segs := childSegments(lo, hi, k)
	if len(segs) == 0 {
		return nil
	}
	out := make([]int, len(segs))
	for j, s := range segs {
		out[j] = s[0]
	}
	return out
}

// Depth returns the number of edges from the root to position i (root = 0).
// Out-of-range i (or k < 1) returns -1.
func Depth(i, n, k int) int {
	if i < 0 || i >= n || k < 1 {
		return -1
	}
	d := -1
	descend(i, n, k, func(int) { d++ })
	return d
}
