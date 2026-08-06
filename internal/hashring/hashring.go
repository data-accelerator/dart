// Package hashring implements DART's two deterministic, coordination-free
// primitives:
//
//   - weighted Rendezvous Hashing (HRW) for chunk placement, and
//   - a preorder full k-ary tree derived from the HRW ordering, used for the
//     P2P distribution topology.
//
// Determinism is a load-bearing invariant of the whole system: because there
// is no coordinator, every node must independently compute a byte-identical
// ordering from identical (key, member-set, weights) inputs — regardless of
// the order members arrived from the Kubernetes watch, the CPU architecture,
// or the Go build. A silent divergence would not produce an error; it would
// only cause a wrong owner / wrong tree parent, i.e. a silent cache miss or an
// extra hop. The exported functions here therefore go out of their way to be
// reproducible, and the accompanying tests pin that reproducibility (see the
// cross-language golden values in hashring_test.go and the shuffle-invariance
// tests).
package hashring

import (
	"encoding/binary"
	"math"
	"sort"
)

// Node is a cluster member participating in placement / distribution.
type Node struct {
	// ID is the stable node identity (e.g. Kubernetes nodeName or a persistent
	// UUID stored under the cache dir). It MUST be identical across all nodes
	// and stable across restarts; never derive it from an ephemeral value such
	// as PodIP, or a restart would reshard the whole ring.
	ID string

	// Weight reflects relative cache capacity (heterogeneous machines). Values
	// <= 0 are treated as 1. Equal weights reduce to standard (unweighted) HRW.
	Weight float64
}

// two64 is 2^64, exactly representable in float64.
const two64 = 18446744073709551616.0

// Hash64 is a deterministic 64-bit hash of (key, nodeID).
//
// It is intentionally dependency-free and fully specified so results are
// identical across platforms and Go versions:
//
//   - FNV-1a (64-bit) over the little-endian 8 bytes of key followed by the
//     raw bytes of nodeID, then
//   - a splitmix64/murmur3-style fmix64 finalizer for good avalanche.
//
// The exact construction is pinned by cross-language golden tests; changing it
// reshuffles the entire ring and MUST be treated as a breaking, epoch-bumping
// change.
func Hash64(key uint64, nodeID string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	var kb [8]byte
	binary.LittleEndian.PutUint64(kb[:], key)
	for _, b := range kb {
		h ^= uint64(b)
		h *= prime64
	}
	for i := 0; i < len(nodeID); i++ {
		h ^= uint64(nodeID[i])
		h *= prime64
	}
	return fmix64(h)
}

func fmix64(z uint64) uint64 {
	z ^= z >> 33
	z *= 0xff51afd7ed558ccd
	z ^= z >> 33
	z *= 0xc4ceb9fe1a85ec53
	z ^= z >> 33
	return z
}

// score returns the weighted Rendezvous Hashing score of node for key; higher
// wins. It uses the standard weighted-HRW formula score = weight / -ln(u),
// where u is a hash-derived value kept strictly inside the open interval (0,1)
// to avoid the ln(1)=0 and ln(0)=-inf edge cases. Larger hash -> u closer to 1
// -> -ln(u) closer to 0 -> larger score, so weight scales ownership share.
func score(key uint64, n Node) float64 {
	w := n.Weight
	if w <= 0 {
		w = 1
	}
	// (h + 0.5) / 2^64 is in (0,1) for every uint64 h.
	u := (float64(Hash64(key, n.ID)) + 0.5) / two64
	return w / -math.Log(u)
}

// Rank returns a new slice of nodes ordered by descending HRW score for key.
//
// Ties (equal score) are broken by ascending ID. Since IDs are unique this is
// a strict total order, which makes the result independent of the input order
// — the property the whole coordination-free design relies on. The input slice
// is never modified.
func Rank(key uint64, nodes []Node) []Node {
	n := len(nodes)
	idx := make([]int, n)
	scores := make([]float64, n)
	for i := range nodes {
		idx[i] = i
		scores[i] = score(key, nodes[i])
	}
	sort.Slice(idx, func(a, b int) bool {
		ia, ib := idx[a], idx[b]
		if scores[ia] != scores[ib] {
			return scores[ia] > scores[ib]
		}
		return nodes[ia].ID < nodes[ib].ID
	})
	out := make([]Node, n)
	for i, j := range idx {
		out[i] = nodes[j]
	}
	return out
}

// Top returns the highest-ranked n nodes for key (owner plus replica
// candidates). If n exceeds the member count, all nodes are returned. n <= 0
// returns nil.
func Top(key uint64, nodes []Node, n int) []Node {
	if n <= 0 {
		return nil
	}
	r := Rank(key, nodes)
	if n < len(r) {
		r = r[:n]
	}
	return r
}
