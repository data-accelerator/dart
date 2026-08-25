// Package cluster models DART's membership: the set of peer nodes, their
// lifecycle state and capacity weight, and a deterministic epoch derived from
// the membership content.
//
// It feeds internal/hashring: the placement layer ranks the Ready members, the
// distribution layer ranks a reader-set subset. A View is an immutable snapshot
// so it can be shared across goroutines without locking; providers publish new
// Views atomically (see provider.go).
//
// The epoch is a deterministic 64-bit hash of the canonical membership. Two
// nodes that observe the same membership compute the same epoch; this is what
// the coordination-free convergence protocol needs: on an epoch mismatch a node
// refreshes from the source of truth and the views converge. The epoch is
// therefore an agreement/change token, not a monotonic counter — ordering, when
// needed, comes from the provider's underlying resource version, not from the
// epoch.
package cluster

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/data-accelerator/dart/internal/hashring"
)

// State is a member's lifecycle state.
//
//	Joining -> Ready -> Suspect -> Leaving
//
// Only Ready members participate in ownership (placement). Read attempts may
// additionally include Suspect members (their data may still be present, so we
// do not reshard immediately on a brief NotReady).
type State uint8

const (
	// Joining: known but not yet serving; excluded from placement and reads.
	Joining State = iota
	// Ready: fully serving; the only state used for ownership/placement.
	Ready
	// Suspect: transiently unreachable; kept for read attempts until timeout.
	Suspect
	// Leaving: draining / shutting down; excluded from placement.
	Leaving
)

// stateRank orders lifecycle states by how authoritative the "can serve" claim
// is, for duplicate-ID dedup in NewView: Ready > Suspect > Joining > Leaving.
func stateRank(s State) int {
	switch s {
	case Ready:
		return 3
	case Suspect:
		return 2
	case Joining:
		return 1
	default: // Leaving and any unknown state
		return 0
	}
}

// String returns the canonical name of the state.
func (s State) String() string {
	switch s {
	case Joining:
		return "Joining"
	case Ready:
		return "Ready"
	case Suspect:
		return "Suspect"
	case Leaving:
		return "Leaving"
	default:
		return "Unknown"
	}
}

// Member is a single cluster member.
type Member struct {
	// ID is the stable node identity (e.g. Kubernetes nodeName or a persistent
	// UUID). It must be identical across the cluster and stable across
	// restarts; never derive it from an ephemeral value such as PodIP.
	ID string
	// Addr is the peer-transport address (host:port) used to reach this member
	// for block pulls. It is auxiliary routing metadata and is deliberately NOT
	// part of the epoch: it does not affect placement ordering, and a stale
	// Addr only causes a fallback to origin (safe under read-only semantics).
	Addr string
	// Weight is the relative cache capacity used by weighted HRW. Values <= 0
	// are treated as 1 by the hashring; they are preserved verbatim here (and
	// thus contribute to the epoch as-is).
	Weight float64
	// State is the member's lifecycle state.
	//
	// State is deliberately NOT part of the epoch. Liveness is *locally derived*:
	// a node marks a peer Suspect because it cannot reach it, and two nodes can
	// legitimately disagree. If State fed the epoch, every node would compute a
	// different epoch and the epoch would stop being what it is for — a token for
	// "are we looking at the same membership?" — leaving no way to detect a genuinely
	// stale view. Liveness is layered on top of the view, not part of it.
	State State
}

// View is an immutable snapshot of cluster membership at a given epoch.
//
// A View is safe for concurrent use by multiple goroutines. All accessors
// either return copies or return internal slices that MUST be treated as
// read-only (documented per method). Construct one with NewView.
type View struct {
	epoch   uint64
	members []Member        // canonical: sorted by (ID,State,Weight), unique ID
	ready   []hashring.Node // precomputed Ready members (sorted by ID)
	live    []hashring.Node // precomputed Ready+Suspect members (sorted by ID)
}

// NewView builds an immutable View from the given members. The input is copied
// and canonicalized: sorted by (ID, state rank desc, Weight) and de-duplicated
// by ID (keeping the first entry in that deterministic order), so the resulting
// View — and its epoch — are independent of the input order. The input slice is
// not modified.
//
// Duplicate IDs are a conflicting-reports case: the rank order Ready > Suspect
// > Joining > Leaving keeps the entry claiming the node can serve. A Joining or
// Leaving marker must never suppress a member another source still reports as
// Ready — placement would silently lose a serving node.
func NewView(members []Member) *View {
	cp := make([]Member, len(members))
	copy(cp, members)
	sort.Slice(cp, func(a, b int) bool {
		if cp[a].ID != cp[b].ID {
			return cp[a].ID < cp[b].ID
		}
		if ra, rb := stateRank(cp[a].State), stateRank(cp[b].State); ra != rb {
			return ra > rb
		}
		return cp[a].Weight < cp[b].Weight
	})
	// De-duplicate by ID, keeping the first (deterministic after the sort).
	canon := cp[:0]
	for i, m := range cp {
		if i > 0 && m.ID == cp[i-1].ID {
			continue
		}
		canon = append(canon, m)
	}

	v := &View{
		epoch:   computeEpoch(canon),
		members: canon,
	}
	for _, m := range canon {
		switch m.State {
		case Ready:
			v.ready = append(v.ready, hashring.Node{ID: m.ID, Weight: m.Weight})
			v.live = append(v.live, hashring.Node{ID: m.ID, Weight: m.Weight})
		case Suspect:
			v.live = append(v.live, hashring.Node{ID: m.ID, Weight: m.Weight})
		}
	}
	return v
}

// computeEpoch is a deterministic 64-bit hash of the *authoritative* part of the
// canonical membership: FNV-1a over each member's (ID, 0x1F, big-endian float64
// bits of weight, 0x1E), then an fmix64 finalizer. members MUST already be
// canonical.
//
// Only ID and Weight participate. Addr is routing metadata, and State is locally
// derived (see Member.State) — hashing either would make two nodes with the same
// membership disagree on the epoch, which is precisely what the epoch exists to
// rule out.
//
// The serialization is fully specified so that an independent implementation
// can reproduce it; it is pinned by a cross-language golden test. Changing it
// changes every epoch value and must be treated as a protocol change.
func computeEpoch(members []Member) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	mix := func(b byte) { h ^= uint64(b); h *= prime64 }
	var num [8]byte
	for _, m := range members {
		for i := 0; i < len(m.ID); i++ {
			mix(m.ID[i])
		}
		mix(0x1F)
		binary.BigEndian.PutUint64(num[:], math.Float64bits(m.Weight))
		for _, b := range num {
			mix(b)
		}
		mix(0x1E)
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

// Epoch returns the deterministic membership epoch.
func (v *View) Epoch() uint64 { return v.epoch }

// Len returns the number of members (all states).
func (v *View) Len() int { return len(v.members) }

// Members returns a copy of all members (any state), in canonical order. The
// caller owns the returned slice.
func (v *View) Members() []Member {
	out := make([]Member, len(v.members))
	copy(out, v.members)
	return out
}

// Get returns the member with the given ID, if present. O(log n).
func (v *View) Get(id string) (Member, bool) {
	i := sort.Search(len(v.members), func(i int) bool { return v.members[i].ID >= id })
	if i < len(v.members) && v.members[i].ID == id {
		return v.members[i], true
	}
	return Member{}, false
}

// Ready returns the Ready members as hashring nodes (sorted by ID), for
// placement (weighted HRW ownership). The returned slice is shared and
// immutable — do NOT mutate it. hashring.Rank copies its input, so it is safe
// to pass directly.
func (v *View) Ready() []hashring.Node { return v.ready }

// Live returns the Ready and Suspect members as hashring nodes (sorted by ID),
// for read attempts that may still reach a briefly-unreachable holder. The
// returned slice is shared and immutable — do NOT mutate it.
func (v *View) Live() []hashring.Node { return v.live }
