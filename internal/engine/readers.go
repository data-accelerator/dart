package engine

import (
	"context"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/tracker"
)

// readerSetTTL is the fallback cache period for a locally cached reader set,
// used only when the tracker granted no lease duration. Normally the cache
// period is half the granted lease (see readers), so renewal always lands
// before the lease lapses.
const readerSetTTL = 2 * time.Second

// readerSet is a cached, frozen reader set for one file.
type readerSet struct {
	nodes     []string
	epoch     uint64
	cachedAt  time.Time
	expiresAt time.Time
}

// trackerAddr returns the address of the tracker responsible for fileKey: the
// HRW top-1 Ready member (every node computes the same one, so no registry is
// needed). ok is false when membership is unusable or this node is the tracker
// (in which case the caller uses the local registry directly).
func (e *Engine) trackerAddr(view *cluster.View, fileKey uint64) (addr string, self bool, ok bool) {
	ranked := hashring.Rank(fileKey, view.Ready())
	if len(ranked) == 0 {
		return "", false, false
	}
	if ranked[0].ID == e.selfID {
		return "", true, true
	}
	m, found := view.Get(ranked[0].ID)
	if !found || m.Addr == "" {
		return "", false, false
	}
	return m.Addr, false, true
}

// readers returns the active reader set for the object, JOINing the tracker (or
// the local registry when this node is the tracker) and caching the result for
// readerSetTTL. It returns nil when no reader set is available, in which case
// callers with tracking enabled route directly to the preferred owner.
func (e *Engine) readers(ctx context.Context, view *cluster.View, objectID string, fileKey uint64) []string {
	if e.trackerClient == nil && e.trackerReg == nil {
		return nil
	}
	now := time.Now()

	e.rsMu.Lock()
	if rs, hit := e.rs[objectID]; hit && now.Before(rs.expiresAt) {
		nodes := rs.nodes
		e.rsMu.Unlock()
		return nodes
	}
	e.rsMu.Unlock()

	addr, self, ok := e.trackerAddr(view, fileKey)
	if !ok {
		return nil
	}

	var resp tracker.JoinResponse
	if self {
		if e.trackerReg == nil {
			return nil
		}
		resp = e.trackerReg.Join(objectID, e.selfID, 0)
	} else {
		if e.trackerClient == nil {
			return nil
		}
		var err error
		resp, err = e.trackerClient.Join(ctx, addr, objectID, e.selfID, 0)
		if err != nil {
			// Tracker unreachable: route directly to the owner rather than
			// failing the read (S is soft state).
			return nil
		}
	}

	// Cache for half the granted lease, so the next re-Join lands well before
	// our tracker lease lapses: a renewal period longer than the lease would
	// silently drop this node from the frozen reader set while it is still
	// reading. (readerSetTTL is only the fallback for a tracker that reported
	// no grant.)
	ttl := time.Duration(resp.TTLMs) * time.Millisecond / 2
	if ttl <= 0 {
		ttl = readerSetTTL
	}
	e.rsMu.Lock()
	if len(e.rs) > 256 {
		// Bound the cache: drop expired entries before growing past a few
		// hundred objects. Live entries stay; the map tracks objects currently
		// being read, which is naturally small.
		for id, rs := range e.rs {
			if now.After(rs.expiresAt) {
				delete(e.rs, id)
			}
		}
	}
	e.rs[objectID] = &readerSet{nodes: resp.Readers, epoch: resp.EpochS, cachedAt: now, expiresAt: now.Add(ttl)}
	e.rsMu.Unlock()
	return resp.Readers
}

// treeNodes returns the ordered candidate set the distribution tree is built
// over for a chunk of the given object, plus whether it came from the reader set.
//
// Preferring the reader set means a node's parent is another node that actually
// wants the data (so it will hold it or be fetching it), instead of an arbitrary
// member that must fetch-on-behalf. The owner always remains first. A missing
// tracked snapshot routes directly to it; tracking disabled uses all eligible
// Ready readers as descendants. Root-only peers never become descendants.
// Eligible self is always included; published snapshots are never mutated.
func (e *Engine) treeNodes(ctx context.Context, view *cluster.View, objectID string, fileKey, chunkKey uint64) ([]hashring.Node, bool) {
	roots := hashring.Rank(chunkKey, view.Roots())
	if len(roots) == 0 {
		return nil, false
	}
	root := roots[0]
	// Supply peers never JOIN as readers merely to answer a peer request.
	if !e.role.CanRead() {
		return []hashring.Node{root}, false
	}
	var nodes []hashring.Node
	fromReaders := e.trackerClient != nil || e.trackerReg != nil
	if fromReaders {
		for _, id := range e.readers(ctx, view, objectID, fileKey) {
			if m, ok := view.Get(id); ok && m.State == cluster.Ready && m.Role.CanRead() && m.Addr != "" {
				nodes = append(nodes, hashring.Node{ID: m.ID, Weight: m.Weight})
			}
		}
	} else {
		nodes = view.Readers()
	}
	// A tracker snapshot may predate our JOIN. Complete and deduplicate a local
	// copy, never the shared tracker/view slice. HRW preserves common-member
	// relative order, so every ordinary ancestor remains strictly earlier.
	unique := make([]hashring.Node, 0, len(nodes)+1)
	present := make(map[string]bool, len(nodes)+1)
	for _, n := range nodes {
		if !present[n.ID] {
			unique = append(unique, n)
			present[n.ID] = true
		}
	}
	if m, ok := view.Get(e.selfID); ok && m.State == cluster.Ready && m.Role.CanRead() && m.Addr != "" && !present[m.ID] {
		unique = append(unique, hashring.Node{ID: m.ID, Weight: m.Weight})
	}
	ordered := hashring.Rank(chunkKey, unique)
	out := make([]hashring.Node, 0, len(ordered)+1)
	out = append(out, root)
	seen := map[string]bool{root.ID: true}
	for _, n := range ordered {
		if !seen[n.ID] {
			out = append(out, n)
			seen[n.ID] = true
		}
	}
	return out, fromReaders
}
