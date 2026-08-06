package engine

import (
	"context"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/tracker"
)

// readerSetTTL is how long a locally cached reader set is trusted before the
// engine re-JOINs the tracker. It is shorter than the tracker's lease so the
// lease never lapses while this node is still reading.
const readerSetTTL = 2 * time.Second

// readerSet is a cached, frozen reader set for one file.
type readerSet struct {
	nodes    []string
	epoch    uint64
	cachedAt time.Time
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
// callers fall back to routing over all Ready members.
func (e *Engine) readers(ctx context.Context, view *cluster.View, objectID string, fileKey uint64) []string {
	if e.trackerClient == nil && e.trackerReg == nil {
		return nil
	}
	now := time.Now()

	e.rsMu.Lock()
	if rs, hit := e.rs[objectID]; hit && now.Sub(rs.cachedAt) < readerSetTTL {
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
			// Tracker unreachable: fall back to all-member routing rather than
			// failing the read (S is soft state).
			return nil
		}
	}

	e.rsMu.Lock()
	e.rs[objectID] = &readerSet{nodes: resp.Readers, epoch: resp.EpochS, cachedAt: now}
	e.rsMu.Unlock()
	return resp.Readers
}

// treeNodes returns the ordered candidate set the distribution tree is built
// over for a chunk of the given object, plus whether it came from the reader set.
//
// Preferring the reader set means a node's parent is another node that actually
// wants the data (so it will hold it or be fetching it), instead of an arbitrary
// member that must fetch-on-behalf. If the reader set is unavailable or too
// small to form a tree, it falls back to all Ready members.
func (e *Engine) treeNodes(ctx context.Context, view *cluster.View, objectID string, fileKey, chunkKey uint64) ([]hashring.Node, bool) {
	if rs := e.readers(ctx, view, objectID, fileKey); len(rs) > 1 {
		nodes := make([]hashring.Node, 0, len(rs))
		for _, id := range rs {
			if m, ok := view.Get(id); ok && m.Addr != "" {
				nodes = append(nodes, hashring.Node{ID: m.ID, Weight: m.Weight})
			}
		}
		if len(nodes) > 1 {
			return hashring.Rank(chunkKey, nodes), true
		}
	}
	return hashring.Rank(chunkKey, view.Ready()), false
}
