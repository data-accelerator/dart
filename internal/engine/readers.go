package engine

import (
	"context"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/tracker"
)

// readerSetTTL is used when a tracker reports no lease duration.
const readerSetTTL = 2 * time.Second

// Startup refreshes are demand-driven: no timer or background worker is kept
// for an object. The normal ceiling remains half of the granted reader lease.
const initialReaderRefresh = 50 * time.Millisecond

type readerBasis struct {
	addr  string
	epoch uint64
}

type readerFlightKey struct {
	objectID string
	basis    readerBasis
}

type readerFlight struct {
	done   chan struct{}
	leader context.Context
	nodes  []string
}

// Published cache entries are immutable; callers only read their node slices.
type readerSet struct {
	nodes               []string
	epoch               uint64
	cachedAt, expiresAt time.Time
	leaseUntil          time.Time
	interval            time.Duration
	basis               readerBasis
	view                *cluster.View // immutable tracker-selection input
}

func readerRefreshDelay(previous, ceiling time.Duration) time.Duration {
	if previous <= 0 {
		return min(initialReaderRefresh, ceiling)
	}
	if previous >= ceiling/2 {
		return ceiling
	}
	return min(previous*2, ceiling)
}

// trackerAddr returns the address of the tracker responsible for fileKey: the
// HRW top-1 Ready member (every node computes the same one, so no registry is
// needed). ok is false when membership is unusable; self selects the local
// registry when this node is the tracker.
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

// readers returns a cached reader set or joins the tracker. One inline caller
// owns each refresh; waiters can cancel independently, and a canceled leader
// allows a live waiter to retry without letting the old result overwrite it.
func (e *Engine) readers(ctx context.Context, view *cluster.View, objectID string, fileKey uint64) []string {
	if e.trackerClient == nil && e.trackerReg == nil {
		return nil
	}
	if ctx.Err() != nil {
		return nil
	}
	// Reuse tracker selection only for the identical immutable view. Epoch
	// alone excludes address and liveness changes, which can change the tracker.
	now := time.Now()
	e.rsMu.Lock()
	if cached := e.rs[objectID]; cached != nil && cached.view == view && now.Before(cached.expiresAt) {
		nodes := cached.nodes
		e.rsMu.Unlock()
		return nodes
	}
	e.rsMu.Unlock()
	addr, self, ok := e.trackerAddr(view, fileKey)
	if !ok || (self && e.trackerReg == nil) || (!self && e.trackerClient == nil) {
		return nil
	}
	basis := readerBasis{addr: addr, epoch: view.Epoch()}
	key := readerFlightKey{objectID: objectID, basis: basis}
	for {
		if ctx.Err() != nil {
			return nil
		}
		now := time.Now()
		e.rsMu.Lock()
		previous := e.rs[objectID]
		if previous != nil && previous.basis == basis && now.Before(previous.expiresAt) {
			// A newly published view may select the same tracker. Remember it
			// without changing the refresh deadline or publication age.
			if previous.view != view {
				updated := *previous
				updated.view = view
				e.rs[objectID] = &updated
			}
			nodes := previous.nodes
			e.rsMu.Unlock()
			return nodes
		}
		if c := e.rsCalls[key]; c != nil && c.leader.Err() == nil {
			e.rsMu.Unlock()
			select {
			case <-ctx.Done():
				return nil
			case <-c.leader.Done():
				continue
			case <-c.done:
				if ctx.Err() != nil {
					return nil
				}
				if c.leader.Err() != nil {
					continue
				}
				return c.nodes
			}
		}
		if e.rsCalls == nil {
			e.rsCalls = make(map[readerFlightKey]*readerFlight)
		}
		c := &readerFlight{done: make(chan struct{}), leader: ctx}
		e.rsCalls[key] = c
		e.rsMu.Unlock()

		var resp tracker.JoinResponse
		var err error
		if self {
			resp = e.trackerReg.Join(objectID, e.selfID, 0)
		} else {
			resp, err = e.trackerClient.Join(ctx, addr, objectID, e.selfID, 0)
		}
		e.rsMu.Lock()
		if e.rsCalls[key] == c {
			delete(e.rsCalls, key)
			if err == nil && ctx.Err() == nil {
				// Clamp before duration conversion, matching the registry's
				// grant bound. A missing grant retains the historical fallback.
				lease := time.Duration(0)
				if resp.TTLMs > 0 {
					lease = time.Duration(min(resp.TTLMs, tracker.MaxLeaseTTL.Milliseconds())) * time.Millisecond
				}
				ceiling := lease / 2
				if ceiling <= 0 {
					ceiling = readerSetTTL
					lease = 2 * ceiling
				}
				interval := time.Duration(0)
				if previous != nil && previous.basis == basis && now.Before(previous.leaseUntil) {
					interval = previous.interval
				}
				interval = readerRefreshDelay(interval, ceiling)
				if len(e.rs) > 256 {
					for id, cached := range e.rs {
						if !now.Before(cached.leaseUntil) {
							delete(e.rs, id)
						}
					}
				}
				if e.rs == nil {
					e.rs = make(map[string]*readerSet)
				}
				// A response from an older tracker view must not replace a cache
				// installed by a more recently started refresh for this object.
				if current := e.rs[objectID]; current == nil || !current.cachedAt.After(now) {
					e.rs[objectID] = &readerSet{nodes: resp.Readers, epoch: resp.EpochS, cachedAt: now,
						expiresAt: now.Add(interval), leaseUntil: now.Add(lease), interval: interval, basis: basis, view: view}
				}
				e.mx.recordReaderSnapshot(len(resp.Readers))
				c.nodes = resp.Readers
			}
		}
		close(c.done)
		e.rsMu.Unlock()
		return c.nodes
	}
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
