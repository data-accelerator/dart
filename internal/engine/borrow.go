package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// borrowBlock is the serving path. The compatibility block/PeerSource APIs
// retain caller-owned copies; node HTTP handlers hold immutable leases instead.
func (e *Engine) borrowBlock(ctx context.Context, url, objectID string, size, index int64, hop int) (store.Lease, error) {
	start := time.Now()
	r, err := e.readBorrowedBlock(ctx, url, objectID, size, index, hop)
	if err != nil {
		return store.Lease{}, err
	}
	e.mx.recordBlockRead(blockRead{source: r.source}, time.Since(start))
	return r.lease, nil
}

func (e *Engine) readBorrowedBlock(ctx context.Context, url, objectID string, size, index int64, hop int) (leasedRead, error) {
	for {
		r, err := e.loadBorrowedBlock(ctx, url, objectID, size, index, hop)
		if !errors.Is(err, errSpeculativeReceive) || isSpeculative(ctx) {
			return r, err
		}
		// Both borrowed and owned-copy demand can join speculative work that
		// skips admission. Retry with demand headroom; another speculative
		// leader winning admission in between must not turn a skip into failure.
	}
}

func (e *Engine) loadBorrowedBlock(ctx context.Context, url, objectID string, size, index int64, hop int) (leasedRead, error) {
	if index < 0 || index > e.cfg.MaxBlockIndex() || size <= 0 || e.cfg.BlockStart(index) >= size {
		return leasedRead{}, ErrRangeNotSatisfiable
	}

	key := store.BlockKey{Chunk: chunk.ChunkKey(e.ns, objectID, e.cfg.ChunkOfBlock(index)), Block: uint64(index)}
	if l, ok, err := store.Acquire(e.store, key); err != nil {
		return leasedRead{}, err
	} else if ok {
		return leasedRead{lease: l, source: blockFromCache}, nil
	}
	ctx, flightKey := e.pinRouting(ctx, key)
	r, err := e.borrowed.do(ctx, flightKey, url, false, func(ctx context.Context) (leasedRead, error) {
		if l, ok, err := store.Acquire(e.store, key); err != nil {
			return leasedRead{}, err
		} else if ok {
			return leasedRead{lease: l, source: blockFromCache}, nil
		}
		want := e.blockLen(size, index)
		if isSupply(ctx) {
			if !e.role.CanRoot() {
				return leasedRead{}, errNoRoot
			}
		} else if e.p2p {
			view, _ := ctx.Value(routingViewKey{}).(*cluster.View)
			if view == nil {
				return leasedRead{}, errNoRoot
			}
			roots := hashring.Rank(key.Chunk, view.Roots())
			if len(roots) == 0 {
				return leasedRead{}, errNoRoot
			}
			if roots[0].ID != e.selfID || !e.role.CanRoot() {
				return e.loadRouted(ctx, url, objectID, size, index, hop, key, want, view, roots)
			}
		} else if !e.role.CanRoot() {
			return leasedRead{}, errNoRoot
		}
		// Terminal origin work can be shared across normal/supply modes and epochs.
		isSpeculativeOuter := isSpeculative(ctx)
		return e.origins.do(ctx, blockLoadKey{block: key}, url, true, func(ctx context.Context) (leasedRead, error) {
			if l, ok, err := store.Acquire(e.store, key); err != nil {
				return leasedRead{}, err
			} else if ok {
				return leasedRead{lease: l, source: blockFromCache}, nil
			}
			b, err := e.receiveBuffer(ctx, key, want, isSpeculativeOuter)
			if err != nil {
				return leasedRead{}, err
			}
			defer b.abort()
			start := e.cfg.BlockStart(index)
			result, err := fetch.FetchInto(ctx, e.fetcher, url, start, start+want-1, b.bytes())
			if err != nil {
				return leasedRead{}, err
			}
			if int64(len(result.Data)) != want {
				return leasedRead{}, fmt.Errorf("engine: origin returned %d bytes for block %d, want %d", len(result.Data), index, want)
			}
			if !result.Coalesced {
				e.mx.recordIngress(blockFromOrigin, len(result.Data))
			}
			// Range-ignored responses retain their unpublished slot/temporary lease
			// without a second payload allocation or a cache entry.
			l, err := e.finishBuffer(&b, key, !result.RangeIgnored)
			return leasedRead{lease: l, source: blockFromOrigin}, err
		})
	})
	return r, err
}

// PeerLeaseSource serves validated whole blocks pinned through HTTP completion.
// The returned lease belongs to the caller, including on error or decline.
func (e *Engine) PeerLeaseSource() peer.LeaseSource {
	return func(ctx context.Context, req peer.BlockRequest) (store.Lease, bool, error) {
		return e.peerRead(ctx, req, false)
	}
}

func (e *Engine) peerRead(ctx context.Context, req peer.BlockRequest, owned bool) (store.Lease, bool, error) {
	if !e.relayableRequest(req) {
		e.mx.recordRelay(false)
		return store.Lease{}, false, nil
	}
	var l store.Lease
	var ok bool
	var err error
	if owned {
		var data []byte
		data, ok, err = e.store.Get(req.Key)
		l = store.HeapLease(data)
	} else {
		l, ok, err = store.Acquire(e.store, req.Key)
	}
	if err != nil {
		return l, false, err
	}
	if ok {
		e.mx.recordCacheHit()
		return l, true, nil
	}
	if req.URL == "" || req.Hop >= maxHop {
		e.mx.recordRelay(false)
		return store.Lease{}, false, nil
	}
	// Roles restrict cold work; validated immutable cache hits do not depend
	// on current placement or origin capability.
	if req.Supply {
		if !e.role.CanRoot() {
			return store.Lease{}, false, nil
		}
		ctx = supplyContext(ctx)
	} else if !e.role.CanRead() {
		if e.cluster == nil {
			return store.Lease{}, false, nil
		}
		view := e.cluster.Current()
		if view == nil {
			return store.Lease{}, false, nil
		}
		roots := hashring.Rank(req.Key.Chunk, view.Roots())
		if len(roots) == 0 || roots[0].ID != e.selfID {
			return store.Lease{}, false, nil
		}
		ctx = supplyContext(ctx)
	}
	ctx, flightKey := e.pinRouting(ctx, req.Key)
	if !isSupply(ctx) && req.HasEpoch && flightKey.epoch != req.Epoch {
		// A changed weight/role/membership basis may reverse reader order.
		// Decline a cold relay before joining a flight; terminal supply and
		// already-cached bytes remain usable during convergence.
		e.mx.recordRelay(false)
		return store.Lease{}, false, nil
	}
	size, err := e.Size(ctx, req.URL)
	if err != nil {
		return store.Lease{}, false, asRelayError(err)
	}
	if e.RangeUnsupported(req.URL) {
		e.mx.recordRelay(false)
		return store.Lease{}, false, nil
	}
	if size <= 0 || e.cfg.BlockStart(int64(req.Key.Block)) >= size {
		e.mx.recordRelay(false)
		return store.Lease{}, false, nil
	}

	oid, _ := chunk.ObjectID(req.URL)
	if owned {
		var data []byte
		data, err = e.block(ctx, req.URL, oid, size, int64(req.Key.Block), req.Hop)
		l = store.HeapLease(data)
	} else {
		l, err = e.borrowBlock(ctx, req.URL, oid, size, int64(req.Key.Block), req.Hop)
	}
	if err != nil {
		return l, false, asRelayError(err)
	}
	e.mx.recordRelay(true)
	return l, true, nil
}
