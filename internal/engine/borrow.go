package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
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
	if index < 0 || index > e.cfg.MaxBlockIndex() || size <= 0 || e.cfg.BlockStart(index) >= size {
		return leasedRead{}, ErrRangeNotSatisfiable
	}

	key := store.BlockKey{Chunk: chunk.ChunkKey(e.ns, objectID, e.cfg.ChunkOfBlock(index)), Block: uint64(index)}
	if l, ok, err := store.Acquire(e.store, key); err != nil {
		return leasedRead{}, err
	} else if ok {
		return leasedRead{lease: l, source: blockFromCache}, nil
	}
	r, err := e.borrowed.do(ctx, blockLoadKey{block: key, hop: hop, supply: isSupply(ctx)}, url, false, func(ctx context.Context) (leasedRead, error) {
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
			view := e.cluster.Current()
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
		// Origin results can be shared across relay depths; peer flights cannot,
		// because a skewed forwarding loop must continue toward the hop bound.
		return e.origins.do(ctx, blockLoadKey{block: key}, url, true, func(ctx context.Context) (leasedRead, error) {
			if l, ok, err := store.Acquire(e.store, key); err != nil {
				return leasedRead{}, err
			} else if ok {
				return leasedRead{lease: l, source: blockFromCache}, nil
			}
			r, dst, cache, err := e.receiveBuffer(ctx, key, want)
			if err != nil {
				return leasedRead{}, err
			}
			defer r.Abort()
			start := e.cfg.BlockStart(index)
			result, err := fetch.FetchInto(ctx, e.fetcher, url, start, start+want-1, dst)
			if err != nil {
				return leasedRead{}, err
			}
			if int64(len(result.Data)) != want {
				return leasedRead{}, fmt.Errorf("engine: origin returned %d bytes for block %d, want %d", len(result.Data), index, want)
			}
			if !result.Coalesced {
				e.mx.recordIngress(blockFromOrigin, len(result.Data))
			}
			var l store.Lease
			if result.RangeIgnored {
				// A range-blind endpoint is passthrough-only. Keep these response bytes
				// without publishing the reserved slot into the block cache.
				l = store.HeapLease(append([]byte(nil), result.Data...))
			} else {
				l, err = e.finishBuffer(&r, key, result.Data, cache)
			}
			return leasedRead{lease: l, source: blockFromOrigin}, err
		})
	})
	return r, err
}

func (e *Engine) receiveBuffer(ctx context.Context, key store.BlockKey, n int64) (store.Reservation, []byte, bool, error) {
	cache := true
	if s, ok := e.store.(store.Reserver); ok {
		r, err := s.Reserve(ctx, key, n, e.classOf(key.Chunk))
		if err == nil {
			return r, r.Bytes(), false, nil
		}
		if errors.Is(err, store.ErrNoSlot) || errors.Is(err, store.ErrBlockTooLarge) {
			cache = false
		} else if !errors.Is(err, store.ErrNoReservation) {
			return store.Reservation{}, nil, false, err
		}
	}
	if err := ctx.Err(); err != nil {
		return store.Reservation{}, nil, false, err
	}
	e.mx.recordTemporary(n)
	return store.Reservation{}, make([]byte, n), cache, nil
}
func (e *Engine) finishBuffer(r *store.Reservation, key store.BlockKey, data []byte, cache bool) (store.Lease, error) {
	if r.Bytes() != nil {
		return r.Commit()
	}
	if cache {
		e.putBlock(key, data, key.Chunk)
	}
	return store.HeapLease(data), nil
}

// PeerLeaseSource serves validated whole blocks pinned through HTTP completion.
// The returned lease belongs to the caller, including on error or decline.
func (e *Engine) PeerLeaseSource() peer.LeaseSource {
	return func(ctx context.Context, req peer.BlockRequest) (store.Lease, bool, error) {
		return e.peerRead(ctx, req, false)
	}
}

func (e *Engine) peerRead(ctx context.Context, req peer.BlockRequest, owned bool) (store.Lease, bool, error) {
	if req.Supply {
		if !e.role.CanRoot() {
			return store.Lease{}, false, nil
		}
		ctx = supplyContext(ctx)
	} else if !e.role.CanRead() {
		if e.cluster == nil {
			return store.Lease{}, false, nil
		}
		roots := hashring.Rank(req.Key.Chunk, e.cluster.Current().Roots())
		if len(roots) == 0 || roots[0].ID != e.selfID {
			return store.Lease{}, false, nil
		}
		ctx = supplyContext(ctx)
	}
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
