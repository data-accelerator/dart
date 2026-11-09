package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

type supplyKey struct{}

func isSupply(ctx context.Context) bool { v, _ := ctx.Value(supplyKey{}).(bool); return v }
func supplyContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, supplyKey{}, true)
}

type routingViewKey struct{}

// pinRouting freezes the ordering basis before admission, including for the
// owned-copy adapter. Reader snapshots may vary; membership weights may not.
func (e *Engine) pinRouting(ctx context.Context, key store.BlockKey) (context.Context, blockLoadKey) {
	k := blockLoadKey{block: key, supply: isSupply(ctx)}
	if !e.p2p || k.supply {
		return ctx, k
	}
	view, pinned := ctx.Value(routingViewKey{}).(*cluster.View)
	if !pinned {
		view = e.cluster.Current()
		ctx = context.WithValue(ctx, routingViewKey{}, view)
	}
	if view != nil {
		k.epoch = view.Epoch()
	}
	return ctx, k
}

var errNoRoot = errors.New("engine: no usable supply root")

// loadRouted preserves directed dependencies: ancestors may relay, whereas
// explicitly selected roots terminate at cache/origin. Neither root misses nor
// pressure cause cache-holder searches. Each address is attempted at most once
// here; all attempts share the caller's bounded block-flight context.
func (e *Engine) loadRouted(ctx context.Context, url, objectID string, size, index int64, hop int, key store.BlockKey, want int64, view *cluster.View, roots []hashring.Node) (leasedRead, error) {
	ctx, cancel := context.WithTimeout(ctx, fetch.FlightLimit(e.fetcher))
	defer cancel()
	if hop >= maxHop || !e.role.CanRead() {
		return leasedRead{}, errNoRoot
	}
	ranked, _ := e.treeNodes(ctx, view, objectID, e.fileKey(objectID), key.Chunk)
	primary, backup := e.hedgeTargets(ranked, view)
	attempted := make(map[string]bool)
	var attemptsMu sync.Mutex
	rootAddr := ""
	if m, ok := view.Get(roots[0].ID); ok {
		rootAddr = m.Addr
	}
	get := func(ctx context.Context, addr string, supply bool) peerResult {
		b, err := e.receiveBuffer(ctx, key, want, isSpeculative(ctx))
		if err != nil {
			return peerResult{err: err}
		}
		defer b.abort()
		// A skipped hedge did not contact this peer. Once other contenders
		// release their buffers, root fallback may still use that address.
		attemptsMu.Lock()
		already := attempted[addr]
		attempted[addr] = true
		attemptsMu.Unlock()
		if already {
			return peerResult{}
		}
		data, held, err := e.peer.GetInto(ctx, addr, peer.BlockRequest{Key: key, URL: url, Hop: hop + 1, Supply: supply, Epoch: view.Epoch(), HasEpoch: true}, b.bytes(), nil)
		if err != nil || !held {
			return peerResult{held: held, err: err}
		}
		e.mx.recordIngress(blockFromPeer, len(data))
		l, err := e.finishBuffer(&b, key, true)
		return peerResult{lease: l, held: err == nil, err: err}
	}
	var last error
	if primary != "" {
		r, ok := e.fetchHedgedResult(ctx, primary, backup, func(ctx context.Context, addr string) peerResult { return get(ctx, addr, addr == rootAddr) })
		if ok {
			return leasedRead{lease: r.lease, source: blockFromPeer}, nil
		}
		last = r.err
		if errors.Is(last, peer.ErrUpstreamRefused) {
			return leasedRead{}, last
		}
	}
	// fetchHedgedResult has joined its workers: attempted is now read serially.
	// A failed contender may have released the only buffer after its hedge
	// skipped admission, so even an admission error can recover at an untried root.
	for i, n := range roots {
		if i >= maxHop {
			break
		}
		if err := ctx.Err(); err != nil {
			return leasedRead{}, err
		}
		if n.ID == e.selfID {
			if !e.role.CanRoot() {
				continue
			}
			return e.readBorrowedBlock(supplyContext(ctx), url, objectID, size, index, hop)
		}
		m, ok := view.Get(n.ID)
		if !ok || m.Addr == "" || attempted[m.Addr] {
			continue
		}
		r := get(ctx, m.Addr, true)
		if r.err == nil && r.held {
			return leasedRead{lease: r.lease, source: blockFromPeer}, nil
		}
		r.lease.Release()
		last = r.err
		if errors.Is(last, peer.ErrUpstreamRefused) || errors.Is(last, ErrReceiveBudget) {
			return leasedRead{}, last
		}
	}
	if last != nil {
		return leasedRead{}, fmt.Errorf("%w: %w", errNoRoot, last)
	}
	return leasedRead{}, errNoRoot
}
