package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

type supplyKey struct{}

func isSupply(ctx context.Context) bool { v, _ := ctx.Value(supplyKey{}).(bool); return v }
func supplyContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, supplyKey{}, true)
}

var errNoRoot = errors.New("engine: no usable supply root")

// loadRouted preserves directed dependencies: ancestors may relay, whereas
// explicitly selected roots terminate at cache/origin. Neither root misses nor
// pressure cause cache-holder searches. Each address is attempted at most once
// here; all attempts share the caller's bounded block-flight context.
func (e *Engine) loadRouted(ctx context.Context, url, objectID string, size, index int64, hop int, key store.BlockKey, want int64, view *cluster.View, roots []hashring.Node) (leasedRead, error) {
	if hop >= maxHop || !e.role.CanRead() {
		return leasedRead{}, errNoRoot
	}
	ranked, _ := e.treeNodes(ctx, view, objectID, e.fileKey(objectID), key.Chunk)
	primary, backup := e.hedgeTargets(ranked, view)
	attempted := make(map[string]bool)
	rootAddr := ""
	if m, ok := view.Get(roots[0].ID); ok {
		rootAddr = m.Addr
	}
	get := func(ctx context.Context, addr string, supply bool) peerResult {
		r, dst, cache, err := e.receiveBuffer(ctx, key, want)
		if err != nil {
			return peerResult{err: err}
		}
		defer r.Abort()
		data, held, err := e.peer.GetInto(ctx, addr, peer.BlockRequest{Key: key, URL: url, Hop: hop + 1, Supply: supply}, dst, nil)
		if err != nil || !held {
			return peerResult{held: held, err: err}
		}
		e.mx.recordIngress(blockFromPeer, len(data))
		l, err := e.finishBuffer(&r, key, data, cache)
		return peerResult{lease: l, held: err == nil, err: err}
	}
	var last error
	if primary != "" {
		attempted[primary] = true
		if backup != "" {
			attempted[backup] = true
		}
		r, ok := e.fetchHedgedResult(ctx, primary, backup, func(ctx context.Context, addr string) peerResult { return get(ctx, addr, addr == rootAddr) })
		if ok {
			return leasedRead{lease: r.lease, source: blockFromPeer}, nil
		}
		last = r.err
		if errors.Is(last, peer.ErrUpstreamRefused) {
			return leasedRead{}, last
		}
	}
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
		attempted[m.Addr] = true
		r := get(ctx, m.Addr, true)
		if r.err == nil && r.held {
			return leasedRead{lease: r.lease, source: blockFromPeer}, nil
		}
		r.lease.Release()
		last = r.err
		if errors.Is(last, peer.ErrUpstreamRefused) {
			return leasedRead{}, last
		}
	}
	if last != nil {
		return leasedRead{}, fmt.Errorf("%w: %w", errNoRoot, last)
	}
	return leasedRead{}, errNoRoot
}
