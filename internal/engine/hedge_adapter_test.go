package engine

import (
	"context"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// Test adapters exercise ancestor/hedge mechanics with caller-owned bytes.
// Production routing uses loadRouted and leased results exclusively.
func (e *Engine) fromPeer(ctx context.Context, url, objectID string, chunkKey uint64, key store.BlockKey, hop int, want int64) ([]byte, bool) {
	if hop >= maxHop {
		return nil, false
	}
	view := e.cluster.Current()
	if view == nil {
		return nil, false
	}
	ranked, _ := e.treeNodes(ctx, view, objectID, e.fileKey(objectID), chunkKey)
	if len(ranked) == 0 {
		return nil, false
	}
	primary, backup := e.hedgeTargets(ranked, view)
	if primary == "" {
		return nil, false // we are the tree root: fetch origin
	}
	return e.fetchHedged(ctx, primary, backup, peer.BlockRequest{Key: key, URL: url, Hop: hop + 1}, want)
}

// fetchHedged asks primary for the block, with two distinct escalations to
// backup:
//
//   - **failover** — primary definitively failed (error, or it does not hold the
//     block). Try backup immediately. This is *not* rate limited: it is a reaction
//     to a known-bad peer, not speculation, and throttling it would only push the
//     read to origin while a peer that almost certainly has the block sits idle.
//   - **hedge** — primary is merely slow (past the estimated p99). Race a
//     duplicate. This *is* rate limited, because when a whole cluster is slow,
//     unthrottled duplicates double its load and amplify the congestion.
//
// The first answer with held=true wins and the loser is cancelled. If neither
// contender can serve the block, ok is false and the caller falls back to origin.
func (e *Engine) fetchHedged(ctx context.Context, primary, backup string, req peer.BlockRequest, want int64) ([]byte, bool) {
	r, ok := e.fetchHedgedResult(ctx, primary, backup, func(ctx context.Context, addr string) peerResult {
		data, held, err := e.peer.GetValidated(ctx, addr, req, want, nil)
		return peerResult{lease: store.HeapLease(data), held: held, err: err}
	})
	defer r.lease.Release()
	return append([]byte(nil), r.lease.Bytes()...), ok
}
