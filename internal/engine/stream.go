package engine

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
)

// upstreamRefused wraps an origin credential refusal so peer.StreamServer can
// report it as such, instead of it looking like this node malfunctioned.
//
// A relay fetches on behalf of a requester using the requester's own upstream URL,
// which may be a presigned link. If that signature has expired the origin returns
// 401/403 through no fault of the relay, and the requester must not charge it to
// the relay's circuit breaker.
type upstreamRefused struct {
	err  error
	code int
}

func (u *upstreamRefused) Error() string       { return u.err.Error() }
func (u *upstreamRefused) Unwrap() error       { return u.err }
func (u *upstreamRefused) UpstreamStatus() int { return u.code }

var _ peer.UpstreamRefusedError = (*upstreamRefused)(nil)

// asRelayError classifies an error raised while fetching on a peer's behalf.
func asRelayError(err error) error {
	var se *fetch.StatusError
	if errors.As(err, &se) && se.Refused() {
		return &upstreamRefused{err: err, code: se.Code}
	}
	return err
}

// PeerStreamSource returns a peer.StreamSource for this node's
// peer.StreamServer. It is the cut-through counterpart of PeerSource:
//
//   - a locally-held block is streamed straight from the store (no full copy);
//   - a missing block is relayed from this node's own tree parent, copying bytes
//     to the requester *as they arrive* while tee-ing them into a buffer so the
//     block still lands in the local cache;
//   - if there is no parent (this node is the tree root) it falls back to
//     fetching the block from origin, then streams it.
//
// Cut-through matters on a multi-hop chain: every hop forwards while receiving,
// so the tail node starts getting bytes after roughly one block-transfer time
// rather than depth × block-transfer time.
func (e *Engine) PeerStreamSource() peer.StreamSource {
	return func(ctx context.Context, req peer.BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
		// Local hit: serve from the store. Get copies the block out under the
		// store lock, so a concurrent eviction that reuses the slot cannot tear the
		// bytes while they stream to the peer (a slot-backed GetReader could). The
		// block-sized copy is cheap next to the correctness it buys. See
		// docs/store.md §5.
		if data, ok, err := e.store.Get(req.Key); err != nil {
			return 0, false, err
		} else if ok {
			sizer(int64(len(data)))
			written, err := io.Copy(w, bytes.NewReader(data))
			e.mx.recordCacheHit()
			return written, true, err
		}
		if req.URL == "" || req.Hop >= maxHop {
			e.mx.recordRelay(false)
			return 0, false, nil // cannot / should not relay
		}

		// Relay: try this node's parent in the tree, cut-through.
		if e.p2p {
			if n, ok, err := e.relayFromParent(ctx, req, w); ok || err != nil {
				if ok {
					e.mx.recordRelay(true)
				}
				return n, ok, err
			}
		}

		// No parent (root) or the parent could not serve: fetch origin, cache,
		// then stream to the requester.
		size, err := e.Size(ctx, req.URL)
		if err != nil {
			return 0, false, asRelayError(err)
		}
		if e.RangeUnsupported(req.URL) {
			// Blocks cannot be fetched piecemeal from this origin; decline, so the
			// requester falls back to its own (passthrough) origin path instead of
			// this node pulling the whole object per block on its behalf.
			e.mx.recordRelay(false)
			return 0, false, nil
		}
		oid, _ := chunk.ObjectID(req.URL)
		data, err := e.block(ctx, req.URL, oid, size, int64(req.Key.Block), req.Hop)
		if err != nil {
			return 0, false, asRelayError(err)
		}
		sizer(int64(len(data)))
		written, err := io.Copy(w, bytes.NewReader(data))
		e.mx.recordRelay(true)
		return written, true, err
	}
}

// relayFromParent streams the block from this node's tree parent into w while
// tee-ing it into memory so it can be cached. ok is false when there is no
// usable parent (root, unknown address, or the parent declined).
func (e *Engine) relayFromParent(ctx context.Context, req peer.BlockRequest, w io.Writer) (int64, bool, error) {
	oid, _ := chunk.ObjectID(req.URL)
	addr, ok := e.parentAddr(ctx, oid, req.Key.Chunk)
	if !ok {
		return 0, false, nil
	}
	var buf bytes.Buffer
	up := peer.BlockRequest{Key: req.Key, URL: req.URL, Hop: req.Hop + 1}
	n, held, err := e.peer.Stream(ctx, addr, up, io.MultiWriter(w, &buf))
	if err != nil || !held {
		return n, false, err
	}
	// Cache what we relayed (best effort; a store error must not fail the relay).
	if int64(buf.Len()) == n && n > 0 {
		e.putBlock(req.Key, buf.Bytes(), req.Key.Chunk)
	}
	return n, true, nil
}

// parentAddr returns the peer address of this node's parent in the preorder
// distribution tree for chunkKey. The tree is built over the active reader set
// when available, else over all Ready members. ok is false when this node is the
// tree root, membership is unavailable, or the parent has no address.
func (e *Engine) parentAddr(ctx context.Context, objectID string, chunkKey uint64) (string, bool) {
	view := e.cluster.Current()
	if view == nil {
		return "", false
	}
	ranked, _ := e.treeNodes(ctx, view, objectID, e.fileKey(objectID), chunkKey)
	if len(ranked) == 0 {
		return "", false
	}
	selfIdx := -1
	for i, n := range ranked {
		if n.ID == e.selfID {
			selfIdx = i
			break
		}
	}
	targetIdx := 0 // not a member: the owner (root) is our upstream
	if selfIdx >= 0 {
		p := hashring.Parent(selfIdx, len(ranked), e.fanout)
		if p < 0 {
			return "", false // we are the root
		}
		targetIdx = p
	}
	m, ok := view.Get(ranked[targetIdx].ID)
	if !ok || m.Addr == "" {
		return "", false
	}
	return m.Addr, true
}
