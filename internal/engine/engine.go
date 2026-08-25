// Package engine is DART's read-through orchestration: it maps an arbitrary
// byte range of an object onto blocks, serves each block from the local cache,
// from a peer, or (as a last resort) from origin, and streams the requested
// bytes back in order.
//
// It wires internal/chunk (addressing), internal/store (block cache),
// internal/fetch (origin read-through with singleflight), internal/hashring
// (placement and the distribution tree) and internal/peer (block transport).
//
// With Cluster, Peer and SelfID configured, a miss is routed to this node's
// parent in the preorder distribution tree rather than straight to origin; the
// parent relays (fetching from its own parent or origin as needed), so only the
// tree root touches origin. Relays stream cut-through, so a multi-hop chain
// pipelines instead of storing-and-forwarding at every hop. Leaving those
// options unset gives single-node behavior, where every miss goes to origin.
//
// A block is validated against the object geometry before it is cached: the
// block cache is write-once per key, so admitting wrongly-sized bytes would be a
// permanent error that no later fetch could repair. See docs/engine.md.
package engine

import (
	"net/http"
	"strings"

	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
	"github.com/data-accelerator/dart/internal/tracker"
)

// Options configures an Engine.
type Options struct {
	// Chunk is the object/chunk/block grid. Must be valid (see chunk.Config).
	Chunk chunk.Config
	// Store is the block cache. Required.
	Store store.Store
	// Fetcher is the origin read-through. Typically a *fetch.Coalescing so
	// concurrent misses for the same block dedup. Required.
	Fetcher fetch.Fetcher
	// Namespace scopes chunk keys; defaults to "dart" if empty.
	Namespace string

	// --- P2P (optional) ---
	// When Cluster, Peer, and SelfID are all set, a cache miss first tries to
	// pull the block from the owning peer(s) (HRW over Cluster.Ready()) before
	// falling back to origin. Leaving any unset disables peer pulls (single-node
	// behavior: every miss goes to origin).
	Cluster cluster.Provider
	Peer    *peer.Client
	SelfID  string
	// Fanout is the distribution-tree branching factor (children per node) used
	// when routing a miss to the parent; default 2. Larger fanout = shallower
	// tree (owner has more direct children).
	Fanout int

	// Replicas is how many HRW candidates count as authoritative holders of a
	// chunk (the owned budget); default 1. Blocks this node is not in the top-R
	// for are cached as borrowed.
	Replicas int

	// Hedge enables tail-latency hedging on the peer path: once a fetch from the
	// tree parent exceeds the estimated p99, a duplicate goes to the grandparent
	// (or the root) and the first answer wins. Disabled by default so the
	// behavior is opt-in.
	Hedge bool
	// HedgeRatio caps the share of peer fetches that may hedge; default 0.05.
	// Hedging trades duplicate work for latency, so this bound is what keeps a
	// uniformly slow cluster from doubling its own load.
	HedgeRatio float64

	// Metrics, if non-nil, instruments block sources, bytes, and latencies.
	// Build it with NewMetrics(registry); nil disables instrumentation.
	Metrics *Metrics

	// TrackerClient and TrackerRegistry enable the active-reader-set tree: the
	// distribution tree is built over the nodes currently reading the object
	// (so a parent is always another reader) instead of over all Ready members.
	// TrackerRegistry is this node's local tracker (used when this node is the
	// HRW tracker for a file); TrackerClient reaches remote trackers. Leaving
	// both nil keeps all-member routing.
	TrackerClient   *tracker.Client
	TrackerRegistry *tracker.Registry
}

// Engine serves object byte ranges from cache with origin read-through.
type Engine struct {
	cfg     chunk.Config
	store   store.Store
	fetcher fetch.Fetcher
	ns      string

	// P2P (optional)
	cluster  cluster.Provider
	peer     *peer.Client
	selfID   string
	fanout   int
	replicas int
	p2p      bool
	mx       *Metrics

	// Tail-latency hedging.
	hedgeEnabled bool
	latency      *latencyEstimator
	hedges       *hedgeLimiter

	// Active-reader-set tracking (optional).
	trackerClient *tracker.Client
	trackerReg    *tracker.Registry
	rsMu          sync.Mutex
	rs            map[string]*readerSet

	mu    sync.Mutex
	sizes map[string]sizeMeta // objectID -> probe result

	// opener streams verbatim origin responses for the passthrough fallback
	// (Range-ignoring origins); nil when the fetcher cannot stream.
	opener fetch.Opener
}

// sizeMeta caches what the size probe learned about an object: its size, and
// whether the origin honors Range at all. A noRange object must be proxied
// verbatim — a per-block fetch would pull the whole object per block.
type sizeMeta struct {
	size    int64
	noRange bool
}

// New validates opt and constructs an Engine.
func New(opt Options) (*Engine, error) {
	if err := opt.Chunk.Validate(); err != nil {
		return nil, err
	}
	if opt.Store == nil || opt.Fetcher == nil {
		return nil, errors.New("engine: Store and Fetcher are required")
	}
	ns := opt.Namespace
	if ns == "" {
		ns = "dart"
	}
	// The namespace is a ChunkKey field: containing the 0x1F separator would
	// make chunk-key serialization non-injective (see docs/chunk.md §3.4).
	if strings.IndexByte(ns, chunk.UnitSep) >= 0 {
		return nil, errors.New("engine: Namespace must not contain the chunk-key separator (0x1F)")
	}
	e := &Engine{
		cfg:     opt.Chunk,
		store:   opt.Store,
		fetcher: opt.Fetcher,
		ns:      ns,
		cluster: opt.Cluster,
		peer:    opt.Peer,
		selfID:  opt.SelfID,
		fanout:  opt.Fanout,
		mx:      opt.Metrics,
		sizes:   make(map[string]sizeMeta),

		replicas:      opt.Replicas,
		hedgeEnabled:  opt.Hedge,
		latency:       newLatencyEstimator(),
		hedges:        newHedgeLimiter(opt.HedgeRatio),
		trackerClient: opt.TrackerClient,
		trackerReg:    opt.TrackerRegistry,
		rs:            make(map[string]*readerSet),
	}
	e.opener, _ = opt.Fetcher.(fetch.Opener)
	if e.fanout < 1 {
		e.fanout = 2
	}
	if e.replicas < 1 {
		e.replicas = 1
	}
	e.p2p = e.cluster != nil && e.peer != nil && e.selfID != ""
	return e, nil
}

// Size returns the total size of the object at url, probing origin once and
// caching the result by object identity. The probe fetches a single byte and
// reads the total from the response (Content-Range, or Content-Length if the
// origin ignores Range). A probe that reveals a Range-ignoring origin also
// marks the object for passthrough (see RangeUnsupported).
//
// A 206 response that hides the total (`Content-Range: bytes 0-0/*`) is a
// probe failure, not a size: block geometry is impossible without the total,
// and caching a fabricated one would poison every later read of the object —
// the cache is write-once and process-lifetime (see §3.3 of docs/engine.md).
// Size therefore returns an error and caches nothing, so a corrected origin
// recovers without a restart.
func (e *Engine) Size(ctx context.Context, url string) (int64, error) {
	oid, _ := chunk.ObjectID(url)
	e.mu.Lock()
	if sm, ok := e.sizes[oid]; ok {
		e.mu.Unlock()
		return sm.size, nil
	}
	e.mu.Unlock()

	// Probe with a single-byte ranged GET and take the total from Content-Range.
	// Deliberately not a HEAD: a presigned upstream is signed for GET only (the verb
	// is inside the signature), so HEAD would return 403. See fetch.Fetcher.
	r, err := e.fetcher.Fetch(ctx, url, 0, 0)
	if err != nil {
		// bytes=0-0 is satisfiable for every non-empty object (RFC 7233), so a
		// 416 to the probe means the object is empty: cache size 0 and let the
		// handler serve a valid empty 200.
		var se *fetch.StatusError
		if errors.As(err, &se) && se.Code == http.StatusRequestedRangeNotSatisfiable {
			e.mu.Lock()
			e.sizes[oid] = sizeMeta{size: 0}
			e.mu.Unlock()
			return 0, nil
		}
		return 0, err
	}
	if r.Total < 0 && !r.RangeIgnored {
		return 0, fmt.Errorf("engine: origin %s answered 206 without revealing the object size", fetch.Redact(url))
	}
	e.mu.Lock()
	// For a Range-ignoring origin the total may legitimately stay unknown (-1
	// on a chunked 200); it is never fabricated — the object is proxied
	// verbatim via ServePassthrough, which needs no size.
	e.sizes[oid] = sizeMeta{size: r.Total, noRange: r.RangeIgnored}
	e.mu.Unlock()
	return r.Total, nil
}

// RangeUnsupported reports whether the origin serving url is known to ignore
// Range requests, as learned by the Size probe (so Size must have been called
// first). Such objects are proxied verbatim (ServePassthrough): a per-block
// fetch would pull the whole object per block.
//
// The mark is process-local and never expires: an origin does not grow Range
// support for an existing object, and a restart re-probes anyway.
func (e *Engine) RangeUnsupported(url string) bool {
	oid, _ := chunk.ObjectID(url)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sizes[oid].noRange
}

// ErrRangeNotSatisfiable is returned by Serve for a start beyond the object.
var ErrRangeNotSatisfiable = errors.New("engine: range not satisfiable")

// Serve writes the inclusive byte range [start, end] of the object at url to w,
// serving each covering block from cache or origin. end is clamped to the last
// byte of the object; start must be within the object.
func (e *Engine) Serve(ctx context.Context, w io.Writer, url string, start, end int64) error {
	size, err := e.Size(ctx, url)
	if err != nil {
		return err
	}
	if start < 0 || start >= size {
		return ErrRangeNotSatisfiable
	}
	if end >= size {
		end = size - 1
	}
	oid, _ := chunk.ObjectID(url)
	for _, seg := range e.cfg.Segments(start, end) {
		data, err := e.block(ctx, url, oid, size, seg.BlockIndex, 0)
		if err != nil {
			return err
		}
		lo := seg.From - e.cfg.BlockStart(seg.BlockIndex)
		hi := seg.To - e.cfg.BlockStart(seg.BlockIndex) + 1
		if hi > int64(len(data)) {
			return fmt.Errorf("engine: short block %d: have %d bytes, need %d", seg.BlockIndex, len(data), hi)
		}
		if _, err := w.Write(data[lo:hi]); err != nil {
			return err
		}
		e.mx.recordClientBytes(int(hi - lo))
	}
	return nil
}

// maxHop bounds relay recursion depth (loop safety under membership skew).
const maxHop = 64

// block returns the bytes of one block: from the local cache, else (with P2P)
// from its parent in the distribution tree, else from origin. Fetched bytes are
// cached. hop is the relay depth (0 when serving a local client).
func (e *Engine) block(ctx context.Context, url, objectID string, size, blockIndex int64, hop int) ([]byte, error) {
	ck := chunk.ChunkKey(e.ns, objectID, e.cfg.ChunkOfBlock(blockIndex))
	key := store.BlockKey{Chunk: ck, Block: uint64(blockIndex)}
	if data, ok, err := e.store.Get(key); err != nil {
		return nil, err
	} else if ok {
		e.mx.recordCacheHit()
		return data, nil
	}
	// The block's byte length is fixed by the object geometry: a full BlockSize,
	// or a shorter final block. Any source that returns a different length has
	// handed us corrupt data, and caching it would be a permanent, self-
	// propagating error because the block cache is write-once per key (a later
	// correct fetch could never overwrite it, only eviction would clear it). So a
	// mismatch is refused rather than stored. See docs/engine.md.
	want := e.blockLen(size, blockIndex)
	if e.p2p {
		start := time.Now()
		if data, ok := e.fromPeer(ctx, url, objectID, ck, key, hop); ok {
			if int64(len(data)) == want {
				e.mx.recordPeerHit(len(data), time.Since(start))
				e.putBlock(key, data, ck)
				return data, nil
			}
			// A peer served a wrongly-sized block: do not trust or cache it; fall
			// through to origin, which is authoritative.
		}
	}
	originStart := time.Now()
	r, err := fetch.FetchBlock(ctx, e.fetcher, url, e.cfg.BlockSize, blockIndex, size)
	if err != nil {
		return nil, err
	}
	if int64(len(r.Data)) != want {
		return nil, fmt.Errorf("engine: origin returned %d bytes for block %d, want %d", len(r.Data), blockIndex, want)
	}
	e.mx.recordOrigin(len(r.Data), time.Since(originStart), r.Coalesced)
	if r.RangeIgnored {
		// The origin answered the ranged request with a whole-object 200 and we
		// sliced the window out of it. Serving those bytes is fine, but caching
		// them is not: a Range-blind origin pays a full object per block, and
		// such objects are meant for the passthrough path (§3.9). This is
		// defense in depth — callers already decline via RangeUnsupported.
		return r.Data, nil
	}
	e.putBlock(key, r.Data, ck)
	return r.Data, nil
}

// blockLen is the exact byte length of block blockIndex given the object size:
// a full BlockSize, except the final block, which is the remainder. Every
// caller guarantees the block lies within the object (0 <= start < size), so
// the result is always in (0, BlockSize].
func (e *Engine) blockLen(size, blockIndex int64) int64 {
	if n := size - blockIndex*e.cfg.BlockSize; n < e.cfg.BlockSize {
		return n
	}
	return e.cfg.BlockSize
}

// fromPeer routes a miss to this node's parent in the preorder distribution
// tree. The tree is built over the active reader set when available (so the
// parent is another reader) and otherwise over all Ready members. The parent
// relays (fetching from its own parent/origin as needed), so it usually holds
// the block. If this node is the tree root it returns ok=false and the caller
// fetches origin. hop guards against loops under membership skew.
func (e *Engine) fromPeer(ctx context.Context, url, objectID string, chunkKey uint64, key store.BlockKey, hop int) ([]byte, bool) {
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
	return e.fetchHedged(ctx, primary, backup, peer.BlockRequest{Key: key, URL: url, Hop: hop + 1})
}

// putBlock stores a block, classifying it as owned or borrowed so the two
// budgets stay isolated (see store's tiered layer). A node owns a block when it
// is in the HRW top-Replicas for the chunk over all Ready members — that is the
// authoritative placement, independent of who happens to be reading. Everything
// else (a copy fetched for a local client, or relayed for a peer) is borrowed.
//
// Insertion is best effort: a full or admission-rejecting cache must not fail a
// read that already has the bytes.
func (e *Engine) putBlock(key store.BlockKey, data []byte, chunkKey uint64) {
	cs, ok := e.store.(store.ClassStore)
	if !ok {
		_ = e.store.Put(key, data)
		return
	}
	_, _ = cs.PutClass(key, data, e.classOf(chunkKey))
}

// classOf reports whether this node is an authoritative holder of the chunk.
func (e *Engine) classOf(chunkKey uint64) store.Class {
	if !e.p2p {
		// Single-node: there is no placement, so everything this node caches is
		// effectively its own.
		return store.Owned
	}
	view := e.cluster.Current()
	if view == nil {
		return store.Borrowed
	}
	ranked := hashring.Rank(chunkKey, view.Ready())
	for i, n := range ranked {
		if i >= e.replicas {
			break
		}
		if n.ID == e.selfID {
			return store.Owned
		}
	}
	return store.Borrowed
}

// fileKey is the deterministic key identifying an object for tracker selection.
// It uses a sentinel chunk index so it never collides with a real chunk's
// placement key.
func (e *Engine) fileKey(objectID string) uint64 {
	return chunk.ChunkKey(e.ns, objectID, -1)
}

// PeerSource returns a peer.Source that serves blocks to other nodes as a
// relay: on a local miss it fetches the block via its own parent/origin (using
// the request's X-DART-Origin url), caches it, and serves it. This is what lets
// intermediate tree nodes offload the owner. Use it as the peer.Server.Src on a
// P2P node; a store-only node can use peer.StoreSource instead.
func (e *Engine) PeerSource() peer.Source {
	return func(ctx context.Context, req peer.BlockRequest) ([]byte, bool, error) {
		if data, ok, err := e.store.Get(req.Key); err != nil {
			return nil, false, err
		} else if ok {
			return data, true, nil
		}
		if req.URL == "" || req.Hop >= maxHop {
			e.mx.recordRelay(false)
			return nil, false, nil // cannot / should not relay
		}
		size, err := e.Size(ctx, req.URL)
		if err != nil {
			return nil, false, asRelayError(err)
		}
		if e.RangeUnsupported(req.URL) {
			// Same decline as the streaming source: blocks cannot be fetched
			// piecemeal from this origin, so decline rather than pulling the
			// whole object per block (and caching fragments of it).
			e.mx.recordRelay(false)
			return nil, false, nil
		}
		oid, _ := chunk.ObjectID(req.URL)
		data, err := e.block(ctx, req.URL, oid, size, int64(req.Key.Block), req.Hop)
		if err != nil {
			return nil, false, asRelayError(err)
		}
		e.mx.recordRelay(true)
		return data, true, nil
	}
}
