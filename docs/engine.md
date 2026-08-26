# `internal/engine`

DART's read-through orchestration: serve an arbitrary byte range of an object
from the block cache, from a peer in the distribution tree, or (as a last resort)
from origin, and stream the requested bytes back. Plus an `http.Handler` that
realizes the HTTP service layer (arbitrary Range in, non-chunked response out).

- Source: `internal/engine/engine.go`, `internal/engine/handler.go`
- Tests: `internal/engine/engine_test.go`, `internal/engine/handler_test.go`
- Import path: `github.com/data-accelerator/dart/internal/engine`

## 1. Overview

This is the first end-to-end path (M1): it wires `internal/chunk` (addressing),
`internal/store` (block cache), and `internal/fetch` (origin read-through with
singleflight) into a working single-node caching proxy. A request's byte range
is decomposed into blocks; each block is served from cache or, on a miss,
fetched from origin and stored, then the requested sub-bytes are streamed in
order.

Peer pulls are wired in (optional): when a `Cluster`, `Peer` client, and
`SelfID` are all configured, a cache miss is routed to this node's **parent in
the preorder distribution tree** (branching factor `Fanout`); the parent
**relays** (fetching from its own parent/origin as needed, via `PeerStreamSource`),
so it usually holds the block. The tree root (owner) fetches origin. This offloads
the owner under a thundering herd.

The tree spans the **active reader set** when a tracker is configured (so a
parent is always another reader — see docs/tracker.md) and otherwise all Ready
members. Without P2P configured the engine is single-node (every miss goes to
origin).

Reader-set lookups are **cached per object for half the granted tracker lease**
(and the cache doubles as the lease renewal: a hit re-serves the frozen set, a
miss re-JOINs, which refreshes this node's own lease). Caching for half the
lease guarantees renewal always lands before the lease lapses; a tracker that
grants nothing falls back to a 2 s period. Expired entries are swept as the
cache grows past a few hundred objects, so the map tracks objects currently
being read and stays small. When the tracker is unreachable the engine falls
back to all-member routing for that lookup (the reader set is soft state).

## 2. Concepts

| Term | Meaning |
|---|---|
| `Engine` | Orchestrator: Size + Serve over cache/origin. |
| object size | Learned once per object via a 1-byte probe, cached in-process. |
| `Handler` | `http.Handler` translating HTTP Range ⇄ `Engine.Serve`. |

## 3. Public API

### 3.1 `type Options` / `func New(Options) (*Engine, error)`

```go
type Options struct {
    Chunk     chunk.Config   // grid; must be valid
    Store     store.Store    // block cache; required
    Fetcher   fetch.Fetcher  // origin read-through; required (typically *fetch.Coalescing)
    Namespace string         // chunk-key namespace; default "dart"

    // P2P (optional; all three enable peer pulls):
    Cluster  cluster.Provider // membership source
    Peer     *peer.Client     // peer block client
    SelfID   string           // this node's cluster ID
    Fanout   int              // distribution-tree branching factor (default 2)
    Replicas int              // HRW candidates that authoritatively hold a chunk (default 1)
    Hedge      bool           // hedge a slow peer fetch (default off)
    HedgeRatio float64        // max share of fetches allowed to hedge (default 0.05)

    // Active-reader-set tree (optional):
    TrackerClient   *tracker.Client   // reach remote trackers
    TrackerRegistry *tracker.Registry // this node's local tracker
}
```

`New` validates the chunk config and required fields. Peer pulls are enabled
only when `Cluster`, `Peer`, and `SelfID` are all set.

### 3.2 `func (e *Engine) Size(ctx, url string) (int64, error)`

Returns the object's total size, probing origin once (a `bytes=0-0` fetch) and
caching the result by object identity (`chunk.ObjectID`). For immutable
content-addressed objects the size is permanently valid. The probe also
records whether the origin honored the Range: a `200` answer marks the object
Range-unsupported (see §3.9).

A `206` that hides the total (`Content-Range: bytes 0-0/*`) is a **probe
failure**, not a size: block geometry is impossible without the total, and
caching a fabricated one would poison every later read of the object (the
cache is write-once and process-lifetime — §3.3). `Size` returns an error and
caches nothing, so a corrected origin recovers without a restart.

### 3.3 `func (e *Engine) Serve(ctx, w io.Writer, url string, start, end int64) error`

Writes the inclusive range `[start, end]` to `w`. `end` is clamped to the last
byte; `start` beyond the object returns `ErrRangeNotSatisfiable`. Each covering
block is served from cache or fetched+stored; only the requested sub-bytes of
each block are written, in order.

**Block-length validation.** A block's byte length is fixed by the object
geometry (a full `BlockSize`, or a shorter final block). Before a fetched block
is cached, its length is compared against that expectation, on **both** the
origin and the peer paths:

- an origin block of the wrong length fails the read (it is never cached);
- a peer that returns a wrongly-sized block is not trusted — the read falls
  through to origin, which is authoritative;
- the cut-through relay (`relayFromParent`) applies the same check to the
  bytes it forwards: relay responses carry no Content-Length (chunked), so a
  short clean EOF is indistinguishable from success at the transport — the
  relay compares the streamed length against the geometry before caching. A
  mismatched block is still served on (the leaf's own check protects clients)
  but never cached; when the object size cannot be resolved the relayed bytes
  cannot be validated and are not cached either.

This is load-bearing because the block cache is **write-once per key**: a later
correct fetch can never overwrite a bad block, only eviction would clear it. So
a single short read (e.g. from a range-clamping proxy) that slipped through
would be a permanent, self-propagating error. Refusing it at ingestion is what
keeps that from happening.

### 3.4 `type Handler` / `func NewStaticHandler(e *Engine, originURL string) *Handler`

```go
type Handler struct {
    E       *Engine
    Resolve func(*http.Request) (string, error) // request -> origin URL; required
}
```

`ServeHTTP`:

- Accepts `GET`/`HEAD` (else `405`).
- `Resolve` failure → `400`; origin/size failure → `502`.
- Parses a **single** Range header (`bytes=a-b`, `bytes=a-`, `bytes=-n`); a
  missing Range serves the whole object; unsatisfiable/multi-range → `416` with
  `Content-Range: bytes */size`. Every Range form — suffix included — is
  unsatisfiable on an empty object: a suffix `bytes=-n` would otherwise clamp
  to the inverted interval `[0, -1]`, so the parser rejects it instead of
  emitting a `206` with an invalid `Content-Range`.
- **Always sets an explicit `Content-Length`** and `Accept-Ranges: bytes`;
  responds `206` + `Content-Range` for a range, else `200`. The explicit
  Content-Length guarantees a **non-chunked** response even though the body is
  streamed block by block (a requirement of the client plane, not an optimization).
- `HEAD` returns headers with no body.
- If `Size` marked the object **Range-unsupported** (the origin ignores
  `Range`), the request is instead proxied verbatim via `ServePassthrough` —
  no blocks are fetched, nothing is cached, and the peer plane is not
  consulted (§3.9).

`NewStaticHandler` serves every request from one fixed origin (single-origin /
testing). Real multi-mode resolvers (registry mirror, forward proxy, overlaybd
p2pConfig prefix) belong to a future proxy layer.

### 3.5 `func (e *Engine) PeerSource() peer.Source`

Returns the relay `Source` for this node's `peer.Server`: on a request for a
block it does not hold, it fetches the block via its own tree parent/origin
(using the request's `X-DART-Origin`), caches it, and serves it. This is what
makes intermediate tree nodes offload the owner. A store-only node can use
`peer.StoreSource` instead (no relay).

**Malformed block indices are declined, not relayed (issue #52).** The wire
carries the block index as an unrestricted `uint64`, while block geometry is
int64 arithmetic. An index above `chunk.Config.MaxBlockIndex()` — above
`MaxInt64`, or large enough that `index*BlockSize` would overflow int64 — can
never come from a legitimate same-config peer, and computing with it would wrap
(wrapped-to-0 start silently serves block 0's bytes; a wrapped-negative start
drops the Range header and pulls the whole object). `PeerSource` and
`PeerStreamSource` decline such requests (`held=false`, recorded as a refused
relay) **before** cache lookup, relay selection, size probe, or origin I/O.
Declined, not errored: the malformed index is the requester's fault, and a 500
would charge this healthy relay on the requester's circuit breaker.

### 3.6 `func (e *Engine) PeerStreamSource() peer.StreamSource`

The **cut-through** counterpart of `PeerSource`, for a `peer.StreamServer`:

- a locally-held block is copied out under the store lock (`Get`) and streamed,
  so a concurrent eviction cannot tear it mid-write;
- a missing block is relayed from this node's tree parent, copying bytes to the
  requester **as they arrive** while tee-ing them into the local cache;
- the tree root (no parent) fetches from origin, caches, then streams.

The malformed-index decline of §3.5 applies identically here: nothing is
streamed, the sizer is not called, and no origin request is made.

This is what `cmd/dart` mounts, so a multi-hop chain pipelines: the tail node
starts receiving after roughly one block-transfer time instead of
depth x block-transfer time.

### 3.7 Cache classification (owned / borrowed)

When the configured `Store` also implements `store.ClassStore` (i.e. a
`store.Tiered`), the engine classifies every block it caches:

- **owned** — this node is in the HRW top-`Replicas` for the chunk over all Ready
  members. That is the authoritative placement, independent of who is reading.
- **borrowed** — anything else: a copy fetched for a local client, or relayed for
  a peer.

This keeps the two budgets isolated so borrowed churn cannot evict the owned
shard set (see docs/store.md §3.4). Without P2P there is no placement, so
everything cached is owned. Insertion stays best effort: a full or
admission-rejecting cache never fails a read whose bytes are already in hand.

### 3.8 Tail-latency hedging

The failure paths already have fallbacks: a `404` or a transport error falls
through to origin. What had no fallback is a peer that is **alive but slow** (GC
pause, disk hitch, or itself waiting on its own parent) — and in a tree that
stall cascades to the whole subtree below it.

With `Hedge` enabled, a peer fetch has **two distinct escalations**, and keeping
them separate matters:

| Escalation | Trigger | Rate limited? |
|---|---|---|
| **failover** | the parent *definitively* failed (error, or does not hold the block) | **No** — it is a reaction to a known-bad peer, not speculation. Throttling it would push the whole subtree to origin while a peer that almost certainly has the block sits idle. |
| **hedge** | the parent is merely *slow* (past the estimated p99) | **Yes** (`HedgeRatio`) — when a whole cluster is slow, unthrottled duplicates double its load and amplify the congestion they react to. |

So a peer fetch proceeds as:

1. ask the tree **parent** (primary) and start a timer at the estimated **p99**;
2. on expiry, if the hedge budget allows, race a duplicate at the **grandparent**
   (or the tree root) — both sit closer to the source, so they more likely already
   hold the block;
3. if a contender *fails* instead, escalate to the backup **immediately and
   unconditionally**;
4. take whichever answers `held=true` first and **cancel the loser**;
5. if neither can serve it, fall through to origin as before.

The hedge delay comes from an online estimator (exact p99 over a bounded window of
recent fetches), floored at 5 ms so a fast cluster does not hedge on noise and
capped at 2 s so a pathological sample cannot disable hedging. Banked hedge credit
is capped at one, so an idle period cannot release a burst.

Separately, `peer.Client` bounds every individual peer request with **three**
timeouts (dial 1 s, response header 10 s, request 30 s — see docs/peer.md §3.3), so
a stalled or departed peer can never hold a read open until the caller's context
expires. And when a `peer.Breaker` is configured, target selection **skips peers
whose circuit is open** — on the buffered Get path by walking further up the
ancestor chain, and on the streaming relay path by skipping an open-circuit
parent and falling through to origin — so a dead branch is routed around rather
than forcing every reader beneath it back to origin. A parent that *errors* on
the relay path before writing a byte is likewise treated as a decline (fall
through to origin); once bytes have streamed to the requester, a failure can
only propagate. If no ancestor is usable, the read falls through to origin.

Together these bound abrupt node death: the dial timeout makes the first affected
read fail in ~1 s, a dial failure opens the circuit on that single observation, and
every later read skips the departed peer entirely.

Metrics: `dart_hedge_total{event=fired|primary_won|backup_won}` and
`dart_peer_failover_total`. Comparing `backup_won` against `fired` shows whether
hedging is paying for itself; `dart_peer_failover_total` rising marks peers
actually going away. `primary_won`/`backup_won` are recorded **only when a hedge
actually fired** — with hedging disabled, or when the backup answered via
failover rather than speculation, no win is recorded, so the comparison stays
meaningful exactly when it is consulted. The latency estimator learns only from
fetches that actually held the block (`held=true`): a fast 404 miss must not
collapse the p99, or hedges would arm a few milliseconds into genuine fetches.
`RegisterStoreMetrics` and `RegisterPeerMetrics` additionally export cache
occupancy and open-circuit counts (see docs/observability.md).

### 3.9 Range-unsupported origins: `RangeUnsupported` / `ServePassthrough`

```go
func (e *Engine) RangeUnsupported(url string) bool
func (e *Engine) ServePassthrough(ctx context.Context, w http.ResponseWriter, r *http.Request, url string) error
```

An origin that ignores `Range` cannot be served through the block layer: every
per-block fetch would pull the whole object, and a block so fetched could never
be safely cached for a *different* range. When the `Size` probe (a `bytes=0-0`
GET) is answered with `200` instead of `206`, the object is marked
Range-unsupported (process-local, never expires — a restart re-probes), and
`Handler` then serves every request for it with `ServePassthrough`:

- the client's `Range` header is forwarded, and the origin's status, entity
  headers (`Content-Type`/`Content-Length`/`Content-Range`/`ETag`/
  `Last-Modified`/`Cache-Control`/`Accept-Ranges`) and body are streamed back
  **verbatim** — the client sees exactly what talking to the origin directly
  would produce;
- nothing is cached, nothing is routed through P2P, and passthrough traffic is
  not coalesced;
- a `HEAD` is served from an upstream GET (a presigned URL is signed for GET
  alone), forwarding headers with an empty body;
- an error is reported (as a clean `502`) only before the response is
  committed; afterwards a failure can only truncate the body.

`ServePassthrough` streams through the fetcher's `fetch.Opener` interface
(`HTTPFetcher` and `Coalescing` implement it); with a fetcher that cannot
stream it fails cleanly instead of falling back to per-block pulls. On the
peer plane, a relay that has marked an object Range-unsupported **declines**
relay requests for it (`held=false`) on both relay sources
(`PeerStreamSource` and `PeerSource`), so the requester uses its own
passthrough path rather than this node pulling the whole object per block on
its behalf. Defense in depth: `block()` never caches bytes sliced from a
Range-ignored whole-object response even when the marker is absent.

Metrics: `dart_passthrough_total{reason="range_unsupported"}` counts proxied
requests; their bytes are counted as both `client` and `origin_in` wire bytes
(they crossed both wires) but not as any block source.

## 4. Invariants & Guarantees

1. **Correct bytes**: `Serve` returns exactly `content[start:end+1]`, across
   block and chunk boundaries.
2. **Read-through + cache**: a block is fetched from origin at most once per
   coalesced flight and zero times more *while cached*. Two qualifications:
   insertion is best-effort — under cache pressure a block may be evicted
   before a later read, which then re-fetches it; and "at most once" assumes
   the configured `Fetcher` coalesces (the node wires `fetch.Coalescing`; a
   custom non-coalescing Fetcher re-fetches per concurrent miss). Bytes sliced
   from a Range-ignored whole-object 200 are served but never cached (§3.9).
3. **Non-chunked responses on the block path**: client reads served from the
   block engine always use Content-Length framing. Verbatim passthrough of a
   Range-blind origin forwards the origin's own framing, chunked included.
   An empty object is a valid `200` with `Content-Length: 0`; a Range request
   on it is `416` (`bytes */0`), suffix ranges included.
4. **Range semantics**: standard `200`/`206`/`416` with correct `Content-Range`.
5. **No wrapped geometry on the relay path**: a peer-supplied block index that
   cannot be represented in int64 range arithmetic
   (`> chunk.Config.MaxBlockIndex()`) is declined before any cache lookup,
   relay selection, size probe, or origin I/O — never served, never cached,
   never turned into a whole-object origin GET (issue #52; the fetch-layer
   arithmetic guard is in docs/fetch.md §3.4).

## 5. Concurrency & Call Permissions

- `Engine` is safe for concurrent use: the size cache is mutex-guarded, the
  store is concurrency-safe, and the `Coalescing` fetcher dedups concurrent
  block misses. Verified with `-race` under overlapping concurrent `Serve`.
- `Handler` is safe for concurrent use if `Resolve`/`E` are not mutated after
  construction.

## 6. Determinism / Stability Contract

- The block key uses `chunk.ChunkKey` (part of the wire protocol via placement),
  but nothing else here is on the wire. HTTP Range/Content-Range is the external
  client contract.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **85.4%** of statements. Uncovered lines are mostly rare I/O
  error branches (short block, mid-stream write error).
- **Note**: tests use `t.TempDir()` (store files); in the sandbox export
  `TMPDIR=$PWD/.gotmp`.
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./internal/engine/ -v -count=1
go test ./internal/engine/ -race -count=1
go test ./internal/engine/ -cover -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestServeFullAndRange` | full and sub-range serve return exact bytes |
| `TestServeAcrossBlocksAndChunks` | a range crossing block/chunk boundaries is assembled correctly |
| `TestServeEndClampAndBadStart` | end clamped to size; start beyond size errors |
| `TestSize` | object size probed correctly |
| `TestSizeHiddenTotalIsProbeFailure` | a 206 hiding the total fails loudly, caches nothing, and never fabricates a size |
| `TestCacheHitAvoidsRefetch` | repeated identical serve makes zero origin requests |
| `TestConcurrentServe` | overlapping concurrent reads correct and race-free |
| `TestServeOriginError` | unreachable origin surfaces an error |
| `TestNewValidation` | invalid config / nil store / nil fetcher rejected |
| `TestHandlerFull` | 200, full body, Accept-Ranges, non-chunked |
| `TestHandlerRange` | 206 + Content-Range + exact bytes, non-chunked |
| `TestHandlerSuffix` / `TestHandlerOpenEnded` | suffix and open-ended ranges |
| `TestHandler416` | unsatisfiable range → 416 + `bytes */size` |
| `TestHandlerHEAD` | HEAD returns headers, empty body |
| **`TestNeverSendsHeadUpstream`** | **a client HEAD is answered without ever HEADing the origin, against a presigned-GET origin that 403s any other verb** |
| `TestSizeProbeUsesRangedGet` | the size comes from exactly one `bytes=0-0` GET |
| `TestHandlerMethodNotAllowed` | non-GET/HEAD → 405 |
| `TestHandlerResolveError` / `TestHandlerOriginError` | 400 / 502 error paths |
| `TestParseRange` / `TestParseRangeEdge` | Range header parsing incl. edges |
| `TestEngineP2PPeerHit` | blocks pulled from a warmed owning peer (origin only for the size probe) |
| `TestEngineP2PPeerMissFallsBackToOrigin` | empty peer (404) → origin fallback, correct bytes |
| `TestEngineP2PSelfOwnerUsesOrigin` | self is owner → peers not consulted, origin used |
| `TestPeerSourceRelayFetchOnBehalf` | relay Source fetches-on-behalf and serves a block it did not hold |
| **`TestRelayHopBoundary`** | **both peer-facing sources: hop == maxHop-1 still relays; hop == maxHop declines without touching the origin** |
| `TestTreeMultiHopRelay` | 3-node fanout=1 chain: a tail request populates every node via the relay chain |
| `TestPeerStreamSourceLocalHit` | locally-held block streamed from the store |
| `TestPeerStreamSourceRootFetchesOrigin` | tree root satisfies a relay request from origin and caches it |
| `TestStreamRelayChainThroughEngines` | 3-node fanout=1 chain over streaming peer servers: bytes intact, every node cached |
| `TestStreamRelayRejectsWrongLengthBlock` | a short clean chunked relay stream is served on but never cached; a correct relay is cached |
| `TestStreamRelayDoesNotCacheWhenSizeUnresolved` | relayed bytes are not cached when the object size cannot be resolved |
| `TestClassOfMatchesPlacement` | owned iff in HRW top-`Replicas`, else borrowed; `Replicas=3` makes all members owners |
| `TestClassOfSingleNodeIsOwned` | without P2P everything cached is owned |
| `TestServeUsesOwnedBudget` | single-node serve fills the owned budget only |
| `TestServeBorrowedWhenNotOwner` | a non-owner caches into borrowed, never owned |
| `TestLatencyEstimator*` | quantile needs ≥16 samples; p50/p99 correctness; bounded window ages out old samples |
| `TestHedgeLimiterEnforcesRatio` | long-run hedge rate tracks the configured ratio (0.05/0.25/0.5) |
| `TestHedgeLimiterDefaultsAndClamps` / `NoBurstAfterIdle` | ratio defaults/clamps; banked credit cannot burst |
| `TestHedgeDelayDisabledAndBounds` | off when disabled or sample-starved; floored and capped |
| **`TestHedgeBeatsSlowPrimary`** | **a 5 s stalled parent no longer stalls the read — completes in ms via the backup** |
| `TestHedgeDisabledWaitsForPrimary` | the contrast: with hedging off the read waits out the stall and sends no duplicate |
| `TestPeerSourceDeclinesRangeBlindOrigin` | the buffered relay declines a Range-blind origin like the streaming one; nothing cached |
| `TestBlockDoesNotCacheRangeIgnored` | defense in depth: bytes sliced from a Range-ignored 200 are served, never cached |
| `TestHedgeWinMetricsRequireFiredHedge` | no hedge fired → no hedge-win counters (the backup_won/fired comparison stays meaningful) |
| `TestMissesDoNotFeedLatencyEstimator` | fast 404 misses never arm the hedge delay |
| `TestReaderSetCacheFollowsGrantedLease` | the reader-set cache renews at half the granted lease, never lapsing mid-read |
| `TestStreamRelayParentErrorFallsBackToOrigin` | an open-circuit/errored parent is skipped to origin, not propagated as a 500 |
| `TestEmptyObjectGetServes200` | empty object: 200 empty on both paths; a Range request stays 416 |
| `TestParseRangeSuffixEmptyObject` | a suffix range on an empty object is unsatisfiable (no inverted `[0,-1]` interval); non-empty suffix behavior unchanged |
| `TestEmptyObjectSuffixRangeServes416` | `bytes=-n` on an empty object → 416 + `Content-Range: bytes */0` for GET and HEAD, never a 206 with `bytes 0--1/0` |
| `TestNamespaceWithSeparatorRejected` | a namespace containing the chunk-key separator fails engine construction |
| `TestHedgeFallsBackWhenBothMiss` | both contenders 404 → ok=false → origin |
| `TestHedgeTargetsPickParentAndGrandparent` | primary=parent, backup=grandparent/root; root has no upstream; non-member asks the owner |
| `TestHedgeTargetsSkipsOpenCircuit` | an open parent is skipped and the grandparent promoted; all-open yields no target |
| `TestFromPeerFallsBackWhenCircuitOpen` | no usable ancestor → origin, with zero dials |
| `TestHedgeSameAddressNotRaced` | no pointless duplicate when there is no distinct backup |
| **`TestFailoverNotRateLimited`** | **with the hedge budget drained, a dead parent still escalates to the grandparent instead of falling to origin** |
| `TestFailoverDoesNotDoubleLaunch` | an in-flight hedge is not launched a second time when the primary then fails |
| `TestHandlerPassthroughForRangeBlindOrigin` | a 200-probe origin is proxied verbatim: 2 origin hits (probe+GET), then 1; nothing cached |
| `TestPassthroughForwardsRangeVerbatim` | the client's Range is forwarded; the origin's 200 full body is returned as-is |
| `TestPassthroughHEAD` | HEAD proxied via an upstream GET; Content-Length forwarded, empty body |
| `TestRangeCapableOriginNotMarked` | a 206-capable origin is never marked or passthrough-ed |
| `TestPassthroughCountsMetrics` | `dart_passthrough_total` increments; bytes count as client+origin wire bytes, not as any block source |
| `TestPassthroughUnavailableWithoutOpener` | a non-streaming fetcher fails the passthrough cleanly with 502 |
| `TestRelayDeclinesRangeBlindOrigin` | a relay declines (held=false) a block of a Range-unsupported object |
| **`TestPeerSourceRejectsMalformedBlockIndex`** | **wire indices above `MaxBlockIndex` (2^63, 2^64-1, first window-overflowing index) are declined before cache lookup/size probe/origin I/O; nothing cached; legitimate control still relays (issue #52)** |
| **`TestPeerStreamSourceRejectsMalformedBlockIndex`** | **same decline on the cut-through path: nothing streamed, sizer not called, zero origin requests; legitimate control still streams** |

## 8. Limitations & TODO

- **P2P tree wired**: a miss routes to the parent in the preorder tree; relay
  nodes fetch-on-behalf and cache, offloading the owner; the root fetches origin.
  Loop-bounded by `X-DART-Hop`. **Cut-through relay is implemented**
  (`PeerStreamSource` + `peer.StreamServer`), so hops pipeline rather than
  store-and-forward. The **active-reader-set tree** is implemented too: with a
  tracker configured the tree spans only the nodes reading the object (see
  docs/tracker.md), falling back to all Ready members otherwise. **Hedging,
  per-request timeouts, and per-peer circuit breaking** bound tail latency and
  isolate sick peers (§3.8). Still missing: `splice`-level zero copy and full
  epoch-skew convergence.
- **Hedging covers `Get`, not `Stream`**: the cut-through relay path
  (`relayFromParent`) is bounded by `peer.Client.Timeout` but does not yet race a
  duplicate, because a hedged stream would need the two bodies reconciled.
- **Range-unsupported origins are proxied, not cached** (§3.9): nothing about
  them is learned beyond the mark itself (no TTL, no re-probe until restart),
  and their traffic bypasses the P2P tree entirely.
- **Readahead**: `Serve` fetches covering blocks sequentially; parallel
  fan-out and prefetch of subsequent blocks are future work.
- **Streaming vs buffering**: blocks are fetched whole into memory (bounded by
  block size) then written; fine for cache hits/`sendfile` later.
- **Size cache lifetime**: sizes are cached for process life with no TTL —
  correct for immutable objects; mutable objects need the policy layer.
- **Mid-stream errors**: once headers are sent, a block error can only truncate
  the body (client detects via Content-Length); prefetching the first block
  before sending headers could turn some of these into clean error statuses.
