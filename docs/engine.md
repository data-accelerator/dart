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
the preorder distribution tree** (`RootFanout` at the owner, `Fanout` below it); the parent
**relays** (fetching from its own parent/origin as needed, via `PeerSource`),
so it usually holds the block. The tree root (owner) fetches origin. This offloads
the owner under a thundering herd.

The tree starts at the preferred owner among root-eligible Ready members. Its
descendants are eligible **active readers** when a tracker is configured, or all
eligible Ready readers when tracking is disabled (ADR-0009). Root-only peers
never become descendants; non-root peers never contact origin. Without P2P,
ordinary nodes operate as single-node caches; node assembly requires P2P for
restricted roles.

Reader-set lookups are **cached per object for half the granted tracker lease**
(and the cache doubles as the lease renewal: a hit re-serves the frozen set, a
miss re-JOINs, which refreshes this node's own lease). Caching for half the
lease guarantees renewal always lands before the lease lapses; a tracker that
grants nothing falls back to a 2 s period. Expired entries are swept as the
cache grows past a few hundred objects, so the map tracks objects currently
being read and stays small. When the tracker is unreachable the engine falls
back to the preferred owner for that lookup (the reader set is soft state).

## 2. Concepts

| Term | Meaning |
|---|---|
| `Engine` | Orchestrator: Size + Serve over cache/origin. |
| object size | Learned once per origin endpoint via a 1-byte probe, cached in-process. |
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
    Role     cluster.Role     // Ordinary (zero), RootOnly, NonRoot
    Fanout   int              // branching factor below the preferred root (default 2)
    RootFanout int            // preferred root branching factor (zero defaults to 1)
    Replicas int              // HRW candidates that authoritatively hold a chunk (default 1)
    Hedge      bool           // hedge a slow peer fetch (default off)
    HedgeRatio float64        // max share of fetches allowed to hedge (default 0.05)

    PrefetchWindow int   // default 2, 1 disables; allowed 1..64 (zero = default)
    PrefetchBytes  int64 // default 64 MiB reserved speculative payload
    PrefetchLoads  int   // default 16 outstanding speculative loads/results
    ReceiveBytes  int64 // default max(256 MiB, BlockSize); live temporary receive payload
    ReceiveLoads  int   // default 64 temporary buffers, including leased results

    // Active-reader-set tree (optional):
    TrackerClient   *tracker.Client   // reach remote trackers
    TrackerRegistry *tracker.Registry // this node's local tracker
}
```

`New` validates the chunk config and required fields. Peer pulls are enabled
only when `Cluster`, `Peer`, and `SelfID` are all set. Negative prefetch limits
or a window above 64 are rejected; zero selects each default.

### 3.1.1 Owner-rooted role routing

ADR-0009 fixes the preferred root to the first HRW candidate from `View.Roots`.
`Options.Role` is the local capability. Active ordinary/non-root readers form the
ordered descendants; other root-only peers never enter the tree. The local
candidate set includes eligible self even when the tracker snapshot omits it,
deduplicated and ranked with the same member weights. The tracker snapshot is
unchanged. An unavailable snapshot yields owner plus eligible self; tracking
disabled uses all Ready reader-eligible members. Every ordinary parent/ancestor
precedes self in the common HRW reader order. Reader-set changes preserve that
relative order and cannot create a forwarding cycle (ADR-0010).

`Options.RootFanout` (zero defaults to 1; negative values are rejected) sets
only the preferred owner's normal child count. `Options.Fanout` (default 2)
sets the degree at every descendant, regardless of that member's root eligibility.
The first partition uses RootFanout and all remaining partitions use Fanout
(ADR-0011). Both values are common cluster configuration; setting them equal
produces a uniform tree. For root=1, fanout=2 and order R,A,B,C,D,E,F,G, edges
are R→A; A→B,E; B→C,D; E→F,G.

This is a topology bound, not a transfer quota: ancestor failover, hedging,
terminal supply, reader snapshot disagreement and reader churn can all increase
actual root sends. In particular, readers with disjoint snapshots can each be
the root's direct child. No admission semaphore or send-count enforcement is
added. Parent and hedge-ancestor navigation use the same two-degree tree.

A root supply request terminates at cache/origin. A non-root never falls through
to local origin; failed ancestors can be bypassed toward root candidates, with
at most the existing hop-bound count of root candidates under the shared flight
lifetime. Normal flights share across hops and reader snapshots. Terminal supply
has a separate flight so a backup root does not join its own waiting relay. A
local backup root bypasses peer HTTP and goes directly to cache/origin. Supply
and normal origin reads still coalesce in the origin flight. A healthy owner miss
never probes another root or a reader. Source refusals preserve their status and
stop redundant credential/source attempts; transport/path failure may fail over.

`func (e *Engine) ProbeSource() func(context.Context, string) (peer.Metadata, error)`
returns terminal metadata service for root-eligible nodes
and nil for non-root nodes. Non-root Size probes use a file-key-ranked root,
endpoint-scoped singleflight and the existing credential/cancellation rules.
Root-eligible nodes retain local endpoint probing. Validated cached blocks remain usable independently of current ownership or
role; cold role restrictions run after the cache lookup. Cached metadata and
blocks continue to work without a reachable root. Uncached frontend operations use
the terminal origin transport below.

`TestRolesColdReadAndRootFailover` uses real HTTP roots and a forbidden local
fetcher to verify delegated metadata, cold P2P reads, no replica query on owner
miss, backup supply, warm reads without roots and bounded cold failure.

### 3.1.2 Uncached origin delegation

`func (e *Engine) OriginTransport() http.RoundTripper` supplies the non-root
frontend transport. Each bodyless GET/HEAD chooses roots by the same file-key
ranking as metadata, independently of object size or the reader tree. It tries
at most the relay hop bound of candidates and skips duplicate addresses.
Peer failures permit fallback; every actual origin HTTP response, including
4xx/5xx, is terminal. No roots yields an error without calling the local fetcher.
Requests are not coalesced or cached. Ranking costs O(N log N) over eligible roots;
streaming storage is independent of object size. The transport is concurrent-safe.

`func (e *Engine) OriginSource(rt http.RoundTripper) http.RoundTripper` supplies a
root's terminal HTTP transport and accounts streamed response bytes as origin
ingress. It returns nil on non-root engines or for a nil transport. The argument
must perform local origin access and must never delegate to other DART peers.
Node assembly mounts it through `peer.OriginServer` on the peer listener.

For Range-unsupported reads, the delegated opener follows HTTP redirects through
`OriginTransport` again; it never opens a direct connection to the redirected
endpoint. Registry control delegation and redirect handling are wired separately
in [node.md](./node.md), using the same transport. The raw peer client never
follows a redirect itself. Header wait and blocked body reads have time bounds;
successful long transfers have no block-sized whole-request deadline. Closing a
body cancels the upstream operation. A midstream failure aborts the downstream
response, including unknown-length framing, and never publishes partial blocks.

`TestNonRootOriginFailoverKeepsRefusalsTerminal` covers definitive origin errors,
root transport failure, and the no-root path. `TestNonRootPassthroughAbortsIncompleteResponse`
covers truncated unknown-length delivery. `TestNonRootPassthroughCountsPeerBytes`
checks both supply and consumer byte accounting and non-root source rejection.

### 3.2 `func (e *Engine) Size(ctx, url string) (int64, error)`

Returns the object's total size, probing origin once (a `bytes=0-0` fetch) and
caching the result by endpoint identity (`fetch.OriginKey`), independently of
the content digest. The same immutable digest can be served by two endpoints
with different Range support or one with an unknown chunked total. Each is
probed separately; refreshed signatures for the same endpoint reuse metadata.
A `200` answer marks only that endpoint Range-unsupported (see §3.9).

When the fetcher implements `fetch.Prober`, `Size` calls `Probe` so concurrent
metadata flights also stay endpoint-scoped, even when block flights are keyed
by digest. Custom fetchers without `Prober` receive `Fetch(ctx, url, 0, 0)` and
must not coalesce these probes across endpoints. Blocks still use
`chunk.ObjectID` and remain shared across origins. A metadata hit costs O(URL
length) key derivation and a map lookup; a miss adds one ranged GET.

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

**Buffered miss coalescing (P2P enabled).** Concurrent misses for the same
block and membership ordering epoch share one normal load through geometry
validation and cache publication. Disjoint subranges do not launch independent peer downloads of
that block. A new leader rechecks cache after admission, closing the window
between an earlier miss and another load's completed insertion. Different
blocks remain independent. Hop is only a forwarding limit, never a content-load
identity. Normal loads pin a membership view before admission and carry its epoch
on outgoing requests. A cold receiver with a different epoch declines before
metadata probing or flight admission; cached bytes and terminal supply remain
usable. This bounds the weight/role/membership convergence case without splitting
ordinary same-epoch requests by hop or tracker reader-set epoch. Direct callers
omitting the epoch adopt the receiver's view; engine forwarding always includes it.

The leader runs synchronously in its serving caller, retaining the existing
peer/origin timeouts. A waiter can cancel independently. If an inline leader is
canceled, new callers and existing waiters may lead a replacement while the old
handler retains ownership of its backend cleanup; a failed URL or
credential is not reused against a different caller's URL. Completed failures
are not retained. Successful immutable bytes can be shared across URL aliases,
including when cache admission declines them. `Serve` and `PeerLeaseSource`
share immutable leased results, cloning references without copying payloads.
Both borrowed and owned-copy demand retry speculative admission skips using
demand headroom. Backend context errors remain shared same-URL failures while an inline leader's
client context is live. Same-URL takeover checks that client's cancellation,
rather than classifying the backend error as a canceled leader.

The compatibility `PeerSource` and owned block adapter use the same lease-flight
lifetime algorithm and copy immutable results into independent mutable bytes,
with O(block bytes) copying. Both adapters join the same leased
miss path. Coordination protects metadata, never network operations or payload
copies. Metadata probes remain endpoint-scoped independently. Origin lease
flights share across relay depths while retaining one result until all consumers
detach. The final departing origin consumer cancels and joins its backend.

Each exclusive producer records a fully received, validated ingress exactly
once before publication or result handoff. Later consumer cancellation cannot
lose this event; two fully completed hedge contenders count as two transfers.
Partial/invalid responses are excluded, and compatible fetch results marked
Coalesced do not count again. Each successful buffered miss
records one latency from its first admission attempt through waiting, retries
and publication, even if a waiter takes over as leader. Joined reads use the
same block source without adding another transfer.
Block, owned-adapter and origin admission have a finite lifetime captured from
fetch.FlightLimit at construction (default ten minutes, shortest exposed nested
Coalescing limit). Expiry adds fetch.FlightGrace. A stale call stops admitting
new readers; live waiters can retry, while the original owner continues joining
cleanup before releasing its buffer. Shared origin contexts expire at the limit;
inline leaders keep their caller's I/O context and their own cleanup obligation.
Late completions cannot delete replacement entries. This bounds sharing age,
independently of peer transport timeout or total client Range duration.
This
coalescing covers `Serve`/`PeerSource` block loads and their origin fallback.
The lifecycle and whole-block pipeline contract is recorded in
[ADR-0010](./adr/0010-coalesce-directed-block-loads.md).

**Block-length validation.** A block's byte length is fixed by the object
geometry (a full `BlockSize`, or a shorter final block). Before a fetched block
is cached, its length is compared against that expectation, on **both** the
origin and the peer paths:

- an origin block of the wrong length fails the read (it is never cached);
- a peer that returns a wrongly-sized block is not trusted — the read falls
  through to origin, which is authoritative;
- peer candidates are checked by `Client.GetInto` (or compatibility
  `Client.GetValidated`) before recording success.
  A wrong length immediately quarantines that parent under the request's captured
  breaker generation. No peer bytes are published downstream until the complete
  block has been obtained and validated; failure may try a backup or origin.
  An unresolved object size fails before fetching/forwarding the block.

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
drops the Range header and pulls the whole object). `PeerSource` declines such requests (`held=false`, recorded as a refused
relay) **before** cache lookup, relay selection, size probe, or origin I/O.
Declined, not errored: the malformed index is the requester's fault, and a 500
would charge this healthy relay on the requester's circuit breaker.

**Origin/key consistency is checked at the same boundary.** When a request
supplies `X-DART-Origin`, the source recomputes its expected chunk key from
`(namespace, chunk.ObjectID(url), ChunkOfBlock(blockIndex))`. A different key
is a requester addressing error and is declined before local cache lookup,
parent selection or any origin probe. This prevents an honest parent serving
a requested cached block from being blamed for a different URL's geometry.
Requests without an Origin URL remain valid cache-only reads; a cold one
cannot relay. Identity checking takes O(URL length) time and space, adds no
network traffic.

### 3.5.1 `func (e *Engine) PeerLeaseSource() peer.LeaseSource`

The node wires this source into `peer.Server.Borrow`. It has PeerSource's key,
URL, geometry, hop, credential and error rules, but transfers a `store.Lease`
that the server releases after writing. Even error/decline results transfer any
returned lease to the caller. Memory hits avoid full-block copies. Supported
memory refills reserve an exclusive slot, receive directly, validate and commit;
failed fills abort without visibility. Shared cold reads retain the same bytes.
Disk and Hybrid preserve their compatible write/promote path.

A cache slot smaller than the requested block also selects an uncached temporary
buffer; cache geometry never turns a valid origin read into a failure. A full
pinned/filling arena waits briefly with cancellation, then uses a temporary
buffer and skips cache publication. Admission rejection has the same fallback.
The exceptional path may allocate/copy. `fetch.IntoFetcher` permits direct origin
fills; unsupported fetchers retain validation and copy into the receive buffer.
Neither cache admission nor pressure may fail a read that already has valid bytes.

`ReceiveBytes` / `ReceiveLoads` independently limit temporary block receive
storage for both origin and peer fills, including hedges and fills for disabled
cache classes. Zero options select defaults of `max(256 MiB, BlockSize)` and 64;
negative limits or an explicit byte limit smaller than a block are rejected.
These are capacity guardrails, not benchmark-derived throughput tuning. Capacity
is acquired before allocation and stays charged through the final result lease,
including shared readers and slow HTTP writers. Arena reservations use their
cache capacity instead. Range-ignored results retain the original unpublished
slot or temporary lease without making a second payload copy.

Demand admission waits at most 2 ms (subject to scheduling and earlier context
cancellation), then returns `ErrReceiveBudget` if still full. It never waits
indefinitely while another peer may depend on this node. Local capacity failure
occurs before contacting a source; relay reporting uses the existing source-failure
response, without quarantining an honest sender. HTTP handlers abort an already
committed response on failure, as for other block errors. Completed valid results
are delivered even if all capacity becomes occupied afterward. To sustain more
concurrent uncached delivery, configure larger receive limits.

Local speculative fills never wait for temporary capacity and leave one load and
one full block of headroom for demand. A skipped fill is retried as demand when
its turn arrives; a foreground caller joining that speculative flight retries
with demand admission. Other concurrent demand or incoming peer loads can still
exhaust the budget. These limits bound live engine-owned receive payloads, not
RSS, buffered copies returned by legacy APIs/custom fetchers, disk-cache hit
copies, transport buffers or garbage awaiting collection. Cache slot capacity
and prefetch reservations remain separate bounds.

### 3.6 Bounded ordered Range prefetch

`Serve` schedules only blocks covering the clamped request, nearest first, and
writes requested subranges strictly in order. `PrefetchWindow` counts the current
block and queued/in-flight later blocks. Window 1 serializes loads. Peer block
handlers fetch exactly one requested block and never expand a prefetch window.

The engine's `PrefetchBytes` and `PrefetchLoads` admission is non-blocking.
Reservations cover each speculative block's expected payload through consumption,
including completed results held behind a slow client. On exhaustion, a foreground
block still loads without a speculative reservation. It remains subject to
temporary receive admission described in §3.5.1. These prefetch limits bound
speculative payload/results, not total process RSS:
foreground reads, caches, transport buffers and temporary validation/singleflight
copies have separate memory costs. A block larger than the byte budget is read on
demand. Existing cache hits and same-block flights are reused, and fetched blocks
are validated and published best-effort before use even if admission declines.

Each Serve owns its workers: cancellation, an earlier block failure or a write
failure cancels and joins all workers before returning. A later failed block is
reported only when reached, after valid earlier blocks have been written. Already
published valid cache entries remain. Reserving results under backpressure costs
O(window) metadata and at most the admitted speculative payload; range length does
not allocate a segment/result per block. Hedge contenders are canceled and joined before the block load returns, and
origin coalescing cancels/joins when its last caller leaves. Shared work with a
surviving caller remains owned by that caller. Extra hedge bandwidth retains its
independent rate limit; backend cancellation must cooperate for prompt exit.

### 3.7 Cache classification (owned / borrowed)

When the configured `Store` also implements `store.ClassStore` (i.e. a
`store.Tiered`), the engine classifies every block it caches:

- **owned** — this node is in the HRW top-`Replicas` for the chunk over root-eligible
  Ready members. That is the authoritative placement, independent of who is reading.
- **borrowed** — anything else: a copy fetched for a local client, or relayed for
  a peer.

This keeps the two budgets isolated so borrowed churn cannot evict the owned
shard set (see docs/store.md §3.4). Without P2P there is no placement, so
everything cached is owned. Insertion stays best effort: a full or
admission-rejecting cache never fails a read whose bytes are already in hand.

### 3.8 Tail-latency hedging

A `404` or transport error permits fallback toward ancestors and eligible supply
roots. A peer can also be alive but slow (GC pause, disk hitch, or waiting for its
parent), allowing a stall to propagate through the subtree.

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
   without rate limiting** (definitive source refusals and local receive-budget
   exhaustion still stop new attempts);
4. take whichever answers `held=true` first and **cancel the loser**;
5. if neither can serve it, try bounded eligible supply-root candidates. Only a
   root serving supply fetches origin; exhaustion fails the read.

The hedge delay comes from an online estimator (exact p99 over a bounded window of
recent fetches), floored at 5 ms so a fast cluster does not hedge on noise and
capped at 2 s so a pathological sample cannot disable hedging. Banked hedge credit
is capped at one, so an idle period cannot release a burst.

Separately, `peer.Client` bounds every individual peer request with **three**
timeouts (dial 1 s, response header 10 s, request 30 s — see docs/peer.md §3.3), so
a stalled or departed peer can never hold a read open until the caller's context
expires. And when a `peer.Breaker` is configured, target selection **skips peers
whose circuit is open** by walking toward usable ancestors and roots. Each
attempt validates the full block before handing it to the requester. Reported
source failures do not quarantine an honest relay; malformed data does. If no
ancestor or supply-root candidate can serve, the read fails. The candidate
sequence shares `fetch.FlightLimit(Fetcher)` (default ten minutes), in addition to each
peer's timeout and the caller's earlier deadline. A hedge that cannot reserve a
receive buffer has not contacted its target and does not mark it attempted.
After contenders exit and release capacity, fallback may contact that untried
root. Attempt accounting is synchronized across hedge workers.

Together these bound abrupt node death: the dial timeout makes the first affected
read fail in ~1 s, a dial failure opens the circuit on that single observation, and
every later read skips the departed peer entirely.

`NewMetrics(r *metrics.Registry) *Metrics` allocates and registers the
engine's metric set; `Engine` accepts it via `Options.Metrics` (a nil one
disables engine metrics). Metrics: `dart_hedge_total{event=fired|primary_won|backup_won}` and
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
GET) is answered with `200` instead of `206`, that origin endpoint is marked
Range-unsupported (process-local, never expires — a restart re-probes), and
`Handler` then serves requests for that endpoint with `ServePassthrough`.
Another endpoint for the same digest can still use the cache and P2P:


- the client's `Range` header is forwarded, and the origin's status, entity
  headers (`Content-Type`/`Content-Length`/`Content-Range`/`ETag`/
  `Last-Modified`/`Cache-Control`/`Accept-Ranges`) and body are streamed back
  **verbatim** — the client sees exactly what talking to the origin directly
  would produce;
- nothing is cached and passthrough traffic is not coalesced. Non-root peers
  delegate the operation to a supply root without using the block tree;
- a `HEAD` is served from an upstream GET (a presigned URL is signed for GET
  alone), forwarding headers with an empty body;
- an error is reported (as a clean `502`) only before the response is
  committed; afterwards a failure aborts the HTTP response, including the
  terminating chunk for unknown-length responses, so truncation is detectable.

`ServePassthrough` streams through the fetcher's `fetch.Opener` interface
(`HTTPFetcher` and `Coalescing` implement it); with a fetcher that cannot
stream it fails cleanly instead of falling back to per-block pulls. Non-root
engines install a delegated opener independently of the local fetcher's streaming
capability. On the
peer plane, a relay that has marked an object Range-unsupported **declines**
relay requests for it (`held=false`) on both relay sources
(`PeerSource`), so the requester uses its own
passthrough path rather than this node pulling the whole object per block on
its behalf. Defense in depth: `block()` never caches bytes sliced from a
Range-ignored whole-object response even when the marker is absent.

Metrics: `dart_passthrough_total{reason="range_unsupported"}` counts proxied
requests; their bytes are counted as `client` plus `origin_in` for a root-capable
node, or `client` plus `peer_in` for non-root delegation. They do not count as
block sources. A supply root records its proxied response bytes as `origin_in`.

## 4. Invariants & Guarantees

1. **Correct bytes**: `Serve` returns exactly `content[start:end+1]`, across
   block and chunk boundaries.
2. **Read-through + cache**: a block is fetched from origin at most once per
   coalesced flight and zero times more *while cached*. Two qualifications:
   insertion is best-effort — under cache pressure a block may be evicted
   before a later read, which then re-fetches it. Engine origin lease flights
   coalesce concurrent reads even with a custom Fetcher. Bytes sliced
   from a Range-ignored whole-object 200 are served but never cached (§3.9).
3. **Non-chunked responses on the block path**: client reads served from the
   block engine always use Content-Length framing. Verbatim passthrough of a
   Range-blind origin forwards the origin's own framing, chunked included.
   An empty object is a valid `200` with `Content-Length: 0`; a Range request
   on it is `416` (`bytes */0`), suffix ranges included.
4. **Range semantics**: standard `200`/`206`/`416` with correct `Content-Range`.
5. **No invalid geometry on the relay path**: after probing, an uncached block
   starting at or beyond the object size is declined before scheduling a worker
   or allocating its receive buffer. Signed-overflow indices
   (`> chunk.Config.MaxBlockIndex()`) are declined before cache lookup,
   relay selection, size probe or origin I/O, preserving the issue #52 geometry
   guarantee (docs/fetch.md §3.4).

6. **No requester identity confusion on the relay path**: when an Origin URL
   is supplied, its namespace/object/chunk key must match the requested key.
   Inconsistent requests yield a clean decline, even if the local cache holds
   that key; requester errors never trigger parent fetches or quarantine.

## 5. Concurrency & Call Permissions

- `Engine` is safe for concurrent use: the size cache is mutex-guarded, the
  store is concurrency-safe, and buffered P2P misses are coordinated by
  block/ordering-epoch through cache publication, separately from terminal supply.
  Origin lease flights independently share across both modes and epochs;
  traditional Coalescing.Fetch remains available for owned callers. Verified with `-race` under overlapping `Serve`.
- Shared buffered loads keep store access in the leader caller's lifetime;
  Serve-owned prefetch workers are canceled and joined before Serve returns.
  Serve and PeerLeaseSource release immutable block leases after writes. The
  compatibility PeerSource retains owned copies. Waiters do not keep a goroutine
  after cancellation; the last origin waiter joins shared backend cleanup
  (ADR-0003 and ADR-0010).
- `Handler` is safe for concurrent use if `Resolve`/`E` are not mutated after
  construction.

## 6. Determinism / Stability Contract

- ADR-0011 defaults the preferred root to one child and descendants to two.
  Explicit equal degrees preserve the uniform tree. Changing degrees preserves
  the strict HRW dependency order; no flight key or membership epoch change is
  introduced. Consistent configuration is required for a consistent tree.
- ADR-0010 specifies complete validated blocks before peer success responses and
  bounded in-range prefetch. The removed streaming internal APIs are not retained.
  ADR-0009 introduces v2 paths and roles; ADR-0010 carries the ordering epoch
  on block requests. Keys, hop limits, credentials and HTTP/1.1 framing remain; cold multi-hop first-block latency includes full transfer per hop.


- The block key uses `chunk.ChunkKey` (part of the wire protocol via placement),
  and the engine supplies hop, supply and ordering-epoch wire fields.
  HTTP Range/Content-Range is the external
  client contract.
- Buffered coalescing preserves geometry and key construction; caller ownership and cancellation follow §3.3 and ADR-0010.

Managed-memory serving preserves whole-block validation and the existing HTTP
wire contract. Borrowed results require explicit release; legacy owned-copy APIs
remain compatible. Prefetch reuses a fixed ring and worker set per request;
queued/error/unused results release pins and speculative budgets before exit.

## 7. Testing

Managed-memory regression coverage:

| Test | Property |
|---|---|
| `TestDelegatedSizeScopesRangeCapabilityToOrigin` / `TestConcurrentDelegatedSizeProbes` | real HTTP metadata roots preserve per-endpoint Range state, independent concurrent probes and cross-origin digest reuse |
| `TestDelegatedProbeRecoveryAndCancellation` | failed/canceled delegated probes terminate and recover; successful results remain cached |
| `TestRootOnlyWarmBlocksIgnorePlacement` | both peer APIs serve valid warm blocks after ownership changes, including cache-only requests, while invalid identity and cold non-owner relay still decline |
| `TestRootFallbackAfterHedgeAdmissionDeclines` | arm the hedge only after the parent holds the sole receive buffer; after rejected hedge admission and parent 404/source failure, fallback contacts the healthy root exactly once |
| `TestRootHedgeCanWinBeforeParentAdmission` | hold the parent before receive admission; the root hedge wins the sole buffer, returns correct bytes, and cancels the parent without any rejected admission or leaked capacity |
| `TestCrossHopReadersShareOnePeerTransfer` | R-hot/A-B-cold real HTTP overlap: one 4 MiB R-to-A payload across hops, with cache refusal, canceled waiter and last-lease release |
| `TestReaderSnapshotChurnPreservesStrictOrder` | missing self, duplicate IDs, changing reader/member subsets and both fanout values preserve strict normal parent/backup order |
| `TestRootFanoutOptions` | root=1 and descendant=2 defaults, explicit degrees and negative root rejection |
| `TestRootFanoutHTTPDistribution` | concurrent cold readers below a hot root; actual HTTP payload counts match asymmetric degrees with root defaults and explicit 1/2/3, correct bytes and no origin reads |
| `TestNormalFlightCanReceiveFallbackSupply` | A waits on B while B falls back to supply from A; cache decline does not create a wait cycle |
| `TestRoutingEpochSkewKeepsCacheAndSupplyUsable` | mismatched cold relay declines before metadata/flight, while terminal supply and warm bytes remain available |
| `TestRoleWeightSkewDeclinesBeforeFlight` | opposing membership weights decline cold relay before admission, recover through terminal supply and release buffers |
| `TestZeroClassBudgetsDeliverOverP2P` | a full multi-block Range crosses two nodes with both serving cache classes disabled and one temporary buffer each |
| `TestRootCandidatesShareAttemptDeadline` | root fallback cannot restart the block-flight deadline; expiry cancels active HTTP and releases buffers |
| `TestRolePlacementAndTreeGolden` | independent weighted mixed-role owners/trees, 25 cases, membership/reader permutations, fanout 1/2/4, no snapshot and sole-reader cases |
| `TestRoleGoldenAgainstPythonReference` | `DART_GOLDEN_REF=1` regenerates and checks `testdata/roles.json` with `contrib/golden/role_reference.py`; CI runs this cross-check |
| `TestReceiveBudgetTracksSharedDelivery` | disabled-class concurrent readers share one refill; final lease returns capacity; exhaustion prevents source I/O and updates metrics |
| `TestReceiveBudgetPrefetchDemandProgress` | one-buffer byte/load limits preserve ordered long-Range delivery without repeated source transfers |
| `TestReceiveBudgetFailureAndCancellation` | failed/truncated/canceled fills and failed writes release capacity |
| `TestReceiveBudgetRangeIgnoredRetainsOneBuffer` | range-ignored results retain one accounted payload without publication |
| `TestReceiveBudgetAdmission` | independent byte/load caps, reserved demand headroom, cancellation, bounded pressure failure and invalid options |
| `TestDemandRetriesSkippedSpeculativeFlight` | borrowed, owned-block and PeerSource demand sharing a declined speculative flight retry with demand admission |
| `TestBorrowedOriginUsesReceiveSlot` | receive/cache/sender share one payload; zero warm block allocation |
| `TestBorrowedPressureBypassesPublication` | correct subrange and temporary accounting with a pinned full arena |
| `TestBorrowedWriteFailureReleasesPins` | all queued/current pins and speculative reservations released on client error |
| `TestLeasedFlightSharesOneImmutableBuffer` | joined readers share a pointer with independent lease owners |
| `TestPeerDeclinesBlocksOutsideObject` | key-consistent past-end blocks decline in memory/disk/Hybrid before any worker or allocation |
| `TestOversizedCacheSlotFallsBackToOrigin` | smaller cache slots do not fail valid origin reads |
| `TestExpiredEngineFlightsAllowFreshAdmission` | nested MaxFlight expiry replaces both outer/origin admission while original owners join; late completion preserves the replacement, including owned and cross-depth reads |
| `TestCanceledEngineLeaderDoesNotBlockReplacement` | new engine requests progress while the canceled original handler joins delayed cleanup, for borrowed and owned adapters |
| `TestInlineLeaseWaiterCanReplaceCanceledCleanup` | an already joined inline waiter retries before the original cleanup completes |
| `TestInlineLeaseFlightRetriesCancellationAndDifferentURL` | canceled leader retry, renewed URL retry, and shared same-URL backend failures including wrapped context errors |
| `TestBackendTimeoutSharedByEngineWaiters` | actual MaxFlight timeout is shared by live borrowed/owned callers without starting successive same-URL flights |
| `TestAbandonedOriginLeaseFlightStopsAdmission` | new callers start fresh while abandoned backend cleanup is joined |
| `TestSharedLeaseFlightCancellationOwnership` | survivor ownership and final-caller cancel/join |
| `TestCompletedOriginIngressSurvivesCanceledHandoff` | completed origin bytes survive canceled inner/outer handoff for borrowed/owned reads, with or without a surviving reader |
| `TestCompletedHedgeIngressCountsBothTransfers` | two validated peer contenders count both ingress payloads while one serving read succeeds |
| `TestBorrowedPeerHTTPAndWarmReuse` | actual borrowed peer HTTP path into memory budgets and warm reuse |


- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **91.9%** of statements (Go 1.27.0). Uncovered lines are mostly rare I/O
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
| `TestConcurrentSubBlockPeerMissesSharePublication` | four 1 MiB subranges share one 4 MiB peer transfer through blocked cache publication, including declined admission; bytes and per-read/wire metrics remain correct |
| `TestBlockLoadsCancellation` | a waiter cancels independently and a live waiter takes over after leader cancellation |
| `TestBlockLoadsRetryFailuresWithOwnURL` | failures do not poison different credentials/endpoints and completed failures are not retained |
| `TestBlockLoadsSeparateKeysSupplyAndEpoch` | distinct blocks, terminal supply and ordering epochs remain independent |
| `TestBlockLoadsDetachLeaderBytes` | caller-owned leader bytes do not alias the waiters' immutable snapshot |
| `TestBlockLoadRechecksCacheAfterAdmission` | a caller with an older miss sees another load's publication before doing new I/O |
| `TestBlockLoadPanicReleasesAdmission` | an unwinding source cannot pin the flight entry |
| `TestCoalescedPeerRefusalRetriesCallerCredential` | a renewed credential survives an expired leader's peer/origin refusal without quarantining the parent |
| `TestCrossHopLoadsReturnOwnedBytes` | cross-hop callers share normal loads while receiving independent mutable copies; origin wire bytes count once |
| `TestBlockLoadAlreadyCanceled` | an already-canceled caller creates no new flight |
| `TestCoalescedBlockValidatesBeforeSharingOriginFallback` | short peer data is rejected; waiters share only the validated origin fallback and wire bytes count once |
| `TestServeAcrossBlocksAndChunks` | a range crossing block/chunk boundaries is assembled correctly |
| `TestServeEndClampAndBadStart` | end clamped to size; start beyond size errors |
| `TestSize` | object size probed correctly |
| `TestSizeScopesRangeCapabilityToOrigin` | both probe orders and known/unknown totals stay origin-scoped; refreshed signatures reuse metadata and digest blocks remain shared |
| `TestConcurrentSizeProbesDoNotShareCapabilities` | a blocked Range-blind endpoint does not delay or contaminate an independent endpoint probe |
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
| `TestWholeBlockRelayChainThroughEngines` | 3-node fanout=1 chain over complete-block peer servers: bytes intact, every node cached |
| `TestRelayDeclinesInconsistentKeyURL` | buffered/streaming, warm/cold relays decline unrelated URLs, wrong chunk indices and namespaces without contacting origins/parents or tripping circuits; valid and cache-only reads still work |
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
| `TestRelayParentErrorFallsBackToOrigin` | an open-circuit/errored parent is skipped to origin, not propagated as a 500 |
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

| `TestRangePrefetchOrderedAndBounded` | out-of-order loads produce ordered exact subranges; covering blocks cache, nothing beyond the request loads |
| `TestRangePrefetchBackpressureAndDemandProgress` | queued results retain global byte/load reservations while an unrelated demand progresses |
| `TestRangePrefetchHonorsGlobalLimits` | independent byte/load exhaustion and oversized blocks preserve demand and release all reservations |
| `TestRangePrefetchFailureIsObservedInOrder` | an ahead failure preserves the valid preceding bytes and never caches failed data |
| `TestRangePrefetchCancelAndJoin` | cancellation/write failure cannot return before all workers exit |
| `TestRangePrefetchDisabledAndShortWrite` | window 1 fetches serially and short downstream writes are errors |
| `TestRangePrefetchSharesDemandFlight` | an in-flight prefetched block and concurrent demand share one peer transfer |
| `TestRangePrefetchKeepsPublishedCacheOnCancel` | completed valid prefetched blocks survive client cancellation; incomplete blocks do not enter cache |
| `TestPrefetchOptions` | invalid limit values are rejected |
| `TestWholeBlockRelayCommitsOnlyAfterValidation` | real partial parent responses commit no downstream byte before complete validation; short/failed parents fall back cleanly |
| `TestWholeBlockRelayReportsCleanFailure` | an unrecoverable source failure yields a clean 503 without partial block/cache publication |

| `TestRangePrefetchReservesUntilOriginFlightExits` | a canceled origin flight retains speculative admission through underlying cleanup |
| `TestHedgedFetchCancelsAndJoinsContenders` | cancellation and a hedge win join all canceled contenders before returning |
| `TestWarmPeerReadRecordsCacheHit` | a warm HTTP peer read records exactly one cache hit without peer/origin traffic |

## 8. Limitations & TODO

- **P2P tree wired**: a miss routes to the parent in the preorder tree; relay
  nodes fetch-on-behalf and cache, offloading the owner; the root fetches origin.
  Normal reader edges are strictly ordered; cold epoch mismatches use supply
  fallback. `X-DART-Hop` caps forwarding. Nodes validate whole blocks before
  relaying. Long client ranges overlap different blocks using bounded prefetch.
  The owner-rooted tree spans eligible readers plus self when configured,
  otherwise all Ready eligible readers. Epoch checks do not synchronize discovery.
- **Range-unsupported origins are proxied, not cached** (§3.9): nothing about
  them is learned beyond the mark itself (no TTL, no re-probe until restart),
  and their traffic bypasses the P2P tree entirely.
- **Readahead scope**: prefetch covers only the current request. Adaptive windows,
  cross-request fairness and reading beyond the requested range are not provided.
- **Streaming vs buffering**: blocks are validated whole before sending, using
  pinned cache leases or bounded temporary buffers. HTTP transport copies remain.
- **Size cache lifetime**: endpoint probe results are cached for process life with no TTL —
  correct for immutable objects; mutable objects need the policy layer.
- **Mid-stream errors**: once headers are sent, a block error can only truncate
  the body (client detects via Content-Length); prefetching the first block
  before sending headers could turn some of these into clean error statuses.
