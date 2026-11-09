# `internal/tracker`

The per-file **active reader set** `S`: which nodes are currently reading a given
object. Below the fixed chunk owner, the distribution tree is built over `S`
instead of over all members.

- Source: `internal/tracker/tracker.go`, `internal/tracker/client.go`
- Tests: `internal/tracker/tracker_test.go`, `internal/tracker/eviction_test.go`
- Import path: `github.com/data-accelerator/dart/internal/tracker`

## 1. Overview

Building the tree over **all** Ready members means a node's parent may be a
member that is not reading the object at all, so it has to fetch-on-behalf just
to pass bytes along. Building it over the **readers** makes every parent a node
that actually wants the data (it either holds it or is already fetching it),
which is the design's active-reader-set optimization (docs/hashring.md §2/§3).

Three properties make this safe and cheap:

- **Leases** — a reader JOINs with a TTL and refreshes; when it stops reading the
  lease lapses and it drops out of `S`, shrinking the tree. `S` is soft state, so
  a tracker restart self-heals as readers renew.
- **Immediate growth, batched departures** — JOIN publishes new readers before
  replying. Renewals leave the set and `epochS` unchanged; removals and expiry
  are collected on a tick (ADR-0012).
- **Control plane only** — small JSON messages; no block data flows through a
  tracker.

Which node is the tracker for a file is **not** stored anywhere: it is the HRW
top-1 Ready member for the file key, so every node computes the same answer
independently — **given the same membership view**. Under split-brain
(divergent member sets) two nodes can elect different trackers for one file,
and the reader sets (hence the distribution trees) diverge silently until the
views reconverge; the epoch handshake is what detects that divergence. This
package therefore holds no placement logic.

## 2. Concepts

| Term | Meaning |
|---|---|
| file key | `chunk.ChunkKey(ns, objectID, -1)` — a sentinel index so it never collides with a real chunk's placement key. |
| lease | A reader's membership deadline; refreshed by re-JOIN. |
| published set | The sorted reader list: grows during JOIN, shrinks on a tick. |
| `epochS` | Bumped only when the published set actually changes. |

## 3. Public API

### 3.1 `Registry`

```go
func NewRegistry(Options) *Registry           // Options{Tick, LeaseTTL, IdleGrace, Now}
func (r *Registry) Join(file, node string, ttl time.Duration) JoinResponse
func (r *Registry) Leave(file, node string)
func (r *Registry) Readers(file string) ([]string, uint64)
func (r *Registry) Files() int
```

- `Join` records or refreshes interest and returns the currently **published** set
  (`JoinResponse{EpochS, Readers, TTLMs}`). `ttl <= 0` uses the registry default;
  `ttl > MaxLeaseTTL` (10m) is **clamped** to `MaxLeaseTTL`, and the granted
  `TTLMs` always reflects the clamp. Leases are meant to be refreshed on a
  seconds cadence; without the bound a caller could pin a dead reader — and with
  it the whole file entry, blocking idle eviction — for an arbitrary duration.
  On the HTTP wire, `ttlMs` values too large for a `time.Duration` are clamped
  *before* conversion, so they can never overflow-wrap into a silently wrong
  lease.
- Every new reader is published before its JOIN returns. A renewal of an already
  published reader does not rebuild the set or change `epochS` unless a due tick
  also removes other readers. Growth does not postpone the removal tick.
- `Leave` drops a lease immediately; the published set updates on the next tick.
  Dropping the last reader forgets the file entirely.
- A lease expires at its TTL deadline regardless of further Joins — expiry is
  not "until the next Join". Enforcement is **lazy**: the sweeper runs on
  registry activity (amortized to at most once per tick; there is no
  background goroutine), so an expired lease in an untouched registry lingers
  until the next call. A published departure may linger until the next tick;
  stale parents are handled by normal forwarding/fallback.
- `Readers` returns a **copy**; `Files` is a diagnostic count.
- **Idle eviction**: a file with no live leases (readers vanished without a
  `Leave`) and no `Join`/`Readers` activity for `IdleGrace` is forgotten, so a
  many-file workload cannot grow the registry without bound. Sweeps are lazy —
  driven by registry activity, at most one O(files) scan per tick — because an
  idle registry grows nothing. Eviction is safe: a later `Join` simply
  recreates the entry (at the price of an `epochS` reset).
- Defaults: `DefaultTick = 3s`, `DefaultLeaseTTL = 2 * DefaultTick`, `MaxLeaseTTL = 10m` (a reader that
  renews once per tick is never dropped spuriously),
  `DefaultIdleGrace = 1m`. `Options.Now` injects a clock for tests.

### 3.2 HTTP

```
POST /tracker/v1/join    {"file","node","ttlMs"} -> {"epochS","readers","ttlMs"}
POST /tracker/v1/leave   {"file","node"}         -> 204
```

The join body is `JoinRequest{File, Node, TTLMs}`: `File` is the opaque object
key, `Node` the reader's stable cluster ID, and `TTLMs` the requested lease in
milliseconds — 0 means the tracker default, and client-supplied values are
clamped to the configured range (see §2; duration-overflow guarded).

```go
mux := (&tracker.Server{R: reg}).Handler()   // serve
c := tracker.NewClient()                     // 2s timeout: control plane fails fast
resp, err := c.Join(ctx, addr, file, node, 0)
err = c.Leave(ctx, addr, file, node)
```

Non-POST is `405`; malformed JSON or missing `file`/`node` is `400`. Bodies are
capped at 64 KiB.

## 4. Invariants & Guarantees

- **Immediate growth**: a successful JOIN includes its reader in the published
  set, without waiting for a tick. Renewal of an existing reader changes neither
  the set nor `epochS` unless a due removal sweep changes it.
- **Batched departures**: removal/expiry recomputation is lazy, at most once per
  tick. No background goroutine runs, and new readers do not delay that sweep.
  `Leave` deletes the lease immediately; deleting the last lease forgets the
  file, so `Readers` reports `(nil, 0)` without waiting for a tick. An idle entry
  is forgotten after `IdleGrace`.
- **`epochS` changes only with the published set**. It is local to one file entry
  on one tracker, not comparable across trackers or entry recreation.
- **Deterministic wire form**: the published set is sorted by node ID; every
  observer of the same registry state reads the same epoch and reader list.
- **No collision with placement keys**: the file key uses the sentinel chunk
  index -1, which no real chunk's `ChunkKey` input can carry.
- **Client-supplied TTLs are clamped** to the configured lease range;
  arithmetic is duration-overflow guarded.

## 5. Engine integration

`engine.Options.TrackerRegistry` (this node's local tracker) and
`TrackerClient` (to reach remote trackers) enable the feature; leaving both nil
uses all eligible Ready readers below the owner. The engine then:

1. computes the file key and the HRW tracker for it;
2. JOINs locally or over HTTP; concurrent refreshes for the same object and
   tracker membership view share one request. On continued routing demand, the
   cache interval starts at 50 ms and doubles after each successful refresh,
   capped at half the granted lease (3 s for the default 6 s lease). There is no
   background polling; completed reads cause no further JOINs. A short grant
   caps even the first interval. Missing grants use a 2 s ceiling. The schedule
   resets after lease expiry or a changed tracker membership view, not after a
   changed reader list or failed JOIN;
3. anchors the tree at the root-eligible chunk owner, appends ranked eligible
   readers union eligible self, deduplicates them (including the owner), and takes
   its parent from that sequence. This local completion does not mutate the
   tracker snapshot or wait for a tick. With fixed member weights, snapshot
   additions/removals preserve common-node order; normal forwarding always goes
   earlier in that order (ADR-0010).
   Root-only peers supply roots but do not JOIN as readers (ADR-0009).

**Fallbacks:** a missing/unreachable tracked snapshot routes directly to the
preferred owner. A sole reader also fetches through that owner. Reader changes
do not move ownership; loss of all usable roots can still fail a cache miss.

## 6. Concurrency & Call Permissions

- `Registry` is safe for concurrent use (single mutex); `Readers`/`Join` return
  copies, never internal slices. Verified with `-race`.
- `Client` is safe for concurrent use.
- The engine's reader-set cache is mutex-guarded and publishes immutable ID slices.
  Waiting callers can cancel independently. A canceled refresh leader permits a
  live waiter to retry; an abandoned or older response cannot overwrite a newer
  installed entry. A failed JOIN is shared by its waiters and is not cached.
  Fresh hits reuse tracker selection for the same immutable membership view; a
  new view rechecks address/liveness even if its ordering epoch is unchanged.

## 7. Stability Contract

- The JSON shapes and paths (`/tracker/v1/join`, `/leave`) are the tracker wire
  protocol; changing them is a protocol change.
- Publish new readers during JOIN and batch departures on ticks (ADR-0012).
- The published set is **sorted**, so every node derives an identical tree from the
  same set — this is load-bearing and must not regress.
- `epochS` must change only when the set changes (readers rely on it to detect
  topology changes cheaply).

## 8. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **89.5%** of statements.
- **Reproduce**:

```bash
go test ./internal/tracker/ -v -count=1
go test ./internal/tracker/ -race -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestJoinPublishesFirstReaderImmediately` | the first reader is published without waiting a tick |
| `TestJoinPublishesGrowthImmediately` | every JOIN publishes growth, keeps sorted copied snapshots and leaves renewal epochs unchanged |
| `TestDeparturesWaitForTick` | joins do not postpone removal batching; rejoining a removed reader publishes immediately |
| `TestLeaseExpiry` | a reader that stops renewing drops out |
| `TestJoinClampsLeaseTTL` | a 24h lease request is granted clamped to `MaxLeaseTTL` and actually lapses then |
| `TestJoinTTLMsOverflowIsClamped` | overflowing `ttlMs` values clamp before duration conversion (no wrap) |
| `TestLeave` | explicit leave; last reader forgets the file |
| `TestReadersSortedDeterministic` | two registries with different join orders publish identical sets |
| `TestEpochStableWhenSetUnchanged` | epoch does not churn across ticks |
| `TestReadersCopyNotAliased` | callers cannot mutate internal state |
| `TestHTTPJoinLeave` | wire round trip incl. requested TTL |
| `TestHTTPBadRequests` | 405 / bad JSON / missing fields |
| `TestClientTrackerDown` | unreachable tracker surfaces an error |
| `TestConcurrent` | concurrent join/read/leave (`-race`) |
| `TestIdleEvictionAfterGrace` | a file with vanished readers is forgotten past the grace, kept within it |
| `TestIdleEvictionKeepsQueriedFiles` | querying counts as activity: a queried file is never evicted |
| `TestRejoinAfterEviction` | a Join after eviction recreates the entry and re-publishes immediately |
| `TestIdleEvictionDefaultGrace` | zero `IdleGrace` selects `DefaultIdleGrace` |

Engine-side (in `internal/engine`):

| Test | Property guarded |
|---|---|
| `TestTreeNodesPrefersReaderSet` | tree anchors at the owner and uses eligible active readers below it |
| `TestTreeNodesWithoutReaderSnapshot` | tracking disabled uses eligible readers; a tracked sole reader retains the owner |
| `TestReaderSetTreeEndToEnd` | 3 nodes sharing one tracker all read correct bytes |
| `TestTrackerStartupPublishesBeforeTick` | real HTTP payload counts prove reader P2P before the first tick |
| `TestTrackerStartupExperiment` | opt-in 1 MiB Range latency, root/reader payload and JOIN cost comparison |
| `TestReaderRefreshCoalescesAndCancelsWaiters` | one refresh per object/view, independent waiter cancellation, no background polling, one histogram observation |
| `TestReaderRefreshCanceledLeaderRetries` | a live waiter replaces a canceled leader without retaining a flight |
| `TestReaderRefreshOldTrackerCannotOverwriteNew` | a late response cannot replace a newer tracker view; tracker epochs are not globally ordered |
| `TestReaderRefreshLeaseBounds` | small, missing and overflowing grants preserve cache lease bounds |
| `TestReaderRefreshBackoffAndLeaseReset` | deterministic doubling, half-lease cap, and restart after idle lease expiry |
| `TestReaderRefreshFailureDoesNotCache` | failed JOIN is shared and a later demand can retry |
| `TestReaderSnapshotCacheHitDoesNotAllocate` | stable-view hits avoid allocation; canceled lookups return no cached readers |
| `TestReaderSnapshotCacheTracksAddressAndLiveness` | address and liveness changes invalidate selection despite unchanged epoch |

## 9. Limitations & TODO

- **Tracker liveness**: a tracker that dies is replaced by HRW on the next
  membership change, and `S` rebuilds from renewals — but in-flight reads during
  the gap route directly to the owner.
- **No pushed invalidation**: the client does not learn about later JOINs until
  its next due routing lookup. Late arrivals after backoff reaches its ceiling
  can wait up to that ceiling. A tracker restart at an unchanged membership view
  likewise rebuilds on normal renewals; there is no event-driven acceleration.
- **No append-only mode**: the design's option to add new readers as leaves and
  only rebalance on a tick boundary is not implemented; growth and departure
  publication can reshape the tree. Common-reader HRW order remains unchanged.
- **Memory**: bounded by **idle eviction** (§3.1): a file with no live leases
  and no activity for `IdleGrace` is forgotten. There is deliberately no hard
  file cap — a hard cap would evict genuinely active files under load.

### Startup measurement and tradeoffs

`TestTrackerStartupPublishesBeforeTick` uses a warm root, eight cold readers and
real HTTP traffic with tracker time held before the first removal tick. Eight
4 MiB reads deliver 8 MiB from the root and 24 MiB from readers, with eight JOINs.
The same test against tick-frozen publication delivered all 32 MiB from the root.
Anchor: `internal/engine/tracker_startup_test.go::TestTrackerStartupPublishesBeforeTick`.

The opt-in experiment reads 32 MiB per reader as sequential 1 MiB Ranges. It
serializes root service at 40 ms per 4 MiB to expose a root bottleneck; hedging
is off, prefetch window is one, and all bytes are checked. These are synthetic
loopback results, not a NIC throughput benchmark or a universal latency claim.
Median of three runs, eight readers, 256 MiB total client payload:

| Arrival pattern / refresh | Root MiB | JOINs | Aggregate Gbps | Range p95 / p99 ms |
|---|---:|---:|---:|---:|
| 1 ms burst / frozen publication + half-lease | 256 | 8 | 0.77 | 347 / 349 |
| 1 ms burst / immediate publication + half-lease | 64 | 8 | 3.04 | 89 / 104 |
| 1 ms burst / one early refresh | 36 | 16 | 4.46 | 58 / 101 |
| 1 ms burst / doubling refresh | 36 | 26 | 4.45 | 59 / 103 |
| 120 ms reverse-HRW arrivals / half-lease | 256 | 8 | 0.77 | 301 / 306 |
| 120 ms reverse-HRW arrivals / one early refresh | 176 | 16 | 1.11 | 185 / 255 |
| 120 ms reverse-HRW arrivals / doubling refresh | 136 | 32 | 1.43 | 182 / 287 |

Doubling helps readers joining after the first early refresh, at the cost of
additional requests. It does not uniformly improve p99: topology changes can
add a relay wait. In the slow-growth comparison, JOIN response JSON totals were
700 / 1433 / 3108 bytes; summed tracker-handler wall times were about
0.54 / 0.89 / 1.46 ms. Handler wall time is not CPU usage. This small experiment
does not establish tracker scalability at thousands of readers or objects.
Without the synthetic root limit, short reads often finish before 50 ms and
produce no additional refresh. The 50 ms start is an initial engineering choice,
not a measured optimum. With a 6 s grant, earliest refresh times are approximately
0, 50, 150, 350, 750, 1550, 3150, 6150 ms, then every 3 s while demanded.

Reproduce all four policies using temporary Go overlays (no source mutation):

```bash
python3 contrib/bench/tracker-startup.py --count 3
# Or only the production policy:
DART_TRACKER_STARTUP_EXPERIMENT=1 go test ./internal/engine \
  -run '^TestTrackerStartupExperiment$' -count=3 -v
```

Anchor: `internal/engine/tracker_experiment_test.go::TestTrackerStartupExperiment`.

`BenchmarkReaderSnapshotCacheHit` isolates the stable-view cache-hit path at
32, 256 and 4096 members. It reports time and allocations separately from the
network experiment; a stable hit requires zero allocations. View replacement
may allocate once to remember an unchanged tracker selection.

```bash
go test ./internal/engine -run '^$' -bench '^BenchmarkReaderSnapshotCacheHit$' -benchmem
```
