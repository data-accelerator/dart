# `internal/tracker`

The per-file **active reader set** `S`: which nodes are currently reading a given
object. The distribution tree is built over `S` instead of over all members.

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
- **Tick freeze** — the set is recomputed only on a fixed tick, so topology (and
  `epochS`) is stable between ticks and peer connections do not churn.
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
| frozen set | The published reader list, recomputed on a tick and sorted by node ID. |
| `epochS` | Bumped only when the frozen set actually changes. |

## 3. Public API

### 3.1 `Registry`

```go
func NewRegistry(Options) *Registry           // Options{Tick, LeaseTTL, IdleGrace, Now}
func (r *Registry) Join(file, node string, ttl time.Duration) JoinResponse
func (r *Registry) Leave(file, node string)
func (r *Registry) Readers(file string) ([]string, uint64)
func (r *Registry) Files() int
```

- `Join` records or refreshes interest and returns the currently **frozen** set
  (`JoinResponse{EpochS, Readers, TTLMs}`). `ttl <= 0` uses the registry default;
  `ttl > MaxLeaseTTL` (10m) is **clamped** to `MaxLeaseTTL`, and the granted
  `TTLMs` always reflects the clamp. Leases are meant to be refreshed on a
  seconds cadence; without the bound a caller could pin a dead reader — and with
  it the whole file entry, blocking idle eviction — for an arbitrary duration.
  On the HTTP wire, `ttlMs` values too large for a `time.Duration` are clamped
  *before* conversion, so they can never overflow-wrap into a silently wrong
  lease.
- The first reader of a file is published immediately (it would be useless to
  make the first reader wait a whole tick); subsequent changes wait for the tick.
- `Leave` drops a lease immediately; the frozen set updates on the next tick.
  Dropping the last reader forgets the file entirely.
- A lease expires at its TTL deadline regardless of further Joins — expiry is
  not "until the next Join". Enforcement is **lazy**: the sweeper runs on
  registry activity (amortized to at most once per tick; there is no
  background goroutine), so an expired lease in an untouched registry lingers
  until the next call — harmless, because only live readers shape the tree.
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

- **Tick freeze**: the published reader set changes only at tick boundaries
  (recomputation is lazy — on activity, at most once per tick; there is no
  background goroutine). Between ticks the topology and `epochS` are stable,
  so TCP connections to readers do not churn.
- **`epochS` bumps only when the frozen set actually changes** — joins that
  refresh an existing lease, and leaves of absent readers, never bump it.
- **Deterministic wire form**: the frozen set is sorted by node ID; every
  observer of the same registry state reads the same epoch and reader list.
- **Membership by liveness only**: a reader is in the set iff its lease is
  unexpired at freeze time. `Leave` deletes the lease immediately; for a file
  with remaining readers the published set follows at the next freeze, but
  removing the **last** lease deletes the whole file entry, so `Readers` then
  reports `(nil, 0)` without waiting for a tick (the tick-freeze guarantee
  covers joins and lease expiries, not this deletion edge). An idle entry is
  forgotten after `IdleGrace`.
- **No collision with placement keys**: the file key uses the sentinel chunk
  index -1, which no real chunk's `ChunkKey` input can carry.
- **Client-supplied TTLs are clamped** to the configured lease range;
  arithmetic is duration-overflow guarded.

## 5. Engine integration

`engine.Options.TrackerRegistry` (this node's local tracker) and
`TrackerClient` (to reach remote trackers) enable the feature; leaving both nil
keeps all-member routing. The engine then:

1. computes the file key and the HRW tracker for it;
2. JOINs (locally if it *is* the tracker, else over HTTP) and caches the reader
   set for ~2s, so a read does not make a tracker call per block;
3. ranks the reader set for the chunk and takes its tree parent from that.

**Fallbacks — the read never fails because of a tracker:** if the tracker is
unreachable, or the reader set has fewer than two usable members, routing falls
back to all Ready members.

## 6. Concurrency & Call Permissions

- `Registry` is safe for concurrent use (single mutex); `Readers`/`Join` return
  copies, never internal slices. Verified with `-race`.
- `Client` is safe for concurrent use.
- The engine's reader-set cache is mutex-guarded and holds only IDs.

## 7. Stability Contract

- The JSON shapes and paths (`/tracker/v1/join`, `/leave`) are the tracker wire
  protocol; changing them is a protocol change.
- The frozen set is **sorted**, so every node derives an identical tree from the
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
| `TestTickFreeze` | a join within a tick does not change the published set or epoch; the next tick does |
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
| `TestTreeNodesPrefersReaderSet` | tree candidates are exactly the readers, not all Ready members |
| `TestTreeNodesFallsBackToAllMembers` | no tracker, or a single reader, falls back to all-member routing |
| `TestReaderSetTreeEndToEnd` | 3 nodes sharing one tracker all read correct bytes |

## 9. Limitations & TODO

- **Tracker liveness**: a tracker that dies is replaced by HRW on the next
  membership change, and `S` rebuilds from renewals — but in-flight reads during
  the gap fall back to all-member routing rather than retrying.
- **No epochS-driven invalidation**: the engine caches the reader set for a fixed
  ~2s rather than reacting to an `epochS` change pushed by the tracker.
- **No append-only mode**: the design's option to add new readers as leaves and
  only rebalance on a tick boundary is not implemented; every tick may reshape the
  tree tail.
- **Memory**: bounded by **idle eviction** (§3.1): a file with no live leases
  and no activity for `IdleGrace` is forgotten. There is deliberately no hard
  file cap — a hard cap would evict genuinely active files under load.
