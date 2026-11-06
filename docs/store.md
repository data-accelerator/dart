# `internal/store`

DART's block cache: fixed-size blocks keyed by `BlockKey`, backed by
managed RAM arenas or preallocated disk arenas.

- Source: `internal/store/store.go`
- Tests: `internal/store/store_test.go`
- Import path: `github.com/data-accelerator/dart/internal/store`

## 1. Overview

Blocks (default 4 MiB, from `internal/chunk`) are the physical cache unit. The
**disk tier** is one preallocated file per budget split into equal-size slots,
with an in-memory index, an LRU eviction list, and a free list. Because blocks
are fixed-size and read-only, eviction is just "free a slot" — **O(1), zero
fragmentation, no compaction**.

On top of that there are two more layers, both implemented:

- the **owned/borrowed two-budget split with TinyLFU admission** (§3.4);
- an optional **in-memory hot set** plus the **hybrid** store that puts it in
  front of disk (§3.5).

Every tier is only a cache: DART is read-only, so the source of truth is always
the remote origin (registry, OSS bucket, package repo). Evicting from memory or
from disk can never lose data — it only costs a re-fetch. The tiers differ in
what they save: memory removes a disk read, disk removes an origin fetch.

## 2. Concepts

| Term | Meaning |
|---|---|
| `BlockKey` | `{Chunk uint64, Block uint64}` — the chunk and the block index within the object. |
| slot | A fixed-size region of the backing file; holds exactly one block's bytes. |
| capacity | `Slots` — the number of blocks the store can hold before evicting. |

## 3. Public API

### 3.1 `type BlockKey`

```go
type BlockKey struct { Chunk uint64; Block uint64 }
```

### 3.2 `type Store`

```go
type Store interface {
    Get(k BlockKey) (data []byte, ok bool, err error)
    GetReader(k BlockKey) (r io.Reader, n int64, ok bool, err error)
    Put(k BlockKey, data []byte) error
    Has(k BlockKey) bool
    Delete(k BlockKey) bool
    Len() int
    Close() error
}
```

`Get` returns a **copy** of the block bytes; the caller owns it and may modify
it freely. `GetReader` returns an `io.SectionReader` over the block's slot plus
its length, so a block can be **streamed out without a copy**. It refreshes LRU
recency like `Get`; a slot-backed reader requires its caller to prevent slot
reuse throughout consumption (§5). The shipped peer block path uses `Get` and
its independent copy so concurrent eviction or deletion cannot tear a response.
Store methods are safe for concurrent use; a returned slot-backed reader has
this additional lifetime precondition.

### 3.3 `type DiskStore` and `func Open(Options) (*DiskStore, error)`

```go
type Options struct {
    Path     string // backing file, created and truncated to SlotSize*Slots
    SlotSize int64  // fixed bytes per slot = max block size
    Slots    int    // number of slots = capacity in blocks
}
func Open(opt Options) (*DiskStore, error)
```

Semantics:

- `Open` validates `Options` (`ErrBadOptions` if `SlotSize<=0` or `Slots<=0`),
  creates/truncates the file, and returns an **empty** store. The directory of
  `Path` must exist and be writable.
- `Put(k, data)`:
  - `ErrEmptyBlock` if `len(data)==0`; `ErrBlockTooLarge` if `len(data) >
    SlotSize`.
  - If `k` is already present, refreshes recency and returns without rewriting
    (blocks are content-addressed/immutable — same key ⇒ same bytes).
  - Otherwise takes a free slot, or **evicts the least-recently-used** block if
    at capacity, then writes and indexes the block.
- `Get(k)` reads the block via `ReadAt`, moves it to the front of the LRU, and
  returns a fresh copy; a miss returns `(nil, false, nil)`.
- `Has` does not affect LRU order. `Delete` frees the slot for reuse (a
  `GetReader` outstanding on that slot is torn — see §5). `Len`
  returns the number of cached blocks. `Close` closes the file and is idempotent.

```go
s, _ := store.Open(store.Options{Path: p, SlotSize: 4 * chunk.MiB, Slots: 4096})
defer s.Close()
_ = s.Put(store.BlockKey{Chunk: ck, Block: bi}, blockBytes)
b, ok, _ := s.Get(store.BlockKey{Chunk: ck, Block: bi})
```

### 3.3.1 Directory lock: `func LockDir(dir string) (*DirLock, error)`

An `flock`-style guard so two nodes never share one cache directory (see §5.1).
The returned `DirLock` is released by `Close`; on non-Unix platforms it is a
no-op stub.

### 3.4 Two budgets: `Tiered` (owned / borrowed) with TinyLFU admission

```go
type Class uint8            // Owned | Borrowed; String() prints "owned"/"borrowed" for logs/metrics
type ClassStore interface {
    Store
    PutClass(k BlockKey, data []byte, c Class) (admitted bool, err error)
    Stats() TieredStats
}
func OpenTiered(TieredOptions) (*Tiered, error) // {Path, SlotSize, Slots, OwnedFraction, Memory}
var ErrBadTieredOptions error  // SlotSize <= 0, Slots < 2, or OwnedFraction < 0 or >= 1 (0 = unset, defaults to 0.8)
func (t *Tiered) Touch(k BlockKey) // record an access in the estimator without reading (used by Hybrid)
```

`Slots >= 2` is required so both budgets get at least one slot.

`Tiered` uses two independent budgets. `TieredOptions.Memory=false` opens two
`DiskStore` files. With `Memory=true`, it opens two `MemStore` instances and
ignores `Path`, creating no files or locks. Both share the class split and
TinyLFU policy (ADR-0007); RAM admission compares and evicts under the same
backend lock. Memory capacity is allocated lazily in arena segments, with compatible copied
reads and immutable existing keys as in §3.5. Closing/reopening starts empty.
`Slots >= 2` and the existing fraction validation apply to both backends.
Memory-only `Stats().MemBlocks/MemSlots` sum both budgets; in a Hybrid those
fields describe its front hot set. Capacity bounds slots, excluding metadata,
caller copies and engine in-flight buffers.

| Budget | Content | Default share | Admission |
|---|---|---|---|
| `Owned` | blocks this node authoritatively holds (HRW top-Replicas) | 80% (`OwnedFraction`) | unconditional |
| `Borrowed` | copies fetched for a local read or relayed for a peer | 20% | TinyLFU |

**Why the split is the point:** without it a burst of hot borrowed copies would
evict the owned shard set, destroying the cluster's balanced distributed cache —
the aggregate capacity placement depends on. Independent backing stores mean
borrowed churn can only ever consume its own share.

**Admission:** once the borrowed budget is full, a candidate is stored only if
TinyLFU estimates it at least as popular as the block that would be evicted, so a
stream of one-shot relayed blocks cannot wipe warm entries. A rejection is
reported as `admitted=false`, **not** an error. The estimator counts exactly the
traffic admission governs: **borrowed hits, misses, and insertions** — owned hits
are excluded because owned blocks are never evicted via the sketch, and counting
them would fire the halving cadence (sized for the borrowed budget) far too
often, decaying warm borrowed estimates to zero where a one-shot could evict
them. A `Hybrid` mem-tier hit still feeds the backing estimator via `Touch`, so
a hot key's estimate does not decay while mem serves its reads; `Touch` is
class-aware and skips owned keys (owned blocks are mirrored into mem too, so a
class-blind touch would feed owned traffic right back in). The TinyLFU
comparison and the evicting `Put` run inside the borrowed store's own lock
(`putIfAdmitted`), so the victim compared is exactly the victim evicted — no
concurrent Get/Delete/Put can reorder the LRU in between. `Put` (the plain
`Store` method) defaults to `Borrowed`.

**Class migration:** a block lives in exactly one budget. `PutClass(k, Owned)`
drops any borrowed copy (promotion); `PutClass(k, Borrowed)` on an owned key
keeps the owned copy and refreshes its recency (never shadows it). Re-putting an
existing key refreshes recency in whichever budget holds it.

The estimator (`sketch.go`) is a 4-bit count-min sketch plus a doorkeeper bloom
filter with periodic halving, so it tracks *recent* popularity and the long tail
of single-touch keys never looks popular. Its size is floored
(`minSketchKeys`): sizing it purely by cache capacity would give a tiny budget
only a handful of counters, every key would alias, and an unseen candidate would
inherit a warm entry's frequency — defeating admission.

```go
ts, _ := store.OpenTiered(store.TieredOptions{Path: p, SlotSize: 4*chunk.MiB, Slots: 4096})
admitted, err := ts.PutClass(key, data, store.Owned)
st := ts.Stats() // OwnedBlocks/Slots, BorrowedBlocks/Slots, AdmitRejected
```

The engine classifies automatically: a block is `Owned` when this node is in the
HRW top-`Replicas` for its chunk over all Ready members, else `Borrowed` (see
docs/engine.md).

### 3.5 Memory tier and `Hybrid`

```go
func OpenMem(MemOptions) (*MemStore, error)      // {SlotSize, Slots}
var ErrBadMemOptions error                       // SlotSize <= 0 or Slots <= 0
func (m *MemStore) Slots() int                   // capacity in blocks
func (d *DiskStore) Slots() int                  // capacity in blocks
func NewHybrid(mem *MemStore, back ClassStore) *Hybrid
```

`MemStore` manages fixed slots in lazy arena segments of at most 64 MiB (one
slot when larger). Slot and chunk descriptors, two bounded open-addressed indices,
free lists and intrusive LRU links allocate once at `OpenMem`. A chunk key maps to
a reusable local descriptor; descriptor plus block index maps to a payload slot.
Indices use backward-shift deletion, so steady churn does not accumulate tombstones
or allocate new entries. Lookup is expected O(1), worst-case O(slots).

`Acquire` pins immutable bytes without copying. `Get`/`GetReader` remain compatible
owned-copy APIs; copying happens outside the metadata lock. `Reserve` fills a
private slot, and `Reservation.Commit` publishes only after caller validation.
The arena and cache metadata perform zero steady-state allocations after the
needed payload segments exist; this does not claim zero allocations in HTTP,
request scheduling, flights, compatibility copies or pressure fallback (ADR-0008).

`Hybrid` is a `ClassStore` that puts a `MemStore` hot set in front of a backing
`ClassStore` (normally a `Tiered`):

- **write-through** — `PutClass` writes to the backing store and mirrors into
  memory. `admitted` is true if the block ended up cached anywhere: the backing
  store may decline a borrowed block by admission while memory still keeps it,
  which is a legitimate outcome, not a failure.
- **read promotion** — `Get` prefers memory and promotes a backing hit into
  memory, so a repeatedly-read block ends up served from RAM.
- **`GetReader` does not promote** — the streaming path is typically a one-shot
  relay, and promoting would mean buffering the whole block, defeating streaming.
- capacity accounting and the owned/borrowed budgets stay with the backing store;
  `Stats()` adds `MemBlocks`/`MemSlots`.

```go
mem, _ := store.OpenMem(store.MemOptions{SlotSize: 4*chunk.MiB, Slots: 64})
h := store.NewHybrid(mem, tieredDisk) // Close(h) closes both tiers
```

`Hybrid.Len` is O(mem slots): the mem tier is checked for duplicates against
the backing store's count. It exists for metrics/diagnostics, not hot paths.

### 3.6 Managed block ownership and direct fills

```go
type Lease struct { /* private data, owner, slot and generation */ }
func HeapLease(data []byte) Lease
func (l Lease) Bytes() []byte
func (l Lease) Clone() Lease
func (l *Lease) Release()
type LeaseStore interface { Acquire(BlockKey) (Lease, bool, error) }
func Acquire(s Store, k BlockKey) (Lease, bool, error)
func (m *MemStore) Acquire(k BlockKey) (Lease, bool, error)
func (t *Tiered) Acquire(k BlockKey) (Lease, bool, error)
func (h *Hybrid) Acquire(k BlockKey) (Lease, bool, error)
type Reserver interface {
    Reserve(context.Context, BlockKey, int64, Class) (Reservation, error)
}
func (m *MemStore) Reserve(ctx context.Context, k BlockKey, n int64, c Class) (Reservation, error)
func (t *Tiered) Reserve(ctx context.Context, k BlockKey, n int64, c Class) (Reservation, error)
type Reservation struct { /* private exclusive slot ownership */ }
func (r Reservation) Bytes() []byte
func (r *Reservation) Commit() (Lease, error)
func (r *Reservation) Abort()
```

A `Lease` owns one reference. Move it by assignment, or call `Clone` to create
another owner; every owner calls `Release`. `Bytes` is a read-only view until that
release, with capacity restricted to valid bytes. Do not mutate it or concurrently
operate on the same lease variable. Different owners can read/release concurrently.
The zero value and repeated release of the same variable are safe. `HeapLease`
takes an existing buffer as immutable owned storage; the caller relinquishes
mutation. The generic `Acquire` wraps a legacy store's copy when needed.

Memory Acquire/Clone/Release use O(1) space and expected O(1) metadata work.
`Tiered` prefers owned and feeds only borrowed accesses to TinyLFU. Disk Acquire
returns a copy. Hybrid borrows RAM hits and uses its ordinary promoting backing
read on a miss; disk storage is never exposed as an unpinned live reader.

`Reservation` is exclusive and unpublished: write only before `Commit`, which
transfers ownership to an immutable lease, or call `Abort`. A concurrent fill
never overwrites an existing key; its result can remain an uncached lease. An
owned publication removes borrowed visibility under the class-publication lock.
The class is ignored by an unclassified MemStore. Non-positive lengths return
`ErrEmptyBlock`, oversize returns `ErrBlockTooLarge`, and canceled waits return
`ctx.Err()`. Disk Tiered returns `ErrNoReservation` to select buffered ingress.

A reservation first uses a free slot or an unpinned eviction candidate, preserving
atomic TinyLFU victim admission. If all slots are pinned/filling it waits at most
2 ms, honoring cancellation, then rechecks capacity under the metadata lock before
returning `ErrNoSlot`. Coalesced release notifications cannot turn reusable
capacity into a false bypass; expiry never starts another wait. An admission rejection
returns that error immediately. This initial fixed wait is a pressure policy.
Callers may allocate a temporary buffer and bypass caching. No per-reader quota
or adaptive backoff is imposed. Generation-checked handles prevent a stale slot
identity from referring to a new occupant; callers must still obey ownership.

Delete hides an entry immediately. Pinned slots remain unavailable until the
last release. Among unpinned candidates the LRU is refreshed when a borrow ends;
a pinned candidate is skipped. `Close` rejects new operations with `ErrClosed`,
hides entries and releases payload segments after the final pin; active leases
remain valid. It never closes storage under a node handler (ADR-0003).
`TieredStats.SlotWaits` counts pressure waits and `WriteBypasses` counts failed
memory reservation due to pressure/admission. Temporary payloads, metadata,
retired pins and HTTP state remain separate from visible occupancy and RSS.

```go
l, ok, err := store.Acquire(cache, key)
if err != nil { return err }
if ok {
    defer l.Release()
    _, err = output.Write(l.Bytes()[offset:end])
    return err
}
```

## 4. Invariants & Guarantees

1. **Bounded indices**: disk lookup is O(1) average; memory's fixed indices are
   expected O(1), worst-case O(slots). Payload copies cost O(block bytes).
2. **Bounded capacity**: `Len() <= Slots`; published, filling and retired pinned
   slots share the same physical capacity. Memory pressure may decline insertion;
   eviction selects an unpinned LRU candidate without fragmentation.
3. **Copy on Get**: returned bytes are a fresh copy, unaffected by later eviction
   or reuse of the slot.
4. **Immutable keys**: `Put` on an existing key keeps the original bytes (only
   recency is refreshed).
5. **Tail blocks**: a block shorter than `SlotSize` is stored and returned at its
   exact length.
6. **Budget isolation**: borrowed churn never evicts owned blocks, and neither
   budget can exceed its own slot count.

## 5. Concurrency & Call Permissions

- `DiskStore` is safe for concurrent use, guarded by a **single mutex** in this
  first cut (including the `ReadAt` in `Get`, so reads are serialized). Per-shard
  locking and pin/refcount for lock-free reads are future optimizations (§8).
- `Get` returns caller-owned copies. `Acquire` returns immutable shared bytes
  pinned until explicit release; reservation bytes are exclusive until publication.
  `GetReader`, by contrast, hands back a reader **backed by the slot** and valid
  only until that slot is reused — and reuse follows **any** removal of the
  block, eviction *and* `Delete` alike: without a pin, a concurrent eviction or
  delete can tear the bytes mid-read, and a torn read is **silent** (no error,
  no generation check exists to detect one). Streaming a block to a socket
  therefore uses owned `Get` or a pinned `Acquire`, **not** an unpinned disk
  `GetReader` — the peer plane
  must never emit a torn block. `GetReader` remains for callers that hold the
  block's lifetime externally: disk `GetReader` does not pin its slot, so callers
  must serialize removal/reuse externally. Memory `Acquire` separately pins its
  physical slot, and disk `Acquire` returns an owned copy. Unpinned disk
  `GetReader` currently has no in-tree callers, which is deliberate.
- Verified with `-race` under concurrent Put/Get/Delete/Has with eviction churn.

## 5.1 Disk footprint and operational notes

- **Deterministic disk paths**: disk and Hybrid modes open `<cache-dir>/blocks.owned` and
  `<cache-dir>/blocks.borrowed`. The same paths are reused on every start, so
  restarts do not accumulate files.
- **Bounded by configuration, not by uptime**: `Open` sets the file to exactly
  `SlotSize * Slots`, growing *or shrinking* it. Lowering `-cache-size` therefore
  reclaims disk on the next start.
- **Sparse files**: the arena is grown by truncating up, which leaves a hole. `ls -l`
  reports the full configured size while `du` reports only the blocks actually
  written — the former is a reservation, not consumption.
- **One process per cache directory.** Two instances sharing a `-cache-dir` would
  corrupt each other's slots in any case; with `O_TRUNC` the second one wipes the
  first's arena on startup. `store.LockDir` guards against this: `cmd/dart`
  acquires an exclusive `flock(2)` in disk/Hybrid mode on `<cache-dir>/.dart.lock` before opening any
  store, so a second instance on the same directory fails fast at startup rather
  than silently destroying the first's cache. The kernel releases the lock if the
  process dies, so a crash never leaves a stale lock behind.
- **No cleanup of foreign files**: only the paths above are managed. Files left in
  the cache directory by anything else are neither read nor removed.

## 6. Determinism / Stability Contract

- Nothing here is on the wire; there is no cross-node or cross-version format to
  keep stable. The backing file layout is a private implementation detail and
  **not** persisted across restarts (§7), so it is free to change.

The additive managed-memory contract follows
[ADR-0008](./adr/0008-borrow-managed-memory-blocks.md). Existing owned-copy APIs
and wire keys retain their contract. Memory reservation can decline under pins;
callers treat caching as best effort. These rules also apply to Hybrid's RAM tier.

## 7. Testing

Managed-memory regression coverage:

| Test | Property |
|---|---|
| `TestLeasePinsDeletedSlot` | active clones prevent reuse after deletion; short tails remain private |
| `TestReservationPublicationAndAbort` | unpublished fills, abort, direct slot publication and duplicate immutability |
| `TestReservationRechecksCoalescedCapacity` | simultaneous releases coalesced before waiters select still allow both reservations |
| `TestLeasePressureCancellationAndClose` | canceled reservations and close preserve active bytes |
| `TestLeaseSteadyStateAllocations` | zero warm borrow and metadata/payload reuse allocations after segment warmup |
| `TestFixedIndexChurnMatchesMap` | 50,000 deterministic random operations cross-checked against an independent map |
| `TestHybridAdmissionFalseWhenBothTiersDecline` | backing admission rejection plus pinned RAM returns false; memory-only admission still returns true |
| `TestReservationCloseBeforePublication` | closing an unfinished fill rejects publication and frees its arena |
| `TestTieredBorrowedLeaseSurvivesPromotion` | promotion preserves old readers, isolation and pressure counters |
| `TestLeaseConcurrentReuse` | concurrent borrowed-slot churn under race detector |


- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **93.0%** of statements. The uncovered lines are the `ReadAt`/
  `WriteAt` I/O-error branches in `Get`/`Put`, which require fault injection to
  hit and are left as defensive handling.
- **Note**: tests use `t.TempDir()`, which honors `TMPDIR`; in the sandbox export
  `TMPDIR=$PWD/.gotmp` (writes are restricted to the workspace).
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./internal/store/ -v -count=1
go test ./internal/store/ -race -count=1
go test ./internal/store/ -cover -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestPutGetRoundtrip` | store then retrieve identical bytes; Has/Len |
| `TestGetMiss` | miss returns `(nil,false,nil)` |
| `TestTailBlockShorterThanSlot` | short (tail) block stored/returned at exact length |
| `TestPutErrors` | empty and oversize blocks rejected; Len unchanged |
| `TestPutExistingKeyDoesNotOverwrite` | immutable-key contract |
| `TestEvictionLRU` | LRU victim chosen; recently-used survives; capacity held |
| `TestDeleteAndSlotReuse` | delete semantics + freed slot reused without eviction |
| `TestCopyOnGet` | returned bytes are a copy, not aliased |
| `TestGetReaderStreams` | streaming read returns exact bytes/length, miss semantics, refreshes LRU |
| `TestGetReaderAfterClose` | streaming read after Close errors |
| `TestReopenStartsCold` | no persistence across restarts; the arena is rebuilt empty (previous run's bytes gone from disk) |
| `TestBadOptions` | invalid SlotSize/Slots rejected |
| `TestOpenFileError` | uncreatable path surfaces an error |
| `TestCloseIdempotent` | double Close is a nil no-op |
| `TestConcurrent` | concurrent ops with eviction churn (`-race`) |
| `TestSketchDoorkeeperAbsorbsFirstSighting` | a first sighting only primes the doorkeeper |
| `TestSketchUnseenKeyIsZero` | an unseen key estimates 0 |
| `TestSketchAdmitFavorsPopular` | hot candidate beats cold victim; unseen candidate loses to hot victim |
| `TestSketchDecay` | halving makes stale popularity fade |
| `TestSketchConcurrent` / `TestNextPow2` | concurrency (`-race`); size rounding |
| `TestTieredBadOptions` / `TestTieredSplitsCapacity` | option validation; 80/20 split, out-of-range fraction rejected (0 = unset → 0.8), ≥1 slot each |
| `TestTieredRoundtripBothClasses` | both budgets store and serve; per-class stats |
| **`TestTieredBudgetIsolation`** | **200 borrowed inserts never evict any owned block** |
| **`TestTieredAdmissionRejectsOneShots`** | **one-shot candidates are refused; warm borrowed entries survive** |
| `TestTieredPutDefaultsToBorrowed` | plain `Put` lands in borrowed |
| `TestTieredPutClassExistingKey` | a borrowed put of an owned key is an admitted no-op (no duplicate) |
| `TestTieredGetReaderAndDelete` | streaming read and delete across both budgets |
| `TestClassString` / `TestTieredConcurrent` | class names; concurrent mixed-class ops (`-race`) |
| `TestMemBadOptions` / `TestMemRoundtrip` / `TestMemTailBlockAndErrors` | memory tier options, round trip, short blocks, rejects |
| `TestMemEvictionLRU` | memory LRU victim selection and capacity |
| `TestMemGetReturnsCopy` | callers cannot reach into a slab |
| **`TestMemSlabRecycledSafely`** | **a copy taken earlier stays intact after its slab is recycled** |
| `TestMemGetReaderAndDelete` / `TestMemPutExistingKeyKeepsOriginal` / `TestMemCloseIdempotent` | streaming read, immutable keys, idempotent Close |
| `TestMemConcurrent` | memory tier under eviction churn (`-race`) |
| `TestHybridWritesThroughToDisk` | a write reaches both tiers |
| **`TestHybridSurvivesMemEviction`** | **churning memory past capacity leaves every block readable from disk** |
| `TestHybridPromotesOnRead` | a disk hit is promoted into memory |
| `TestHybridGetReaderDoesNotPromote` | the streaming path does not buffer to populate memory |
| `TestHybridHasDeleteLenClose` / `TestHybridPutDefaultsToBorrowed` | cross-tier Has/Delete/Len; plain Put is borrowed |
| `TestHybridConcurrent` | concurrent mixed-class hybrid ops (`-race`) |
| `TestOwnedTrafficDoesNotDecayBorrowedEstimates` | owned reads never feed the borrowed estimator (no premature decay of warm entries) |
| `TestHybridMemHitsFeedBackingEstimator` | a mem-tier hit still feeds the backing Tiered estimator via Touch |
| `TestTouchSkipsOwnedKeys` | Touch is class-aware: owned mem hits never feed the borrowed estimator |
| `TestPutClassOwnedDropsBorrowedCopy` | promotion to owned drops the borrowed copy (one block, one budget) |
| `TestBorrowedPutClassOnOwnedKeyRefreshesRecency` | the owned-hit branch refreshes recency as promised |
| `TestPutClassFeedsEstimator` | insertion is an access: write-only patterns do not degenerate admission |
| `TestBorrowedAdmissionSerializesWithEviction` | concurrent admit+evict never exceeds the budget (`-race`) |
| `TestMemStoreLenCounterExact` | the O(1) `Len` counter is exact across Put/refresh/evict/Delete/Close (`BenchmarkMemStoreLen` pins O(1) at 10k slots) |

| `TestMemoryTieredBudgetsAndAdmission` | RAM class isolation, TinyLFU rejection, promotion, immutable copies, occupancy and close |
| `TestMemoryTieredValidationAndConcurrency` | invalid capacities/fractions and concurrent RAM class operations |
| `TestMemAdmissionRejectKeepsVictim` | rejected RAM candidate leaves its victim intact; ordinary LRU insertion still evicts |

## 8. Limitations & TODO

- **No warm restart**: the store starts cold every boot, and the arena is
  **rebuilt empty** (`O_TRUNC`) on open. Under DART's read-only semantics a cold
  cache is always safe — blocks are re-fetched from the origin, which is the source
  of truth — and it avoids serving stale/torn data after a crash. Because a
  reopened store can never reuse the previous run's slots, keeping their contents
  would buy nothing while leaving stale bytes readable on disk (including bytes
  fetched from an authenticated origin), so they are discarded. A WAL or per-slot
  commit header for warm restart is a future addition.
- **Metadata contention**: memory uses a short mutex for its bounded indices and
  reference/LRU updates, with no payload copy or I/O inside it. Sharding and NUMA
  placement remain future tuning work guided by measured contention.
- **Socket copy avoidance**: pinned memory serving avoids the cache's payload
  copy. Ordinary HTTP writes still use the kernel's normal copying socket path;
  vector writes and kernel zero-copy sends are separate future work. Disk
  GetReader remains unpinned, so disk serving uses owned bytes.
- **No hybrid admission coupling**: the memory tier uses plain LRU; it does not
  yet consult TinyLFU, so a one-shot block can transiently occupy a slab (it
  cannot displace disk contents, and the slab is recycled on eviction).
- **O_DIRECT**: not used yet; add for the disk tier when tuning the data path.
