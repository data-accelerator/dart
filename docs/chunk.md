# `internal/chunk`

DART's three-level addressing — object → chunk → block — and the pure,
deterministic keying and range decomposition built on it.

- Source: `internal/chunk/chunk.go`
- Tests: `internal/chunk/chunk_test.go`
- Import path: `github.com/data-accelerator/dart/internal/chunk`

## 1. Overview

Reads enter as arbitrary HTTP byte ranges but must map onto a fixed grid:

```
object   whole blob (GB-scale)
chunk    large logical unit (default 256 MiB): HRW placement + P2P tree
block    small physical unit (default 4 MiB): transfer / cache slot / on-demand read
```

Placement and the distribution tree operate on **chunks** (via `ChunkKey` →
`hashring.Rank`); transfer and caching operate on **blocks**. This package is
pure and deterministic; it holds no state and does no I/O.

## 2. Concepts

| Term | Meaning |
|---|---|
| `Config` | The grid: `ChunkSize` and `BlockSize` (ChunkSize a positive multiple of BlockSize). |
| chunk index | `offset / ChunkSize`; the placement/tree unit. |
| block index | `offset / BlockSize`; global over the object; the transfer/cache unit. |
| `Segment` | Intersection of a requested range with one block: which chunk/block, and the absolute `[From,To]` to serve. |
| `ChunkKey` | 64-bit key of `(namespace, objectID, chunkIndex)` fed to `hashring.Rank`. |
| `objectID` | Content-addressed identity (digest) when available, else canonical URL. |

## 3. Public API

### 3.1 `type Config`

```go
type Config struct { ChunkSize, BlockSize int64 } // bytes
func DefaultConfig() Config          // {256 MiB, 4 MiB} => 64 blocks/chunk
func (c Config) Validate() error     // BlockSize>0, ChunkSize>0, ChunkSize%BlockSize==0
func (c Config) BlocksPerChunk() int64
```

Constants `MiB` and `GiB` are provided for convenience. All grid methods below
assume a valid Config.

### 3.2 Grid math

```go
func (c Config) ChunkIndex(offset int64) int64    // chunk containing offset
func (c Config) BlockIndex(offset int64) int64    // global block containing offset
func (c Config) ChunkOfBlock(blockIndex int64) int64
func (c Config) BlockStart(blockIndex int64) int64 // absolute byte offset of block start
```

All are O(1) integer arithmetic; `offset`/`blockIndex` must be ≥ 0.

### 3.3 `func (c Config) Segments(start, end int64) []Segment`

Decomposes the **inclusive** absolute range `[start, end]` into the ordered
blocks covering it.

```go
type Segment struct {
    ChunkIndex int64
    BlockIndex int64
    From       int64 // inclusive, >= block start and >= start
    To         int64 // inclusive, <= block end and <= end
}
```

- The caller must clamp `start`/`end` to the object bounds
  (`0 <= start <= end <= size-1`); this package needs no object size.
- Invalid input (`start < 0` or `end < start`) returns `nil`.
- Guarantee: the segments **tile `[start, end]` exactly** — contiguous, no gaps
  or overlaps, each within its own block, with the correct owning chunk.

```go
for _, s := range cfg.Segments(start, end) {
    key := chunk.ChunkKey(ns, oid, s.ChunkIndex)  // -> hashring.Rank for placement/tree
    // fetch/serve block s.BlockIndex, slice bytes [s.From, s.To]
}
```

### 3.4 `func ChunkKey(namespace, objectID string, chunkIndex int64) uint64`

Deterministic 64-bit key: FNV-1a over `(namespace, 0x1F, objectID, 0x1F,
big-endian chunkIndex)` then fmix64. Feeds `hashring.Rank` to pick owner and
replicas. **Part of the wire protocol** — see §6.

**Field exclusion**: the `0x1F` separator must never appear inside a field, or
the serialization is not injective (`("a","b␟c")` would collide with
`("a␟b","c")`). Enforced on both sides: a namespace containing `0x1F` is
rejected at engine construction, and derived object identities have `0x1F`
stripped (a URL can smuggle one in via a percent-encoded `%1F` in the path).
Note that cross-field collisions are the only case: within one namespace the
fixed 8-byte chunk-index suffix forces equal objectIDs anyway.

### 3.5 `func ObjectID(rawURL string) (id string, contentAddressed bool)`

Derives the caching identity of a blob URL, preferring content addressing:

- if a path segment is a digest `<algo>:<hex>` (≥32 hex, e.g. an OCI
  `sha256:<64 hex>`), returns that digest lower-cased with
  `contentAddressed=true` (enables cross-origin/registry dedup; marks the object
  immutable);
- otherwise returns a canonical URL (lower-cased scheme+host, path without
  query/fragment) with `contentAddressed=false`.

Non-OCI integrity schemes (npm/pypi `sha512-<base64>`, etc.) are **not**
recognized here; mapping those is a policy-layer concern.

### 3.5 Object identity and presigned upstreams

```go
func ObjectID(rawURL string) (id string, contentAddressed bool)              // = LayoutDistribution
func ObjectIDLayout(rawURL string, layout DigestLayout) (string, bool)
const ( LayoutDistribution DigestLayout = iota; LayoutOCIOnly )
```

Two URL shapes carry a digest, because an upstream may be either an OCI blob
address or a **presigned URL into the registry's backing bucket** — and the latter
is the more common form in practice:

| Shape | Example | Recognized by |
|---|---|---|
| OCI blob | `/v2/lib/nginx/blobs/sha256:<64 hex>` | both layouts |
| Distribution object store | `/docker/registry/v2/blobs/sha256/ab/ab<62 hex>/data` | `LayoutDistribution` (default) |

Without the second, a presigned URL falls back to the canonical URL, so the same
layer reached through **different buckets or endpoints is cached twice**.

**Recognition is deliberately strict**, because a false positive would map two
*different* objects onto one key — a correctness bug, not a slow cache. Three
independent conditions must hold:

1. the algorithm is one whose digest length is known (`sha256`, `sha512`);
2. the hex is well-formed and exactly that length;
3. **the intermediate segment equals the hex's own first two characters.**

Condition 3 is the strong one: it is a self-consistency check that arbitrary paths
are vanishingly unlikely to satisfy by accident, which is what makes defaulting
the layout ON acceptable. `LayoutOCIOnly` (`dart -oci-digest-only`) turns it off
for a non-standard backing store; identity then falls back to the canonical URL,
which costs dedup but cannot conflate anything.

**The query is always excluded.** That is load-bearing rather than tidy: a
presigned signature differs on every request, so including it would give one
object a new identity each time — and would also leak a credential into the key.
This holds for the raw fallback too (scheme/host-less or unparsable input): the
query and fragment are cut at the first `?`/`#` before the identity is derived.

## 4. Invariants & Guarantees

1. **Deterministic keying**: `ChunkKey` depends only on its inputs; identical
   inputs → identical key on every node/platform.
2. **Exact tiling**: `Segments` output is contiguous and non-overlapping,
   covering exactly `[start, end]`; each segment lies within one block; a
   segment's `ChunkIndex == ChunkOfBlock(BlockIndex)`.
3. **Nesting**: with a valid Config, `BlocksPerChunk` blocks fit exactly per
   chunk, so chunk boundaries fall on block boundaries.
4. **No object size needed** for range decomposition (only for tail-block length
   at the fetch layer, which owns the size).

## 5. Concurrency & Call Permissions

- All functions are **pure with no shared state** → inherently goroutine-safe,
  no locking, no allocation beyond the returned `Segments` slice.
- No I/O, no globals, no `init()`; only stdlib (`encoding/binary`, `errors`,
  `net/url`, `strings`).
- `Config` is a small value passed by value.

## 6. Determinism / Stability Contract

- The `ChunkKey` construction is **part of the wire protocol**: it selects chunk
  owners. Changing it reshuffles placement across the cluster and must be treated
  as a breaking change (bump the epoch). It is pinned by a cross-language golden
  test computed with an independent Python implementation.
- `ObjectID`'s digest-extraction and URL-canonicalization rules affect cache
  identity and dedup; changing them changes cache keys and should be versioned
  deliberately.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **97.9%** of statements. All functions 100% except `isDigest`
  92.9% (one defensive branch uncovered).
- **Reproduce**:

```bash
# In the sandbox, export the cache dirs first (see docs/README.md)
go test ./internal/chunk/ -v -count=1
go test ./internal/chunk/ -race -count=1
go test ./internal/chunk/ -cover -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestChunkKeyGolden` | `ChunkKey` matches an independent Python reference (serialization pinned) |
| `TestChunkKeyAgainstPythonReference` | CI diffs the ChunkKey goldens against the tracked Python script |
| `TestConfigValidate` | valid/invalid configs; `BlocksPerChunk` |
| `TestGridMath` | ChunkIndex/BlockIndex/ChunkOfBlock/BlockStart across boundaries |
| `TestSegmentsSingleBlock` | a small read maps to exactly one block/sub-range |
| `TestSegmentsCrossBlockAndChunk` | a range crossing block and chunk boundaries splits correctly |
| `TestSegmentsInvalidRange` | negative start / end<start return nil |
| `TestSegmentsCoverProperty` | exhaustive tiling: contiguous, in-bounds, correct chunk mapping |
| **`TestObjectIDPresignedDedupAcrossEndpoints`** | **one layer via 4 different signed/host forms plus the OCI form all yield one identity** |
| **`TestObjectIDRejectsNonLayoutPaths`** | **12 near-miss paths are refused rather than fabricating a digest** |
| `TestObjectIDSha512Layout` | the recognizer is not hardcoded to sha256 |
| `TestObjectIDLayoutSwitch` | `-oci-digest-only` falls back to the canonical URL, still without the query |
| `TestObjectIDQueryNeverInIdentity` | different signatures give the same identity |
| `TestObjectIDCaseNormalization` | digest and host are lower-cased |
| `TestObjectID` | digest extraction, lower-casing, query stripping, host:port not a digest, fallbacks |
| `TestObjectIDFallbackStripsQueryAndFragment` | the raw fallback cuts query/fragment too: signatures never move identity |
| `TestObjectIDNeverContainsSeparator` | a %1F-decoded separator never lands in a derived identity |
| `TestIsDigest` | digest-shape recognition incl. rejects (bad hex, short, uppercase algo) |

## 8. Limitations & TODO

- **Tail-block length**: `Segments` needs no object size; the fetch layer clamps
  the final block to the object size (it holds the size from Content-Range/HEAD).
- **Per-origin grid**: `Config` is per-origin configurable (e.g. smaller blocks
  for latency-sensitive origins). Selection/wiring is a higher-layer concern.
- **Integrity schemes**: npm/pypi integrity → objectID mapping belongs to the
  policy layer, not here.
