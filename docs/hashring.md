# `internal/hashring`

Weighted Rendezvous Hashing (HRW) for placement, plus a full k-ary preorder
tree derived from the HRW ordering for distribution. These are DART's two
**deterministic, coordination-free** primitives.

- Source: `internal/hashring/hashring.go`, `internal/hashring/tree.go`
- Tests: `internal/hashring/hashring_test.go`, `internal/hashring/tree_test.go`
- Import path: `github.com/data-accelerator/dart/internal/hashring`

## 1. Overview

The system has no central coordinator, so every node must independently derive
an **identical** result from the same `(key, member-set, weights)` inputs:

- **Placement** — which node owns a chunk (plus replica candidates): weighted HRW.
- **Distribution topology** — when many nodes read the same chunk, who relays to
  whom: interpret the HRW total order as the preorder traversal of a full k-ary
  tree.

This package provides only pure computation. Membership source, epoch handling,
and reader-set maintenance belong to higher layers (`internal/cluster`, etc.).

## 2. Concepts

| Term | Meaning |
|---|---|
| `key` | The 64-bit key fed into the hash (e.g. a chunkKey). |
| `Node` | A cluster member with a stable `ID` and a capacity `Weight`. |
| Placement order `L` | The return of `Rank(key, nodes)`: a total order by descending HRW score. `L[0]` = owner, `L[0..R-1]` = replica candidates. |
| Preorder tree | An ordered node sequence (the placement order, or the active-reader-set order) interpreted as the preorder traversal of a full k-ary tree; `Parent`/`Children` operate on **index positions**. |

> Placement HRW runs over **all Ready nodes**; the distribution tree runs over
> the **active reader set S** (two scopes). This package does not
> distinguish them — it just ranks "the nodes you gave it"; the scope is the
> caller's choice.

## 3. Public API

### 3.1 `type Node`

```go
type Node struct {
    ID     string  // stable identity: identical cluster-wide, stable across restarts; never derive from ephemeral values like PodIP
    Weight float64 // relative cache capacity; <=0 is treated as 1; equal weights reduce to standard HRW
}
```

- `ID` must be byte-identical across all nodes (same encoding/case, no stray
  whitespace); otherwise the hashes differ and the ring splits.
- `Weight` determines ownership share, proportionally (see §4).

### 3.2 `func Hash64(key uint64, nodeID string) uint64`

Deterministic 64-bit hash: FNV-1a (over the little-endian 8 bytes of `key`
followed by the raw bytes of `nodeID`) then an fmix64 finalizer.

- **Complexity**: O(len(nodeID)).
- **Determinism**: fully specified; identical across platforms and Go versions;
  pinned by cross-language golden tests.
- **Stability**: changing this function reshuffles the entire ring — a
  **breaking change** that must bump the epoch (see §6).

### 3.3 `func Rank(key uint64, nodes []Node) []Node`

Returns a **new slice** of `nodes` ordered by **descending** HRW score for `key`.

- Score: `score = Weight / -ln(u)`, with `u = (Hash64(key,ID)+0.5)/2^64 ∈ (0,1)`.
  Higher score ranks first.
- **Tie-break**: equal scores order by ascending `ID`. Since IDs are unique this
  is a strict total order, so **the result is independent of input order** — the
  foundation of the coordination-free design.
- **Does not mutate** the input; returns a freshly allocated slice owned by the
  caller.
- **Complexity**: O(m·L) hashing + O(m log m) sort (m = node count, L = mean ID
  length).
- **Boundary**: nil/empty `nodes` returns an empty slice.

```go
r := hashring.Rank(chunkKey, members)
owner := r[0]        // authoritative holder
replicas := r[1:R]   // replica candidates
```

### 3.4 `func Top(key uint64, nodes []Node, n int) []Node`

Returns the top-`n` nodes (owner + replica candidates).

- `n <= 0` → nil; `n >= len(nodes)` → all.
- Semantically equivalent to `Rank(...)[:n]`.
- **Complexity**: currently a full `Rank` then a slice, O(m log m). (Can be
  optimized to O(m log n) with partial selection, see §8.)

### 3.5 Preorder tree: `Parent` / `Children` / `Depth`

These operate on **index positions** in `[0, n)`, where `n` is the sequence
length and `k` is the fanout (max children per node, ≥1). The sequence itself
(which index is which node) is obtained by the caller via `Rank`.

```go
func Parent(i, n, k int) int     // parent index of i; root (i==0) returns -1
func Children(i, n, k int) []int // direct children indices of i (ascending); leaf returns nil
func Depth(i, n, k int) int      // number of edges from root to i (root = 0)
```

- Tree layout: position 0 is the root; the remainder `(0, n-1]` is split into up
  to k **contiguous, as-balanced-as-possible** segments (the first
  `remaining mod k` segments get one extra), applied recursively.
- `Parent` and `Children` share the internal `childSegments`, so they are
  **exact mutual inverses**.
- **Complexity**: all O(depth·k) = O(k·log_k n), no significant extra allocation.
- **Boundary**: out-of-range index or `k < 1`: `Parent`/`Depth` return -1,
  `Children` returns nil.
- **Degenerate cases**: `k=1` → linked list (`Parent(i)=i-1`); `k ≥ n-1` → star
  at the first level below the root.

```go
L := hashring.Rank(chunkKey, readerSet) // rank the active reader set
// after finding this node's index i in L:
parentIdx := hashring.Parent(i, len(L), fanout)
parent := L[parentIdx] // pull / relay from it
```

## 4. Invariants & Guarantees

Callers may rely on the following (each is guarded by a test, see §7):

1. **Determinism**: identical `(key, node-set, weights)` → identical `Rank`
   output, independent of input order, platform, or build.
2. **Total order**: `Rank` is a strict total order (score desc + ID asc
   tie-break), no ambiguity from ties.
3. **Weight proportionality**: the probability a node becomes owner equals
   `weight_i / Σweight` (weighted HRW / straw2).
4. **Minimal disruption**: adding/removing one node reassigns only ~`1/(m±1)` of
   keys; on addition, keys move only onto the new node, never between two
   pre-existing nodes.
5. **Preorder tree structure**:
   - any prefix `[0..m]` forms, under the parent pointer, a **connected subtree
     containing the root** (the "joined-so-far" set is always a valid tree);
   - every non-root node's parent **ranks earlier** (the parent entered the
     sequence earlier and is more likely to already hold the data);
   - `Children` over all nodes is exactly a **partition** of `[1, n)` (each
     non-root position has exactly one parent);
   - acyclic — following `Parent` from any node always reaches the root;
   - each node has at most k children.

## 5. Concurrency & Call Permissions

- **All functions are pure with no package-level mutable state**, hence
  inherently **goroutine-safe**; call concurrently without locking. (`-race`
  passes.)
- `Rank`/`Top` **do not mutate** the input `nodes`; they return a **freshly
  allocated** slice the caller owns exclusively and may modify freely.
- `Node` is a value type, copied by value; slice elements are value copies, so
  the returned slice's elements do not alias the input's.
- No I/O, no global initialization, no `init()`, no external dependencies (only
  stdlib `encoding/binary`, `math`, `sort`).
- **Caller contract**:
  - the placement layer calls `Rank`/`Top` with **all Ready nodes**;
  - the distribution layer calls `Rank` with the **active reader set S**, then
    calls `Parent`/`Children` on the **indices** of the returned sequence;
  - `Parent`/`Children`'s `n` must equal the length of the sequence in use, and
    `i` must be a valid index within it, otherwise you get -1/nil.

## 6. Determinism / Stability Contract

- The construction of `Hash64` (FNV-1a byte order + fmix64 constants) and the
  `score` formula are **part of the protocol**. Any change reshuffles the ring /
  changes tree shapes — a **breaking change**: it must bump the cluster epoch and
  rely on epoch convergence under the read-only semantics (while old and new
  views coexist, only routing efficiency is affected, never correctness).
- **Floating point note**: `score` uses `math.Log` (a pure-Go implementation in
  Go, hence cross-platform consistent); `u=(h+0.5)/2^64` stays strictly inside
  `(0,1)`, avoiding `ln(1)=0`/`ln(0)=-inf`. For equal weights the HRW order is
  equivalent to descending `Hash64` integer order — cross-validation confirmed
  the float order == the integer order, and the measured minimum hash gap
  (~5.5e16) far exceeds the float-ambiguity threshold (~2^11), so floating point
  introduces no ordering ambiguity.
- **Golden value source**: the `Hash64` golden values were computed by an
  **independent Python implementation** (not by the Go code itself), forming a
  genuine cross-implementation guard; changing the hash trips the golden test
  immediately.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` all pass.
- **Coverage**: **97.0%** of statements (`go test -cover`). Per function:
  `Hash64/fmix64/score/Top/Parent/Children/Depth` 100%, `Rank` 93.3%,
  `childSegments` 94.4%, `descend` 93.8% (the uncovered branches are defensive
  returns that are unreachable for valid inputs — expected).
- **Reproduce**:

```bash
# In the sandbox, export the cache dirs first (see docs/README.md)
go test ./internal/hashring/ -v -count=1
go test ./internal/hashring/ -race -count=1
go test ./internal/hashring/ -cover -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestHash64Golden` | `Hash64` matches the 5 golden values from an independent Python reference (cross-implementation determinism) |
| `TestRankGolden` | full ordering for a fixed node-set+key matches the golden order (float order == integer order) |
| `TestRankShuffleInvariant` | 200 random input shuffles yield byte-identical `Rank` output (core coordination-free property) |
| `TestRankDoesNotMutateInput` | `Rank` does not mutate the caller's slice |
| `TestRankIsPermutation` | output is a permutation of the input, no dupes/omissions |
| `TestTieBreakStableAcrossShuffle` | the tie-break forms a total order (stable under shuffles) |
| `TestUnweightedBalance` | equal weights spread ownership evenly (10 nodes, rel dev ≤10%) |
| `TestWeightedDistribution` | ownership share ∝ weight (1:1:2:4, rel dev ≤8%) |
| `TestMinimalDisruption` | adding 1 node migrates only ~1/(m+1) of keys, and only onto the new node |
| `TestTopAndEdgeCases` | `Top` boundaries (n≤0/oversize), `Rank(nil)`, zero-weight==1 |
| `TestTreeGolden` | hand-verified tree shape for n=7,k=2 (Parent/Children/Depth) |
| `TestChainWhenK1` | k=1 degenerates to a linked list |
| `TestParentChildrenInverse` | Parent/Children are inverses over a broad (n,k) matrix |
| `TestFanoutDegree` | each node has ≤ k children |
| `TestNoCyclesReachRoot` | following Parent always reaches the root, no cycles |
| `TestPartitionExactlyOnce` | Children partitions `[1,n)` exactly |
| `TestPrefixConnectivity` | any prefix is a connected subtree containing the root |
| `TestTreeEdgeCases` | out-of-range / single-node / k<1 return contract |

## 8. Limitations & TODO

- **Hash**: currently FNV-1a+fmix64 (dependency-free, adequate distribution). The
  design target is xxh3/highwayhash; if switched, follow the §6 breaking-change
  process and update the golden values.
- **`Top` performance**: currently a full sort then slice; for large member sets
  it can become a heap-based partial selection (O(m log n)).
- **`Rank` caching**: rankings for hot keys could use a small LRU cache
  (invalidated on membership epoch change); not built in here — left to the
  caller.
- **Real-holder awareness**: the deterministic preorder tree is the
  coordination-free fallback; at runtime, real holder information can be used to
  skip intermediate nodes that are "in the tree but hold no data" (implemented in
  the distribution layer, not here).
