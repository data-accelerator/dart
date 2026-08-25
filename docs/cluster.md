# `internal/cluster`

Cluster membership: the set of peer nodes with their lifecycle state and
capacity weight, plus a deterministic epoch derived from the membership content.
It feeds `internal/hashring`.

- Source: `internal/cluster/cluster.go`, `internal/cluster/provider.go`
- Tests: `internal/cluster/cluster_test.go`, `internal/cluster/provider_test.go`
- Import path: `github.com/data-accelerator/dart/internal/cluster`

## 1. Overview

DART discovers peers from Kubernetes (all peers equal, TCP only, no broadcast).
This package models the resulting membership as an immutable snapshot (`View`)
and exposes it to the placement/distribution layers. It deliberately contains no
Kubernetes code: a pluggable `Provider` supplies Views, so the core is testable
and usable without a cluster (`StaticProvider`). A Kubernetes EndpointSlice
seeder ships as the separate `providers/k8s` module (see [k8s.md](./k8s.md)),
keeping `client-go` out of this module.

## 2. Concepts

| Term | Meaning |
|---|---|
| `State` | Member lifecycle: `Joining → Ready → Suspect → Leaving`. |
| `Member` | One node: stable `ID`, capacity `Weight`, `State`. |
| `View` | Immutable membership snapshot at a given `Epoch`; safe to share across goroutines. |
| `Epoch` | Deterministic 64-bit hash of the canonical membership; an agreement/change token, not a monotonic counter. |
| `Provider` | Source of Views over time (`Current` + `Subscribe`). |

Only **Ready** members participate in ownership (placement). Read attempts may
also include **Suspect** members (their data may still be present, so a brief
NotReady does not force an immediate reshard).

## 3. Public API

### 3.1 `type State`

```go
type State uint8
const ( Joining State = iota; Ready; Suspect; Leaving )
func (s State) String() string
```

`String` returns `"Joining"|"Ready"|"Suspect"|"Leaving"`, or `"Unknown"` for
any other value.

### 3.2 `type Member`

```go
type Member struct {
    ID     string  // stable identity, cluster-consistent, restart-stable; never PodIP
    Addr   string  // peer-transport address host:port; auxiliary, NOT part of the epoch
    Weight float64 // relative cache capacity; <=0 treated as 1 by hashring; stored verbatim
    State  State
}
```

`Addr` is routing metadata (where to pull blocks from this member) and is
deliberately excluded from the epoch: it does not affect placement ordering, and
a stale `Addr` only causes a fallback to origin (safe under read-only
semantics). It is retrievable via `Get`.

### 3.3 `type View` and `func NewView(members []Member) *View`

`NewView` copies and **canonicalizes** the input: sorts by `(ID, State, Weight)`
and de-duplicates by `ID`. Duplicate IDs are a conflicting-reports case: the
canonical sort is `(ID, state rank, Weight)` with the rank **Ready > Suspect >
Joining > Leaving**, so the entry claiming the node can serve always wins — a
`Joining` or `Leaving` marker must never suppress a member another source still
reports as `Ready` (placement would silently lose a serving node). The resulting
View and its epoch are therefore **independent of input order**.
The input slice is not modified.

Methods (all on an immutable receiver; safe for concurrent use):

| Method | Semantics | Complexity |
|---|---|---|
| `Epoch() uint64` | deterministic membership epoch | O(1) |
| `Len() int` | number of members (all states) | O(1) |
| `Members() []Member` | **copy** of all members in canonical order (caller owns it) | O(n) |
| `Get(id string) (Member, bool)` | member by ID via binary search | O(log n) |
| `Ready() []hashring.Node` | Ready members as hashring nodes, sorted by ID (**shared, read-only**) | O(1) |
| `Live() []hashring.Node` | Ready+Suspect members as hashring nodes, sorted by ID (**shared, read-only**) | O(1) |

`Ready()`/`Live()` return internal slices precomputed at construction. They are
shared and must not be mutated; `hashring.Rank` copies its input, so they are
safe to pass directly.

```go
v := cluster.NewView(members)
owner := hashring.Rank(chunkKey, v.Ready())[0] // placement over Ready members
```

### 3.4 `type Provider` and `StaticProvider`

```go
type Provider interface {
    Current() *View                       // latest View, never nil, lock-free
    Subscribe() (<-chan *View, func())     // current + subsequent Views, plus cancel
}
```

`StaticProvider` is an in-memory implementation for tests and non-Kubernetes
deployments:

```go
func NewStaticProvider(members ...Member) *StaticProvider
func (p *StaticProvider) Current() *View
func (p *StaticProvider) Set(members []Member) *View  // rebuild View + notify subscribers
func (p *StaticProvider) Subscribe() (<-chan *View, func())
```

`Subscribe` returns a **buffered(1), coalescing** channel: the current View is
delivered immediately, then each `Set` — atomically: registration and the
snapshot happen under one lock, so a concurrent `Set` is either in the initial
delivery or arrives right after, never lost in a gap. `Set` likewise stores and
notifies under one critical section, so concurrent `Set`s linearize: the one
serialized last owns `Current()` and subscribers observe every `Set` in that
order. A slow consumer only ever sees the most
recent View (intermediate ones are dropped) and the producer never blocks. The
returned cancel func unsubscribes and is idempotent.

## 3.9 Discovery: `Seeder` + `DynamicProvider`

```go
type Seeder interface { Seeds(ctx) ([]string, error) }   // candidate addresses
type DNSSeeder struct { Name string; Port int; Lookup ... }
type StaticSeeder []string
func ParseSeeder(spec string) (Seeder, error)            // "dns:<name>:<port>" | "static:a,b"

type RosterFetcher interface { FetchRoster(ctx, addr) (members []Member, responderID string, err error) }
func NewDynamicProvider(DynamicConfig) *DynamicProvider  // implements Provider
```

Discovery is split in two, and the split is the design:

| Half | Question | Where it runs |
|---|---|---|
| **Seeder** | "what addresses might be peers?" | environment-specific, pluggable — DNS, a static list, anything |
| **Roster exchange** | "what are their stable identities, and who else do they know?" | over DART's own peer connections, identical everywhere |

The reason not to push the second half into the environment too: a seed answer
carries **addresses**, and a `Member.ID` must be a *stable* identity (never an
address — see §3.2). Only the peer itself can say what its ID is. Exchanging
rosters also means one reachable neighbour is enough to find the whole cluster,
which is what makes a truncated DNS answer or a partial seed list survivable.

### Adding and forgetting are not symmetric

- **Adding** happens immediately, on hearsay. Being wrong costs a request sent to a
  node that turns out not to hold the block, which is safe.
- **Forgetting** waits for `ForgetAfter` (default 60s) of no direct contact, because
  removing a member re-runs placement, moves ownership of ~1/N of the keyspace and
  re-forms the distribution tree. Routing around an unreachable peer already happens
  in about a second, locally, via the circuit breaker — so removal can afford to be
  slow, and should be.

### Only direct contact refreshes the liveness clock

`tracked.lastContact` is updated by a **successful roster fetch answered by that
member**, or by that member **contacting us**. It is deliberately *not* updated by
another peer mentioning it — and that includes hearsay carrying a *changed*
address: the newest address is adopted (a pod can be recreated with a new IP
while keeping its identity), but the clock is untouched, since an address report
is not liveness evidence. Otherwise two survivors flip-flopping conflicting
reports about a dead node would refresh each other's memory of it forever.

The credit goes to the member that *answered*, identified by its self-reported
stable ID (`responderID`), never to the member that happens to advertise the
dialed address: an address can be recycled to a different member (pod-IP reuse),
and crediting by address would keep the previous owner's clock alive
indefinitely, pinning a dead member in placement. A responder that does not
identify itself earns no credit.

This is load-bearing rather than fussy. Every surviving peer keeps listing a dead
node for as long as it still remembers it, so if hearsay refreshed the clock, two
survivors would refresh each other's memory of the dead node and **nobody would ever
remove it**. A live 3-node run demonstrated exactly that before the distinction
existed.

Counting *inbound* contact as evidence also handles asymmetric reachability: a peer
we cannot dial but which can dial us stays in membership.

### Liveness is never on the wire

`Member.State` is not serialized. A peer's opinion about who is up is its own local
observation; importing it would let one node's transient failure become cluster-wide
membership churn. Nodes are allowed to disagree — under read-only semantics a wrong
guess costs a hop, never correctness.


## 4. Invariants & Guarantees

1. **Deterministic epoch**: identical membership → identical epoch, regardless of
   input order; any change to any `ID`/`Weight` changes the epoch. `State` and
   `Addr` are excluded (see below).
2. **Canonical order**: members are sorted by `(ID, State, Weight)` and unique by
   `ID`; `Ready()`/`Live()` are sorted by `ID`.
3. **Immutability**: a `View` never changes after construction; `Members()`
   returns a copy; `Ready()`/`Live()` return shared read-only slices.
4. **Placement scope**: `Ready()` contains exactly the Ready members; `Live()`
   adds Suspect; Joining/Leaving are excluded from both.

## 5. Concurrency & Call Permissions

- `View` is immutable and **safe for concurrent use**; share the pointer freely.
- `StaticProvider.Current()` is **lock-free** (`atomic.Pointer[View]`), safe on
  the hot path. `Set`/`Subscribe`/cancel take a short mutex; subscriber
  notification is non-blocking.
- `Members()` returns a fresh copy (caller may mutate). `Ready()`/`Live()` return
  shared slices that **must not be mutated**.
- `NewView` does not mutate its input slice.
- Verified with `-race` (concurrent readers + writers + a draining subscriber).

## 6. Determinism / Stability Contract

- The epoch serialization (FNV-1a over `ID`, `0x1F`, big-endian float64 weight
  bits, `0x1E`, per canonical member, then fmix64) is **part of the wire
  protocol** (`X-DART-Epoch`). Changing it changes every epoch and must be
  treated as a protocol change. It changed once, in 2026-08, when `State` was
  removed from the hash.
- The epoch covers only the **authoritative** fields: `ID` and `Weight`. `Addr` is
  routing metadata, and `State` is a *local* judgement about liveness. Hashing
  either would make two nodes holding the same membership compute different
  epochs, and the epoch would stop being able to answer the one question it
  exists for — "are we looking at the same membership?". Liveness is layered on
  top of the view, not part of it.
- The epoch is an **agreement/change token, not a monotonic counter**: on a
  mismatch a node refreshes membership (re-resolving seeds and re-exchanging
  rosters) and views converge. This suffices under the read-only semantics, where
  a transiently divergent view only affects routing efficiency, not correctness.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` all pass.
- **Coverage**: **95.6%** of statements. Every function is 100% except `NewView`
  95.0% (the uncovered path is a defensive branch).
- **Reproduce**:

```bash
# In the sandbox, export the cache dirs first (see docs/README.md)
go test ./internal/cluster/ -v -count=1
go test ./internal/cluster/ -race -count=1
go test ./internal/cluster/ -cover -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestEpochGolden` | epoch matches an independent Python reference (serialization pinned) |
| `TestEpochAgainstPythonReference` | CI diffs the epoch goldens against the tracked Python script |
| `TestEpochDeterministicUnderShuffle` | epoch independent of input order |
| `TestEpochChangesOnMutation` | an id or weight change bumps the epoch; a state change does not |
| **`TestEpochExcludesState`** | **any combination of states yields one epoch, so the epoch can serve as a convergence token** |
| `TestReadyLiveFilters` | Ready=Ready only, Live=Ready+Suspect, sorted, weights carried |
| `TestDedupDeterministic` | duplicate IDs collapse deterministically regardless of order |
| `TestDedupKeepsServingStateOverJoining` | duplicate IDs keep the most authoritative serving state (Ready > Suspect > Joining > Leaving) |
| `TestHearsayAddrChangeDoesNotRefreshLiveness` | hearsay address changes are adopted but never refresh the liveness clock; flip-flops still expire |
| `TestConcurrentSetLastReturnedWins` | concurrent Sets linearize: the Set serialized last owns Current() |
| `TestSubscribeNeverMissesConcurrentSet` | a Set racing Subscribe is always delivered (no registration gap) |
| `TestGet` | binary-search lookup, presence and absence |
| `TestMembersCopy` | `Members()` returns a copy; View not mutable through it |
| `TestInputNotMutated` | `NewView` does not reorder/modify the caller's slice |
| `TestFeedsHashring` | `Ready()` ranks cleanly via hashring (Suspect excluded) |
| `TestStateString` | State stringification incl. unknown |
| `TestStaticProviderCurrent` | seeded current view |
| `TestStaticProviderSetBumpsEpoch` | `Set` publishes a new epoch/view |
| `TestSubscribeReceivesCurrentThenUpdates` | subscriber gets current then each update |
| `TestSubscribeCoalesces` | slow consumer sees only the latest; producer never blocks; buffer ≤1 |
| `TestUnsubscribe` | cancel stops delivery and is idempotent |
| `TestConcurrentSetAndCurrent` | lock-free reads under concurrent writers + subscriber (`-race`) |
| `TestDynamicConfirmCreditsResponderNotAddress` | liveness credit goes to the answering member's ID, not the dialed address; a dead member whose address was recycled is forgotten after `ForgetAfter` |
| `TestDynamicUnidentifiedResponderEarnsNoCredit` | an anonymous responder refreshes nobody's clock |

## 8. Limitations & TODO

- **Kubernetes seeder**: implemented as the separate `providers/k8s` module
  (EndpointSlice watch → seed addresses; see [k8s.md](./k8s.md)). It is a
  `Seeder`, not a `Provider`: identity learning, the state machine and removal
  hysteresis stay here in `DynamicProvider`, unchanged across platforms.
- **Weight hysteresis**: `View` stores weights verbatim; smoothing/hysteresis is
  a provider-level concern (all members currently self-report weight 1, and the
  k8s seeder does not read weights from Kubernetes metadata yet), not part of
  the immutable snapshot.
- **Conflicting address reports are resolved by arrival order**: one `Refresh`
  asks both the seeds and the already-known peers, concurrently, and `Learn`
  takes the newest report for an ID unconditionally. If two live endpoints claim
  the *same* node ID with *different* addresses — an old pod lingering through a
  rollout, or its IP recycled by another pod — the address that ends up in the
  view is whichever reply landed last, so it can differ between nodes and between
  passes. The consequence is bounded: a request to a stale address fails and the
  circuit breaker routes around it, and the next refresh re-resolves. Making the
  merge deterministic (a tie-break, or preferring the address a node reports for
  itself over hearsay) needs a decision on which report should win, and is
  deliberately left open rather than guessed at.
