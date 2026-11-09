# `internal/peer`

DART's node-to-node block transport: an HTTP server that serves locally-held
blocks to other nodes, and a pooled client that fetches a block from a peer.

- Source: `internal/peer/peer.go`
- Tests: `internal/peer/peer_test.go`
- Import path: `github.com/data-accelerator/dart/internal/peer`

## 1. Overview

When many nodes read the same content, a node can pull a block from a peer that
already holds it instead of hitting origin. This package is the wire transport
for that pull. It is plaintext HTTP/1.1 (peer-plane encryption is delegated to
the CNI layer, per the design), which keeps `sendfile`/`splice` viable later.

This first cut is transport only: a peer serves whatever its local `Source`
holds and returns `404` otherwise. **Routing** — which peer to ask (placement
via `internal/hashring`) and the distribution tree — is wired in a later step;
this package does not decide who to contact.

## 2. Wire form

```
GET /peer/v2/block/<chunkKey-hex>/<blockIndex>
X-DART-Origin: <upstream url>   (optional; enables relay fetch-on-behalf)
X-DART-Hop:    <n>              (relay depth, for loop safety)
```

- `<chunkKey-hex>` is `store.BlockKey.Chunk` in base-16; `<blockIndex>` is
  `store.BlockKey.Block` in base-10. The index is an **unrestricted uint64 on
  the wire**: transport (`parseBlockPath`) accepts any uint64, and it is the
  relay-capable Source's job to reject indices that cannot be represented in
  signed range geometry before doing arithmetic with them — the engine's
  sources decline anything above `chunk.Config.MaxBlockIndex()` with a `404`
  (issue #52, see docs/engine.md §3.5). `StoreSource` never does range
  arithmetic with the index, so it needs no such check.
- When `X-DART-Origin` is present, engine sources verify that the wire chunk
  key agrees with the URL's object identity, namespace and block's chunk index.
  An inconsistent pair is a requester error and yields `404` before cache or
  upstream access; it must not quarantine an honest parent. Cache-only requests
  without this header remain valid.
- `X-DART-Origin` lets a relay-capable Source fetch a block it does not hold
  (via its own parent/origin); `X-DART-Hop` bounds relay recursion.
- `X-DART-Hop` is optional (absent = depth 0) and must be a non-negative
  decimal int; any other value — malformed, negative, or out of range — is
  rejected with `400` before the Source is invoked. Accepting a negative hop
  would weaken the engine's relay-loop bound (`hop >= maxHop`, incremented at
  each relay): a negative start would delay the cutoff, a huge negative one
  effectively forever. The bound itself (`maxHop`) is enforced by the engine,
  not here, so any non-negative value — including one at or past the bound —
  is a valid wire value at this layer.
- Responses: `200` + complete block bytes (`X-DART-Node`, explicit `Content-Length`), `404` if the peer cannot provide the block, `400`
  for a malformed path or an invalid `X-DART-Hop`, `405` for non-GET, `500`
  on a source error, and `502` +
  `X-DART-Upstream-Status: <origin code>` when a relay's origin fetch was
  refused (§3.6 — the peer is fine; the caller's credential is not).
- The path has no embedded URLs, so it is parsed from `URL.Path` (no `//` trap).

## 3. Public API

### 3.1 `type Source` / `func StoreSource(store.Store) Source`

```go
type BlockRequest struct {
    Key store.BlockKey // the block
    URL string         // X-DART-Origin: origin for relay; "" = transport-only
    Hop int            // X-DART-Hop: relay depth
}
type Source func(ctx context.Context, req BlockRequest) ([]byte, bool, error)
```

Returns a block for a peer request; `held=false` is a miss (`404`). `StoreSource`
adapts a `store.Store` (transport-only: serves only locally-held blocks, ignores
`URL`). A relay-capable Source (the engine's `PeerSource`) uses `req.URL` to
fetch-on-behalf via its own parent/origin, caching and serving the block.

### 3.2 `type Server`

```go
type Server struct {
    NodeID string // echoed as X-DART-Node
    Src    Source // required
}
```

An `http.Handler` for the wire form above: `ServeHTTP` dispatches
`/peer/v1/block` (and rejects invalid `X-DART-Hop` values, §2); unknown paths
get 404. Safe for concurrent use, as any handler must be.

### 3.3 `type Client` / `func NewClient() *Client`

```go
const (
    DefaultDialTimeout           = 1 * time.Second
    DefaultResponseHeaderTimeout = 10 * time.Second
    DefaultRequestTimeout        = 30 * time.Second
)
type Client struct {
    HTTP    *http.Client   // nil uses a pooled default
    Timeout time.Duration  // per-request bound; 0 = default, negative = rely on ctx
    Breaker *Breaker       // optional per-peer circuit breaking
}
func NewTransport(dial, responseHeader time.Duration) *http.Transport
func (c *Client) Get(ctx, addr string, req BlockRequest) (data []byte, held bool, err error)
```

There are **three** bounds, not one, because the failure modes differ by orders of
magnitude — and getting this wrong is what made an abruptly dead machine take
tens of seconds to route around:

| Bound | Covers | Why this value |
|---|---|---|
| dial (1 s) | the peer's **machine is gone** | A dead host sends no RST, so without an explicit bound the dial falls back to the OS SYN retry schedule (minutes on Linux). Peers are inside one cluster: failing to connect within a second means unusable. |
| response header (10 s) | the host dies while one of its connections sits in our **idle pool** — the write succeeds locally and nothing comes back | Must stay above a relay's legitimate time-to-first-byte, since a relay may have to reach its own parent or the origin before streaming. |
| request (30 s) | the whole exchange including the body | Bounds a slow but live transfer. |

Without the dial bound the other two are the only thing standing between a dead
peer and a 30-second stall on every read that routes through it.

Fetches a block from the peer at `addr` (`host:port`). `held=false` on a `404`;
`err` on transport/other-status failures. `NewClient` returns a client with a
keep-alive connection pool (`MaxIdleConnsPerHost=32`, HTTP/1.1 forced so the
data path stays zero-copy-friendly).

```go
srv := &peer.Server{NodeID: myID, Src: peer.StoreSource(myStore)} // mount on the peer port
c := peer.NewClient()
data, held, err := c.Get(ctx, ownerAddr, peer.BlockRequest{Key: store.BlockKey{Chunk: ck, Block: bi}})
```

### 3.3.1 Borrowed serving and direct ingress

```go
type LeaseSource func(context.Context, BlockRequest) (store.Lease, bool, error)
// Server.Borrow LeaseSource takes precedence over Server.Src.
func (c *Client) GetInto(ctx context.Context, addr string, req BlockRequest,
    dst []byte, validate func(context.Context, int64) error) ([]byte, bool, error)
```

`LeaseSource` transfers immutable pinned bytes to the server, including any lease
returned on error/decline. `Server.Borrow` releases the result after success or
write failure, without holding a store lock across the HTTP write. Existing Src
and StoreSource keep their owned-copy contract. Node assembly uses borrowed
serving; paths, headers, status and framing are identical.

`GetInto` receives directly into caller storage, applies GetValidated's length,
callback, timeout and captured-generation breaker rules, and returns a slice
aliasing dst only on successful complete validation. No caller buffer is retained
after return. On error dst may contain partial bytes and must not be published.
It reads at most len(dst)+1 bytes, without a growing payload allocation. The
known-length GetValidated compatibility API allocates one expected-size payload
buffer. Read-only sharing and cache publication belong to the engine (ADR-0008).

### 3.4 Validated whole-block reads: `Client.GetValidated`

```go
func (c *Client) GetValidated(ctx context.Context, addr string, req BlockRequest, expected int64,
    validate func(context.Context, int64) error) (data []byte, held bool, err error)
```

Reject negative expected lengths. A Content-Length above `expected` is rejected
without reading the body; otherwise read at most `expected+1` bytes (without
integer overflow). A short or oversized complete body returns `ErrBlockLength`,
no data and `held=false`, opening the current-generation circuit. Transport body
errors retain their transport classification. After exact length validation,
invoke the optional validator with the length and timeout-bounded request context.
`Get` uses the same transport with no expected-length bound; engine ingestion
always uses bounded `GetInto` or `GetValidated`. Misses, HTTP errors and interrupted bodies do
not invoke validation.
Validation failure returns no data and `held=false`, wraps the error and opens
the current-generation circuit before another request can record success. The
validator runs inline and must respect its context. Time and caller-owned memory
are O(expected block length), plus validator work. The engine supplies expected
geometry.

`Server` obtains its `Source` result before sending a success header or any block
byte. Failed loads produce a clean `500`, credential refusals a `502` with
`HeaderUpstreamStatus`, and declines a `404`. A failure writing an already valid
block still truncates its Content-Length response. See
[ADR-0008](./adr/0008-borrow-managed-memory-blocks.md) for the cross-package rule.

`UpstreamRefusedError` is the source error interface with `UpstreamStatus() int`;
`Server` uses it to preserve the origin refusal code without blaming the relay.

### 3.5 Per-peer circuit breaking: `Breaker`

```go
func NewBreaker(BreakerOptions) *Breaker  // {FailureThreshold, Cooldown, HalfOpenProbes, Now}
func (b *Breaker) Allow(addr string) bool
func (b *Breaker) RecordSuccess(addr string)
func (b *Breaker) RecordFailure(addr string)      // soft: spends one unit of budget
func (b *Breaker) RecordHardFailure(addr string)  // definitive dial/protocol failure: opens at once
func (b *Breaker) State(addr string) BreakerState   // closed | open | half-open (String() prints those words)
func (b *Breaker) Healthy(addr string) bool         // usable now (no probe reserved)
func (b *Breaker) OpenCount() int
var ErrCircuitOpen = errors.New("peer: circuit open")
```

Without a breaker, a node that is down is re-dialed on **every** block: each read
pays a connect timeout before falling back, multiplied by the subtree beneath it in
the distribution tree. The breaker remembers the failure so later requests fail
immediately with `ErrCircuitOpen` (no dial at all), while still probing so a
recovered peer is picked back up without operator action.

States: `closed` → (`FailureThreshold` **consecutive** failures) → `open` → (after
`Cooldown`) → `half-open`, admitting `HalfOpenProbes` probes. After the first
success, admission pauses until all admitted probes finish. All must succeed to
close the circuit; a probe failure re-opens it and restarts the cooldown.

**A `404` is not a failure.** It is a legitimate answer ("I do not hold that
block") and blocks legitimately 404 all the time in a distributed cache — counting
them would trip the breaker on perfectly healthy peers.

**Caller cancellation is not a failure either.** When the caller's own context
is aborted (a hedging loser, a client disconnect mid-body), `Get`/`GetValidated`
record *answered*, not a failure: the peer demonstrably responded, and charging
it would open circuits on healthy peers. (The half-open probe slot must also be
released, which recording answered does.) Our own per-request timeout firing
while the caller is still waiting still counts as a soft failure — the peer was
genuinely slow.

**The cooldown starts when the circuit opens**, not when the last in-flight
request dies: a late failure arriving while already open does not restamp
`openedAt`, so a trickle of late completions cannot pin the circuit open. Only
the Closed→Open threshold transition and a failed half-open probe stamp it.

**The peer map is hard-bounded at 4096 tracked addresses.** Past the cap,
creating an entry first sweeps entries that carry no information (closed, zero
failures, no probes in flight). If the sweep frees nothing — every tracked
address still carries failure state, as happens under failed-address churn in
a long-lived dynamic cluster — the **least-recently-active** entry is evicted
instead, so the map can never grow past the cap. Activity is stamped by
request paths only (`Allow` and the `Record*` reports); read-only observers
(`State`/`Healthy`/`OpenCount`) never stamp, so a metrics scrape cannot
launder stale entries into "recently used". Evicting a dirty entry resets
that peer's circuit to closed, which is safe: the victim is by construction
the address no request has touched for the longest time — almost always a
departed node — and if it is genuinely sick and still in rotation, the next
request re-creates the entry and re-accumulates the failures.

Failures are split by how conclusive they are, which `Client` classifies
automatically:

| Outcome | Example | Effect |
|---|---|---|
| answered | `200`, `404` | closes the circuit, clears the count |
| **soft** fail | timeout, unexpected status, truncated body | spends one unit of the budget; may be a transient hiccup |
| **hard** fail | the dial failed or timed out (`net.OpError` with `Op == "dial"`) | **opens immediately** |

A peer we cannot connect to at all is definitive rather than suspicious, so
spending the whole budget on it — five dial timeouts, serially — is exactly what
made a departed node slow to route around. Opening on one observation stays safe
because opening is cheap and **reversible**: the cooldown plus the half-open probe
restores a recovered peer with no operator action.

`GetValidated` records a validated protocol violation as a hard failure,
without first recording transport success. Every `Client` request (Get, GetValidated and FetchRoster) binds its result to the breaker entry and
generation captured when it starts. Opening a circuit or recovering from
half-open invalidates older results. Checking the permit and recording the
outcome occur under the same mutex: an old success cannot clear quarantine or
consume a newer half-open probe slot; an old failure cannot undo recovery.
Evicted entries are not recreated by their old requests' completions. Fresh
roster requests still bypass admission and can restore discovery reachability,
but do not consume data-probe slots or override outstanding probe outcomes.
The public `Allow`/`Record*` API remains an explicit, unscoped reporting API;
callers managing those reports themselves must synchronize their lifecycle.

Defaults: `DefaultFailureThreshold` 5, `DefaultBreakerCooldown` 5s,
`DefaultHalfOpenProbes` 1. `Options.Now` injects a clock for tests. The engine
additionally uses `Healthy` to route *around* an open peer when choosing a tree
parent (see docs/engine.md §3.8).

Idle-pool sizing (`MaxIdleConns` 512 global / `MaxIdleConnsPerHost` 32): the
global cap holds ~16 fully-idle peers' worth. Past that, global pressure evicts
the oldest idle connections — the cost is a re-dial (~1 RTT), never a failure.
Per-peer idle counts are far below 32 in practice and `IdleConnTimeout` reaps
them anyway, so the cap is deliberately not scaled to cluster size (which a leaf
transport constructor does not know).

### 3.6 Upstream refusal vs peer fault: `HeaderUpstreamStatus`

```go
const HeaderUpstreamStatus = "X-DART-Upstream-Status"
var ErrUpstreamRefused = errors.New("peer: upstream refused the supplied credential")
type UpstreamRefusedError interface { error; UpstreamStatus() int }
```

A relay fetches on the requester's behalf using the **requester's own** upstream
URL. When that is a presigned link whose signature has expired, the origin answers
401/403 through no fault of the relay.

Without a way to say so, the requester recorded a soft failure against the relay,
so a handful of blocks would open a circuit on a **perfectly healthy peer** — a
client-side credential problem ejecting a good node.

So a relay whose `Source` returns an `UpstreamRefusedError` replies `502` with
`X-DART-Upstream-Status: <origin code>`, and the client:

- returns an error wrapping `ErrUpstreamRefused` (the fetch genuinely failed), but
- records the outcome as **answered** — the peer did its job correctly.

Genuine relay faults (a `500` with no such header) still spend failure budget, so
the breaker keeps working; a control test asserts that.


### 3.7 Membership exchange: `GET /peer/v2/roster`

```go
const RosterPath = "/peer/v2/roster"
const HeaderPeerAddr = "X-DART-Peer-Addr"
type Roster struct { Epoch string; Members []RosterMember }
type RosterMember struct { ID, Addr string; Weight float64; Role cluster.Role }
type RosterServer struct {
    NodeID string
    Role cluster.Role
    Src func() Roster
    Learn func(id, addr string)
    LearnMember func(RosterMember)
}
// RosterServer.ServeHTTP answers GET RosterPath with Src() as JSON, and feeds
// the fetcher's self-identification headers to LearnMember (direct contact),
// falling back to the legacy identity-only Learn callback when unset.
func (c *Client) FetchRoster(ctx, addr, selfID, selfAddr string) (Roster, string, error)
func (c *Client) FetchRosterMember(ctx context.Context, addr string, self RosterMember) (Roster, string, error)
```

Membership rides the peer listener. Five properties are deliberate:

- **`Members` always includes the sender.** That entry is why a caller holding only
  an address makes the request: DNS and other seeds hand out addresses, never
  identities, and placement needs a stable `ID`.
- **The responder's own ID is returned separately** (second return value, from its
  `X-DART-Node` self-identification header). Liveness must be credited to that ID —
  never to the dialed address, which pod-IP reuse can recycle to a different member.
- **`Epoch` is a decimal string, not a JSON number.** An epoch is a full `uint64`
  and JSON numbers are doubles, so values above 2^53 would be silently corrupted by
  any conforming parser.
- **No `State` field.** Liveness is a local observation; propagating it would let one
  node's transient failure become cluster-wide churn.
- **`Learn` makes the exchange bidirectional.** The caller sends its ID and its
  *advertised* address (`X-DART-Peer-Addr`, which cannot be derived from the
  connection — `RemoteAddr`'s port is the ephemeral source port). Without the inbound
  half, the node that started first, and so had nothing to seed from, could stay
  isolated indefinitely.

**`FetchRoster` does not consult the circuit breaker for admission**, unlike a block
fetch, though it still records outcomes. Skipping a peer whose circuit is open is
free for a block — there is a grandparent and an origin — but for a roster there is
no alternative source, so refusing to ask is refusing to ever learn about that node.
A cluster booting together *always* has nodes dialing peers that are not listening
yet, and a dial failure opens a circuit on the first attempt; gating discovery on
that stalls convergence, and with a cyclic seed list it stalls every node at once.
This was observed in a live 3-node run. Because outcomes are still recorded, a
successful current-generation roster fetch can close a recovered peer's circuit
when no data probes remain outstanding. It never consumes their slots or overrides
their pending outcomes, so discovery also restores reachability without hiding a
data-probe failure.


## 4. Invariants & Guarantees

1. **Faithful transport**: a `200` returns exactly the block bytes the source
   provided; a miss is a clean `404` (not an error).
2. **Held-vs-error distinction**: `Client.Get` returns `held=false, err=nil` for
   a miss and `err!=nil` only for transport/protocol failures — callers can fall
   back to the next candidate or origin without conflating the two.
3. **StoreSource does no routing**: a plain `StoreSource` serves only what the
   local store holds and never fetches on a requester's behalf. Routing exists
   one layer up: the engine may wrap the source in a relay-capable one (§3.6)
   that fetches from its own parent/origin when the block is missing — that is
   opt-in per node, bounded by `X-DART-Hop`, and reports refusals as 502 +
   `X-DART-Upstream-Status` rather than as peer faults.

## 5. Concurrency & Call Permissions

- `Server` is safe for concurrent use if `Src` is; `StoreSource` is (the store
  is concurrency-safe).
- `Client` is safe for concurrent use; its pooled transport reuses connections
  across goroutines. Verified with `-race`.
- Response bytes are read fully into a caller-owned buffer or exclusive reserved
  slot; borrowed server responses remain pinned through the synchronous write.

## 6. Determinism / Stability Contract

- The wire path (`/peer/v2/block/<hex>/<dec>`) is part of the peer protocol.
  Changing it is a peer-protocol version bump.
- Peer-plane traffic is plaintext by design; encryption is a CNI concern.
- ADR-0008 requires full validated engine blocks before peer success is committed.
  Server responses use Content-Length; clients also accept older chunked peers.
  Whole-block forwarding preserves paths, keys and epoch framing. The removed
  streaming APIs are internal and have no compatibility shim.

## 7. Testing

Managed-memory regression coverage:

| Test | Property |
|---|---|
| `TestGetIntoBoundedAndGeneration` | destination aliasing and invalid-length quarantine |
| `TestLeaseServerReleasesOnWriteError` | failed borrowed HTTP writes release their pin |


- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **92.4%** of statements (Go 1.27.0). Uncovered lines are a couple of rare
  I/O-error branches.
- **Note**: tests use `t.TempDir()` (store files); in the sandbox export
  `TMPDIR=$PWD/.gotmp`.
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./internal/peer/ -v -count=1
go test ./internal/peer/ -race -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestServerClientRoundtrip` | store → server → client returns exact block bytes |
| `TestClientMiss` | a not-held block yields `held=false, err=nil` (404) |
| `TestServerNodeHeader` | `X-DART-Node` echoed on responses |
| `TestServerBadPathAndMethod` | malformed paths → 400/404; non-GET → 405 |
| **`TestServerHopValidation`** | **both servers: absent hop = 0; non-negative hops (incl. ≥ the engine's relay bound) accepted; negative/malformed/overflow hops → 400 and the Source is never invoked** |
| `TestParseHop` | decoder edges: empty, leading plus, `-0`, overflow |
| `TestServerSourceError` | a source error → 500 |
| `TestClientConnError` | a closed peer port surfaces a transport error |
| `TestParseBlockPath` | path parsing incl. max uint64 and rejects |
| `TestConcurrent` | pooled client + server under concurrent load (`-race`) |
| `TestClientLateFailureCannotUndoRecovery` | an old failed request cannot reopen a circuit recovered by a current half-open probe |
| `TestGetValidatedUsesRequestContext` | validation receives the bounded peer request context and the complete body length |
| `TestClientLateSuccessCannotClearProtocolQuarantine` | deterministic barriers cover stale Get/GetValidated/roster success during open and half-open states; only a current probe can recover the circuit |
| **`TestClientRequestTimeout`** | **a stalled peer cannot hold `Get`/`GetValidated` open** |
| `TestClientTimeoutDisabled` | a negative `Timeout` defers to the caller's context |
| `TestBreakerOpensAfterThreshold` | opens only at the consecutive-failure threshold |
| `TestBreakerSuccessResetsFailures` | intermittent failures never accumulate into an open circuit |
| `TestBreakerHalfOpenRecovery` | cooldown → half-open admits one probe → success closes |
| `TestBreakerHalfOpenFailureReopens` | a failed probe re-opens and restarts the cooldown |
| `TestLateFailureDoesNotExtendCooldown` | a failure arriving while open does not slide the cooldown; a failed probe does |
| `TestBreakerSweepsCleanEntries` | the peer map is bounded: clean entries are swept past the cap, dirty ones kept |
| **`TestBreakerCapHardUnderFailedChurn`** | **the cap is a HARD bound even when every churned address carries failure state (below-threshold / open / half-open); recently-touched addresses keep their breaker behavior** |
| **`TestBreakerEvictionDropsStalest`** | **eviction picks the least-recently-active entry; an actively routed-to sick peer never loses its open circuit** |
| `TestCallerCancelNotChargedToPeer` | a caller-aborted read records answered, not a soft failure |
| `TestBreakerIsolatesPeers` | one sick peer does not affect others |
| `TestBreakerDefaultsAndStateNames` / `TestBreakerConcurrent` | defaults; concurrency (`-race`) |
| **`TestClientBreakerShortCircuits`** | **an open circuit stops dialing entirely (server sees no further requests)** |
| **`TestClientBreakerIgnores404`** | **20 consecutive misses leave the circuit closed** |
| `TestClientBreakerRecovers` | a recovered peer is used again after the cooldown |
| **`TestBreakerOpensOnFirstHardFailure`** | **an unreachable peer is routed around after ONE attempt, not after the full threshold** |
| `TestClassifyDialFailureIsHard` | dial failure → hard, `500` → soft, nil → answered |
| `TestSoftFailureStillNeedsThreshold` | a reachable-but-erroring peer is not ejected on one hiccup |
| `TestBreakerHardFailureRecovers` | opening on one observation is reversible via cooldown + probe |
| `TestTransportTimeoutsConfigured` | the dial bound is actually set (no fallback to OS SYN retries); HTTP/2 stays off |
| `TestResponseHeaderTimeoutBoundsSilentPeer` | a peer that accepts and never answers is cut loose well before the request timeout |
| **`TestRelayUpstreamRefusalDoesNotBlamePeer`** (engine) | **5 expired-signature refusals leave the relay's circuit closed at threshold 3** |
| `TestRelayGenuineFailureStillBlamesPeer` (engine) | the control: 3 genuine 500s still open the circuit |
| `TestRelayRefusalPropagatesStatus` (engine) | the relay returns 502 + `X-DART-Upstream-Status` |
| `TestRosterRoundTrip` | a caller with only an address learns the responder's ID and its known peers |
| `TestRosterCarriesNoState` | liveness never appears in the JSON; unknown fields decode cleanly |
| **`TestRosterEpochIsStringForPrecision`** | **a uint64 epoch survives the round trip; as a JSON number it would not** |
| `TestRosterLearnsCaller` | the inbound half fires; an anonymous caller teaches nothing |
| `TestRosterAdvertisedAddrNotRemoteAddr` | the learned address is the advertised one, not the ephemeral source |
| `TestRosterServerNamesItself` | a responder omitting itself is repaired from the header |
| **`TestRosterFetchIgnoresOpenCircuit`** | **discovery proceeds through an open circuit and closes it on success** |
| `TestRosterFetchRecordsFailure` | a failed roster dial still opens the circuit for the data path |
| `TestRosterServerRejectsWrites` / `TestRosterServerNoSource` / `TestRosterFetchBadBody` | method, misconfiguration and malformed-body handling |
| `TestDeadPeerRoutedAroundQuickly` | 200 further attempts at a dead peer cost negligible time |

| `TestGetValidatedBoundsPeerBody` | advertised and unknown-length excess bodies are rejected before exceeding expected+1 bytes, without invoking later validation |

## 8. Limitations & TODO

- **Routing lives in the engine**: this package is the transport; the engine
  decides who to ask (parent in the preorder tree over `cluster.Ready()`) and
  provides the relay `Source` (`engine.PeerSource`). Membership skew is bounded
  by `X-DART-Hop`; full epoch convergence is future work.
- **Whole-block only**: a complete validated block is transferred as one
  Content-Length body. Cold multi-hop startup waits for each whole-block load;
  concurrent covering-block requests supply steady-state pipelining (ADR-0008).
- **Epoch convergence**: `X-DART-Epoch` is defined but not yet exchanged/acted
  on; membership-skew convergence is a later addition.
- **Zero-copy**: the server currently `Write`s a buffer; serving via
  `sendfile`/`splice` from the store slot is future optimization.

Half-open regression tests: `TestHalfOpenProbesWaitForSiblingOutcome` covers
concurrent recovery and a sibling hard-failure veto;
`TestHalfOpenRosterSuccessCannotConsumeDataProbe` covers roster slot ownership.

### Role-aware roster exchange

As specified by ADR-0009, `RosterMember.Role` carries the numeric cluster role.
`HeaderPeerRole` (`X-DART-Peer-Role`) and `HeaderPeerWeight`
(`X-DART-Peer-Weight`) carry the caller's self-report; the response role header
also preserves self-role information when the body omits the responder entry.
`RosterServer.Role` supplies that fallback. `RosterServer.LearnMember` receives
validated direct reports and takes precedence over the identity-only `Learn`
adapter. `Client.FetchRosterMember` sends the complete report, preserving the
existing timeout and discovery breaker-admission bypass. `FetchRoster` remains
an ordinary-role convenience wrapper. Unknown numeric roles and invalid caller
weights are rejected before learning. Identified callers must explicitly send
their role; every response member must carry a non-null role, and the response
role header must be present and consistent with the responder's unique entry.
Anonymous diagnostic GETs may omit caller headers but do not teach membership.
The v2 roster path is a coordinated-upgrade
boundary, not implicit compatibility with the old v1 discovery protocol.

`TestRosterRolesBothDirections` verifies caller roles and reconstructed responder
roles over real HTTP; `TestRosterRejectsUnknownRoles` verifies both ingress paths.
`TestRosterRequiresExplicitConsistentRoles` rejects missing/null capabilities,
conflicting self-identification and duplicate responder entries.

### Terminal supply and metadata

ADR-0009's block endpoint uses v2. `BlockRequest.Supply` is encoded as
`HeaderSupply` (`X-DART-Supply: 1`); other nonempty values are rejected before
source invocation. The engine enforces terminal cache/origin service and root
eligibility. This flag is part of routing, not an authorization boundary.

`MetadataPath` (`/peer/v2/metadata`) accepts GET with `HeaderOrigin`.
`MetadataServer.Probe` returns `Metadata` (Size and RangeUnsupported); the callback
must be terminal and root-eligible. `Client.Probe` applies its timeout, bounds the
JSON response to 4096 bytes and rejects missing or inconsistent fields. Metadata
is endpoint-specific even when block bytes share a digest identity.

`SourceFailureError` marks a healthy source reporting an upstream/path failure.
`HeaderSourceFailure` carries it as `X-DART-Source-Failure: 1`; clients return
`ErrSourceUnavailable` while preserving the relay's breaker health. HTTP origin
statuses retain `HeaderUpstreamStatus`, including a typed fetch.StatusError for
credential retry logic. `TestSourceFailureDoesNotQuarantineRelay` verifies this
boundary over a real HTTP connection.
