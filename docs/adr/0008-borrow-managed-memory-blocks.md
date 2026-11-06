# ADR-0008: Borrow managed memory blocks through validated publication and sending

- Status: accepted
- Date: 2026-11-06
- Supersedes: ADR-0006
- Binds: docs/store.md memory lifecycle / Stability Contract; docs/engine.md block ownership / prefetch / Stability Contract; docs/fetch.md exclusive fills; docs/peer.md borrowed responses / bounded ingress; docs/node.md handler lifetime; docs/dart.md cache behavior; docs/observability.md memory pressure accounting

## Context

Issue [#87](https://github.com/data-accelerator/dart/issues/87) identifies whole-block
allocation and copying on memory hits under a shared mutex, receive-buffer growth,
and another copy during cache insertion. Concurrent small ranges multiply this
work. Large memory capacity removes disk I/O but does not remove those costs.
ADR-0006 establishes complete-block validation and request-owned bounded prefetch;
its caller-owned byte results require copying when concurrent readers share data.

## Decision

Manage memory payloads and reusable metadata in bounded arenas and indices, and
carry immutable reference-counted leases from exclusive receive reservations
through validation, cache publication, singleflight sharing and response writing;
allow bounded-wait, uncached buffered fallback when storage is unavailable.

## Consequences

Memory payload segments allocate lazily; slot descriptors, chunk descriptors,
indices and free/LRU links have fixed capacity. Chunk identity resolves to a local
descriptor and then a block slot. Payload copying and network I/O never hold the
metadata mutex. An unpublished reservation has one writer. Only complete validated
blocks may be published. Published bytes are immutable. Each reader or shared
result holds a lease; deletion hides the index immediately, and physical reuse
waits for the last release. Handles include slot generations. Existing Get and
owned peer-source APIs retain independent mutable copies for compatibility.

Owned and borrowed budgets remain isolated, with TinyLFU admission atomic with
victim selection. When all slots are filling or pinned, reservation waits at most
two milliseconds with cancellation, then permits temporary allocation and skips
cache publication. The wait is an initial fixed pressure policy, not a network
latency estimate. Admission rejection also skips publication. No separate pin
quota or adaptive backoff is introduced. Capacity bounds payload slots, not total
process RSS, temporary buffers, HTTP state or metadata. Pressure and temporary
buffer use are observable. All cache modes still restart cold; memory mode opens
no cache files, directories or locks. Disk and Hybrid retain their storage policy.

ADR-0006's whole-block and lifetime rules continue: peer success waits for a fully
validated block; ordered prefetch covers only the client's range and never recurses
at peer handlers. Window one is serial; defaults remain two covering blocks,
64 MiB and sixteen speculative loads/results, reserved through consumption or
discard. Demand bypasses speculative admission. A fixed request-owned worker set
and future ring are reused across blocks. Cancellation joins workers and hedge
contenders before releasing pins or returning. Store close follows ADR-0003.

Block flights retain relay depth to prevent skewed-loop wait cycles. Origin lease
flights share across depths, with independent consumers and a final-caller
cancel-and-join. A canceled block leader permits surviving waiter retry; failed
credentials or URLs are not imposed on a different caller URL. Completed failures
are not retained. Exactly one successful consumer accounts each shared transfer,
even when its first caller cancels. Endpoint-specific probes remain independent
of digest-keyed content. No background operation may access a released buffer.

The new optional direct-fill API delegates exclusive buffers without lower-layer
byte sharing; the engine performs lease sharing above it. Unsupported fetchers
and non-reserving stores use the compatible copy path. Expected lengths still
bound reads to one extra byte before rejecting oversized bodies. Captured breaker
generations govern peer outcomes. Hashes, keys, geometry, paths, headers, HTTP/1.1
framing and rolling interoperability are unchanged; no epoch bump is needed.

## Alternatives considered

Pooling independent Get copies retains allocation ownership and full-block copy
bandwidth. Returning unpinned live slices permits eviction to corrupt responses.
Keeping network I/O under a cache lock serializes clients. Absolute zero allocation
under exhaustion complicates recovery; temporary buffered responses are acceptable
in this exceptional state. Per-block goroutines and futures allocate in proportion
to file length, so prefetch reuses a bounded worker set. Shared-memory persistence,
NUMA placement tuning and custom vector or kernel zero-copy socket transmission
are deferred for separate evidence and lifecycle decisions.
