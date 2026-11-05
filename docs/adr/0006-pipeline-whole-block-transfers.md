# ADR-0006: Pipeline whole-block transfers with bounded Range prefetch

- Status: accepted
- Date: 2026-11-05
- Supersedes: ADR-0005
- Binds: docs/engine.md §3.3 / §3.6 / §5 / Stability Contract; docs/peer.md §3.4 / Stability Contract; docs/fetch.md shared-flight lifecycle / Stability Contract; docs/node.md handler lifetime; docs/dart.md prefetch flags; docs/observability.md block-source and wire-byte accounting

## Context

Cut-through relays expose upstream bytes before the block succeeds. A late error
requires aborting downstream responses, loses the original error across hops and
complicates validation, failover and flight sharing. They still retain complete
cache buffers. Serial client-range refill leaves successive block transfers and
writes without overlap. Issue [#83](https://github.com/data-accelerator/dart/issues/83)
chooses simpler recovery with bounded cross-block rather than within-block overlap.

## Decision

Obtain and validate complete peer blocks before forwarding, and pipeline only
covering blocks of a client Range with a bounded ordered, caller-owned prefetch
window and non-blocking node-wide speculative byte/load admission.

## Consequences

Cold multi-hop first-block latency includes a full transfer per hop. Concurrent
covering-block requests can pipeline steady-state transfers. Peer handlers never
expand a requested block into their own prefetch window. Paths, identities,
placement, hop bounds, credential refusal and HTTP/1.1 framing remain interoperable.
The streaming internal APIs are removed; success responses have Content-Length.

The coalescing guarantees of ADR-0005 remain: block/depth flight identity prevents
skewed-loop waits, the leader rechecks cache, validated immutable results share
through best-effort publication, returned bytes are owned, failures are not retained
or imposed across different URLs, and canceled leaders permit live waiter retry.
Source/latency observations include the caller's wait/retry; wire bytes count once.
Prefetch workers belong to their Serve call and cancel/join before its exit,
preserving ADR-0003. Hedge contenders are joined before their load returns.
Shared origin flights count interested callers; the last departure cancels and
joins the backend before releasing its reservation, while live peers retain
ownership. This also applies through nested probe/fetch wrappers. A backend must
cooperate with cancellation for prompt exit; an uncooperative one retains its
owner and resources. One successful consumer accounts a shared origin transfer
even if its original leader canceled. Successful cache publications survive
cancellation. Expected peer lengths bound response reads to the block plus one byte before
validation, and warm peer reads count as cache hits.

The default request window is two blocks, including demand. Speculative payloads
reserve node capacity through consumption/discard, including completed results;
default limits are 64 MiB and sixteen outstanding loads/results. Foreground reads
bypass speculative admission so an exhausted budget cannot stall demand. These
are payload bounds, not total RSS bounds: stores, foreground reads, transport and
transient copies are separate. Window one disables prefetch. Errors are consumed
in block order and no block beyond the request is scheduled.

## Alternatives considered

Cut-through reduces cold-chain startup but retains partially committed recovery
states. Serial whole-block refill loses overlap; unlimited prefetch makes slow
clients grow memory and expands cancellation traffic. Releasing admission as soon
as a fetch completes fails to bound queued results. Recursive peer-side readahead
multiplies work at each hop. Detached cache-warming workers need a separate shutdown
owner and are not justified by in-range prefetch. Beyond-range speculation and
adaptive windows are deferred until workloads justify their complexity.
