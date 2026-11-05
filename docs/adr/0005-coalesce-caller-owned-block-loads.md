# ADR-0005: Coalesce caller-owned buffered block loads through cache publication

- Status: proposed
- Date: 2026-11-05
- Supersedes: none
- Binds: docs/engine.md §3.3 / §5 / Stability Contract; docs/observability.md block-source and wire-byte accounting

## Context

Issue [#81](https://github.com/data-accelerator/dart/issues/81) demonstrates four
peer downloads of one cold 4 MiB block for four concurrent 1 MiB client ranges.
Origin-only singleflight cannot merge the earlier peer path. Shared work must
also preserve the handler/store lifetime established by ADR-0003, per-caller
credentials, cancellation and owned return buffers. Membership skew may route a
request back through a node at a greater relay depth.

## Decision

Coalesce buffered P2P misses by node-local block key and relay depth through
validation and cache publication, executing the load in its leader caller and
giving joined readers owned bytes without counting duplicate wire transfers.

## Consequences

The leader rechecks cache after admission. Waiters cancel independently; a
canceled leader lets a survivor retry with its own context. Failures are not
retained or imposed on another URL, while successful immutable bytes share like
cache hits. Store access remains inside an active serving caller. A shared
flight takes one immutable snapshot before returning the leader's owned bytes;
joined readers copy from that snapshot. Different relay depths remain separate
so forwarding can advance to its hop bound rather than forming a wait cycle.
Origin results are detached before publication because the origin singleflight
can share its backing array across these separate engine flights. Each caller's
single final latency observation includes earlier waits and retries, independently
of which caller executes the successful transfer.
Cut-through relay bodies retain their existing streaming behavior.

## Alternatives considered

Origin-only singleflight misses successful peer traffic. Coalescing the network
read alone leaves a duplicate-fetch window before cache publication. Detached
cache-writing workers would require an additional shutdown/join ownership
contract. Canceling every caller when the leader disconnects couples independent
reads. Dropping relay depth from the identity can deadlock a skewed forwarding
loop. Lowering block size merely hides duplicate same-block misses and does not
fix their coordination.
