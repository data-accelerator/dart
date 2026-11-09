# ADR-0010: Coalesce directed normal block loads across hops

- Status: accepted
- Date: 2026-11-09
- Supersedes: ADR-0008
- Binds: docs/engine.md routing / flights / Stability Contract; docs/peer.md block epoch / Stability Contract; docs/hashring.md order; docs/tracker.md local reader completion; docs/store.md lease ownership; docs/fetch.md exclusive fills; docs/node.md handler lifetime; docs/dart.md cache behavior; docs/observability.md wire accounting

## Context

Issue [#97](https://github.com/data-accelerator/dart/issues/97) demonstrates that
hop-keyed flights download the same block twice when local and downstream reads
overlap. A real HTTP regression counts two 4 MiB root-to-reader payloads with a
hot root and hedging disabled. Origin-only coalescing cannot remove this traffic.
The old fallback for a reader absent from a snapshot could choose a later-ranked
reader. Hop isolation avoided joining a loop but preserved the routing defect.

## Decision

Share normal block loads across hops using strictly earlier reader dependencies,
and separate terminal root supply from normal forwarding work.

## Consequences

Preserve ADR-0008's managed cache, exclusive fills, complete-block validation,
lease ownership, bounded pressure fallback, prefetch and cleanup guarantees.
Replace only its relay-depth flight isolation and related wire compatibility
claim. ADR-0009's fixed-owner root and role eligibility continue to apply.

The local reader candidates are eligible snapshot readers union eligible self,
deduplicated and ranked with common HRW weights; the tracker snapshot is unchanged.
The owner is prepended once. Every ordinary parent and fallback ancestor precedes
self. Under common weights, set additions/removals preserve relative order, so
ordinary forwarding cannot cycle even with different reader snapshots or fanout.
Without an eligible self position, only terminal root supply may be selected.

Normal loads use block identity and a pinned membership ordering epoch, not hop
or reader-set epoch. Outgoing block requests carry that epoch. On a cold normal
request, a different membership epoch declines before metadata probing or flight
admission; the caller may recover through terminal supply. Warm immutable blocks
remain usable. This handles real ordering changes such as weight updates; opposite
weights are not evidence of cycles caused by ordinary reader-set churn. Omitted
epochs on direct calls adopt the receiver's view; engine forwarding always sends
one. Hop and request deadlines remain independent limits. Coordinated rollout
follows the v2 boundary already established by ADR-0009.

Supply goes directly to cache/origin and never joins a waiting normal flight. If
A waits on B and B falls back to A as a backup root, merging the returning supply
with A's normal load would create a wait cycle outside the ordinary tree. A local
root fallback also goes directly to cache/origin, without an HTTP self-request.
Origin work remains shared across both modes and ordering epochs. Canceled
consumers and cache-admission declines retain the existing lease lifetime rules.
The trusted-cluster assumptions remain unchanged.

## Alternatives considered

Keeping hop in flight identity duplicates normal P2P transfers and masks unsafe
parent selection. Removing only the key field leaves that routing defect intact.
Merging supply with normal work introduces the backup-root dependency above.
Waiting for tracker publication is unnecessary: local inclusion preserves order.
Treating arbitrary opposing weights as ordinary snapshot churn confuses two
separate cases and provides no reason to duplicate same-epoch normal loads.
