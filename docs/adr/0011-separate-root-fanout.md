# ADR-0011: Configure root fanout independently of reader branching

- Status: accepted
- Date: 2026-11-09
- Supersedes: none
- Binds: docs/design-assumptions.md A3; docs/hashring.md tree navigation and Stability Contract; docs/engine.md routing and Stability Contract; docs/node.md assembly; docs/dart.md configuration

## Context

Issue [#100](https://github.com/data-accelerator/dart/issues/100) separates the
preferred owner's first-hop replication from subsequent reader distribution.
Even a group of fast root-capable nodes can become an aggregate cluster hotspot.
A uniform branching factor cannot reduce normal owner sends without also
narrowing the entire reader tree. The chosen default is one root child, with
two children at subsequent nodes.

## Decision

Use a common cluster root-fanout setting, defaulting to one, at the preferred
chunk owner and the existing fanout setting, defaulting to two, at every
descendant.

## Consequences

The owner stays fixed by root-eligible HRW placement. Eligible readers retain
their HRW order, including local self completion. Partition descendants into
up to root-fanout balanced contiguous segments once; recursively partition
each segment using fanout. The distinction is tree position, not member role:
a root-capable ordinary peer in a descendant position uses fanout. Root-only
peers remain excluded from reader descendants.

Parent, child and depth navigation use the same partitions and remain mutually
consistent. Every ordinary parent and fallback ancestor strictly precedes its
child. Reader-set disagreement and different positive degree settings cannot
reverse that order under common weights. Cross-hop flight sharing and terminal
supply separation are unchanged. Hash64, HRW scores, chunk keys, member epochs
and peer wire framing are unchanged; changing degree does not require another
ordering epoch. Both values must nevertheless agree across the deployment to
obtain a common tree, and neither is a per-node discovery capability.

The root degree bounds only direct children in a shared normal tree, not actual
sends under hedging, failed ancestors, terminal supply or reader churn. Disjoint
reader snapshots can each place a different reader directly below the owner.
Failure recovery and receive-budget admission remain unchanged. The first
reader bears more aggregation work, and a narrower first level may increase
whole-block first-read latency or concentrate an outage on that subtree.
Chunk-dependent HRW reader ranking spreads that responsibility across chunks.

The CLI requires a positive root-fanout and defaults to one; the engine API
zero value selects one and negative values are rejected. Existing uniform tree
helpers retain their behavior. Explicitly setting root-fanout equal to fanout
restores the uniform topology. Deploy compatible binaries to all participants
before using the new common configuration. The role/v2 coordinated-upgrade
boundary remains separate and unchanged.

## Alternatives considered

Defaulting root-fanout to fanout preserves the prior topology but does not
address aggregate root hotspots without configuration. The selected default
trades a narrower first hop for less normal root replication.

Setting fanout only on root hosts cannot express this policy: readers compute
their own parent. Per-member branching capabilities would add discovery and
agreement complexity without being needed for a tree-position distinction.

Strictly enforcing a root send quota would require separate admission/waiting
policy and constrain healthy failure recovery. This feature expresses topology
only; it does not introduce such a quota.
