# ADR-0007: Select node-local cache backends without weakening class isolation

- Status: superseded by ADR-0009
- Date: 2026-11-09
- Supersedes: none
- Binds: docs/design-assumptions.md A3; docs/dart.md cache modes; docs/node.md cache assembly / Stability Contract; docs/store.md Tiered / Stability Contract; docs/observability.md occupancy

## Context

The library has a memory block store, but node assembly always opens and locks a
disk cache before optionally adding a memory hot set. A warm Hybrid still writes
disk on refill. Issue [#85](https://github.com/data-accelerator/dart/issues/85)
requests memory-only operation without writable cache storage. A single shared
memory LRU would let borrowed churn evict owned shards, weakening the policy
that underpins distributed cache capacity.

## Decision

Select node-local memory, disk or default Hybrid backends explicitly, sizing the
backing class budgets by cache-size and preserving owned/borrowed isolation and
TinyLFU admission in each backend.

## Consequences

Tiered stores use independent RAM stores or disk stores under the same policy.
Admission comparison and eviction remain atomic inside each backend. Memory mode
opens no cache directory, arena or directory lock, and ignores cache-dir and
mem-size. Disk mode ignores mem-size. Hybrid preserves its RAM front tier and
existing mem-size-below-one-block fallback. At least two whole-block slots are
required so both class budgets remain usable. Slabs allocate lazily; the capacity
bounds cached payload slots, not metadata, copies or process RSS. Memory occupancy
sums its two class budgets; Hybrid memory occupancy remains its front tier.

Backend choice and capacity may differ across nodes; addressing geometry and
protocol settings remain cluster-uniform. Keys, HTTP paths and placement do not
change, so no epoch bump is needed. All modes restart cold. Resource cleanup keeps
ADR-0003's handler/store safe join, including any directory lock when disk exists.

## Alternatives considered

Using a large Hybrid hot set retains disk write-through and directory dependence.
Using a RAM filesystem retains disk-arena and lock machinery. One unclassified
memory LRU sacrifices shard protection. Overloading cache-size=0 to mean memory
conflates capacity with backend selection and would change existing validation.
A new capacity flag is unnecessary: cache-size already names backing capacity,
while mem-size names the optional Hybrid front tier.
