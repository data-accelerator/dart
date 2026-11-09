# ADR-0009: Anchor distribution at role-eligible owners

- Status: proposed
- Date: 2026-11-09
- Supersedes: ADR-0007
- Binds: docs/design-assumptions.md A3/A4; docs/cluster.md membership and epoch; docs/hashring.md placement and trees; docs/engine.md routing and metadata; docs/peer.md protocol; docs/store.md class budgets; docs/node.md assembly; docs/dart.md configuration; docs/k8s.md discovery; docs/observability.md diagnostics

## Context

Issue #95 requires compute peers without origin access and dedicated supply peers
without local clients. Ranking all Ready members for ownership cannot express
these roles. Ranking only readers can choose an origin-isolated root. Local Size
probes and unconditional origin fallback also prevent isolated consumers from
using even a fully cached remote source. ADR-0007 permits local cache backends
but requires both class budgets to have capacity.

## Decision

Declare ordinary, root-only and non-root member roles; select each chunk's
preferred root and owned replica holders from root-eligible Ready members using
HRW, and build the reader distribution below that owner. A root cache miss goes
directly to origin, without querying replicas or readers.

## Consequences

Ordinary peers can occupy any tree position. Root-only peers supply roots and
accept peer/control operations but no client data traffic. Non-root peers may
relay or consume data and never contact origin. Roles are authoritative member
configuration, independent of transient liveness. Direct self-reports replace
roles; third-party hearsay cannot overwrite a directly confirmed role.
Role changes affect agreement;
capacity/backend choices remain local. Ownership is independent of readers and
of temporary backup-root selection. Replica placement does not actively copy data.

A failed parent can be bypassed toward ancestors and eligible backup roots under
bounded attempts and deadlines. A root supply operation terminates at its local
cache or origin, never reentering the distribution tree. Normal misses and cache
admission declines are not peer failures. Reported upstream errors do not mark an
honest relay as a malformed-data sender. Cached immutable data remains usable
without contacting an owner. Snapshot disagreement retains bounded hop/lifetime
protection; singleflight keys must not introduce cyclic waits between routing modes.

Object metadata is delegated to a root chosen by a file-level key independent of
Size and reader membership. Endpoint-specific Size/Range probe state remains
separate from digest-shared bytes. Full origin isolation also covers passthrough
and Registry control traffic; no frontend may silently bypass role restrictions.

Memory, disk and Hybrid backend selection and cold restart semantics from
ADR-0007 continue. Owned-only and borrowed-only budgets are permitted; disabled
classes bypass publication without consuming the other class's capacity. Existing
TinyLFU, complete-block validation, bounded slot waits, lease lifetime and shared
uncached delivery from ADR-0008 remain. Role alone does not allocate cache space.
Transient receive storage requires bounded admission separately from persistent
capacity and speculative prefetch limits. Peer block requests do not expand
prefetch windows.

Membership epochs use FNV-1a over ASCII `dart-roles-v1` and NUL, followed by each
canonical member's ID, 0x1F, big-endian float64 weight, one role byte (ordinary=0,
root-only=1, non-root=2), and 0x1E; fmix64 finalizes the hash. State and address
remain excluded. Canonical duplicate ordering adds role ascending after the
existing ID/state-rank/weight ordering. Unknown role values must be rejected at
configuration and discovery boundaries, never interpreted as ordinary.

This changes placement scope, trees, epoch and peer capabilities even for an
all-ordinary cluster. Coordinated deployment is required; optional fields or
an epoch mismatch do not establish compatibility with old binaries. Hash64,
HRW scores and ChunkKey remain unchanged. Root-eligible members are assumed to
access all configured origins in their namespace; per-origin role matrices are
outside this contract.

## Alternatives considered

Probing replica caches on an owner miss adds latency to the normal cold path for
uncertain hit benefit. Querying recent readers adds reverse dependencies and
requires exceptional nonrecursive request semantics. Both are rejected, including
cache-only probes. Replica candidates remain failure-recovery supply endpoints.

Weight zero is not a role: the established weight contract normalizes nonpositive
weights to one. Implicit roles derived from cache capacity conflate local storage
policy with cluster routing. Automatic role changes on origin errors cause
placement churn. Active replication, cache-holder directories and mutable-origin
behavior are not introduced.
