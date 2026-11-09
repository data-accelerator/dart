# ADR-0012: Publish reader growth immediately and refresh on bounded demand

- Status: accepted
- Date: 2026-11-09
- Supersedes: none
- Binds: docs/tracker.md publication, integration and Stability Contract; docs/engine.md reader refresh and Stability Contract; docs/observability.md topology metrics; docs/dart.md tracker-tick

## Context

Issue [#102](https://github.com/data-accelerator/dart/issues/102) identifies two
startup delays: freezing the first JOIN's singleton reader set until a tracker
tick, then retaining that response in client caches for half a reader lease.
Local self completion keeps routing acyclic but cannot discover other readers.
Even with root fanout one, many different singleton snapshots can send all
readers directly to the root. Normal reader forwarding already follows strictly
earlier HRW ranks under common member weights; correctness does not require
synchronized reader snapshots or frozen topology.

## Decision

Publish newly joined readers before replying, batch departures on ticks, and
refresh client snapshots only on routing demand with shared per-object/view
JOINs and time-based doubling from 50 ms to half the granted lease.

## Consequences

Renewing an already published reader changes no publication epoch unless a due
removal sweep changes the set. New JOINs do not postpone removal processing;
last-reader deletion and idle eviction retain their immediate/lazy semantics.
Snapshots remain sorted copies, and publication epochs remain local to one
tracker file entry. JSON and peer wire framing do not change.

An active client's successful refresh intervals double independently of reader
list changes. Short grants cap even the initial interval; missing grants retain
a two-second ceiling. Expired leases or a different tracker membership view
restart the schedule. There are no background refresh workers. Waiters may
cancel independently; a canceled leader can be replaced without publishing its
late result. Failed JOINs are shared but not cached. A response started before
a more recently installed view cannot overwrite that view.

A synthetic warm-root experiment supports repeated early refresh for gradual
arrivals, while a single early refresh is cheaper for completed bursts. Fifty
milliseconds is a starting engineering choice, not an optimal cluster constant.
Late arrivals after the schedule reaches its ceiling still wait for the next
routing lookup. Refresh traffic scales with active node/object pairs; this
change makes no thousand-reader control-plane scalability claim.

Expose low-cardinality counters for dispatched root/parent/ancestor block
attempts and a histogram of reader counts in successful JOIN replies. Exclude
pre-dispatch rejections, local hits and coalesced JOIN waiters. Attempts are not
wire-byte counters; raw reader counts are not filtered tree sizes.

The preferred owner, local self completion, strict earlier-rank forwarding,
normal cross-hop coalescing, terminal supply flights and ordering-epoch boundary
remain unchanged. Mixed versions retain read correctness but obtain only
partial startup performance improvement; coordinated upgrade is recommended.

## Alternatives considered

- Tick-frozen growth and normal half-lease refresh preserve the initial root
  bottleneck despite a healthy, warm root and available reader peers.
- A single early refresh has lower control-plane cost for bursts but can finish
  before later readers join; simple doubling also covers gradual startup.
- Repeated fixed fast polls continue spending requests after startup. Background
  refresh also spends requests on completed reads.
- Notifications, gossip convergence, reader-change-triggered resets and global
  synchronization add state or RPC dependencies that ordered forwarding does
  not need. This remains a trusted-cluster design.
- Per-object or per-peer metric labels have unbounded operational cardinality;
  aggregate topology classes and snapshot-size observations suffice here.
