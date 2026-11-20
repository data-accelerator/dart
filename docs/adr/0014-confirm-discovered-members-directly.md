# ADR-0014: Confirm discovered members directly before publication

- Status: accepted
- Date: 2026-11-20
- Supersedes: none
- Binds: docs/cluster.md dynamic discovery and Stability Contract

## Context

Issue #108 demonstrates that deleting an expired member is insufficient when
third-party rosters can immediately re-admit it with a new grace period.
Staggered observers can repeatedly resurrect departed identities, including an
old ID at an endpoint now occupied by a replacement. A restarted observer has
no expiration history and can learn the same obsolete identity again.

## Decision

Require direct identity evidence before publishing a dynamically discovered
member; treat third-party roster entries as unpublished probe candidates.

## Consequences

Candidates are probed on the next refresh and expire using the same retention
interval as members. Repeated hearsay cannot extend an existing entry's clock.
Recreating an expired candidate supplies no publication authority. Neither
placement nor served rosters include candidates. An identified response or an
inbound self-report admits a real rejoin, including asymmetric reachability.
Previously confirmed members retain the existing forgetting grace period.

No wire fields, epoch encoding, static-provider semantics or persistent state
change. Indirect discovery can require one additional refresh before admission.
Old observers in mixed deployments can still resurrect stale entries locally;
all observers must upgrade for cluster-wide convergence. Confirmation is direct
observation under the trusted-network contract, not cryptographic authentication.

## Alternatives considered

Shorter grace periods only accelerate oscillation. Address-based deduplication
mistakes routing metadata for identity and cannot handle ordinary departures.
Permanent local tombstones grow with historical identities and disappear on
restart; bounded tombstones eventually let old rumors restore active membership.
Durable or distributed tombstones and incarnation protocols add storage or wire
coordination when direct probing already supplies the evidence needed here.
