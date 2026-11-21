# ADR-0015: Exchange self reports when published wire rosters match

- Status: accepted
- Date: 2026-11-21
- Supersedes: none
- Binds: docs/peer.md membership exchange, docs/cluster.md discovery, docs/node.md discovery assembly

## Context

All-to-all direct probes make departures and endpoint reuse observable. In a
converged cluster of N nodes, returning N members on each of N(N−1) probes
multiplies the serialization, transmission and ingestion of identical data.

## Decision

Allow a v2 caller to advertise a versioned digest of its current published
wire membership. The digest covers stable IDs, addresses, weights and roles
in canonical order. When it matches the responder's view, the responder may
send only its own complete entry from that same view. Keep the regular JSON
response and the ordinary bidirectional self-identification checks. Return the
full roster whenever the hint is missing, unknown or different.

## Consequences

Matching peers exchange one entry per probe. A stale or restarted observer
requests with its own current view, so a missing member causes a full exchange.
Each successful reply identifies the responder even on a reused address, and
an inbound self-report continues to count as direct contact. The existing
forgetting interval, epoch, probe schedule and discovery candidates keep their
semantics. During churn or mixed-version operation, full responses may still
be needed on every exchange.

## Alternatives considered

The placement epoch deliberately omits addresses and could hide a move. A
validator remembering only the previous response from an address would not
notice that the caller had since forgotten a member. Sampling fewer peers
changes direct liveness and convergence latency. A dedicated status code or
an empty response would require a second source of authoritative self metadata.
