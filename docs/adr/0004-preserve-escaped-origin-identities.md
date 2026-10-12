# ADR-0004: Preserve escaped origin identities independently of endpoint probes

- Status: accepted
- Date: 2026-10-12
- Supersedes: none
- Binds: docs/chunk.md Stability Contract; docs/fetch.md origin probes; docs/engine.md endpoint metadata

## Context

Decoded URL paths collapse distinct immutable origins into the same cache key.
A content digest can identify shared bytes across origins, but cannot establish
that both endpoints support Range or expose the same size metadata. Existing
ADRs preserve trusted, immutable origins; none specifies URL identity encoding.

Upstream reports: [#77](https://github.com/data-accelerator/dart/issues/77) and
[#78](https://github.com/data-accelerator/dart/issues/78).

## Decision

Preserve escaped path boundaries in fallback object identities and keep endpoint
capability probes separate from digest-addressed content deduplication.

## Consequences

Affected non-digest paths require coordinated node upgrades; stores restart
cold. Digest identities, ChunkKey construction and HRW golden vectors remain
unchanged. A new endpoint performs its own capability probe even when another
endpoint has already populated the digest cache.

## Alternatives considered

Decoding before splitting paths loses encoded slash boundaries. Stripping
separators conflates distinct paths. Digest-keying endpoint probes saves a
request but incorrectly shares endpoint-specific Range capability. Giving up
cross-origin digest block reuse would avoid that coupling at unnecessary cost.
