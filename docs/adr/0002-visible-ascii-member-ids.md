# ADR-0002: Visible-ASCII member IDs instead of an epoch reframe

- Status: accepted
- Date: 2026-08-26 (backfilled; implemented in PR #44, issue #21)
- Binds: docs/cluster.md §3.3.1 / §6, internal/node flag parsing (`-self-id`, `-peers`)

## Context

The cluster epoch is a hash over a serialization that frames member fields
with the control bytes 0x1F (unit separator) and 0x1E (record separator). A
member ID containing those bytes could make two genuinely different
memberships serialize identically — two nodes would believe they agree on the
epoch while disagreeing on membership, and placement would silently diverge.
A crafted two-byte ID was shown to collide a real two-member view with a
crafted one-member view (regression: TestEpochCollisionInputsRejected).

## Decision

Member IDs are restricted to visible ASCII — every byte in [0x21, 0x7E] —
enforced at all ingress points (`-self-id` validation, `-peers` parsing, and
roster ingest via `ValidMemberID`); the epoch serialization format is left
unchanged.

## Consequences

- No wire-protocol change, no golden regeneration, no epoch bump: mixed
  clusters keep working through the rollout.
- Rejected roster IDs are reported through `DynamicConfig.OnError` (PR #47)
  so a mixed-version cluster surfaces a diagnostic instead of a silently
  missing member.
- Future non-ASCII ID schemes are excluded until a superseding ADR reframes
  the serialization deliberately (which is then a T1-triggered change).

## Alternatives considered

- **Reframe the serialization** (length-prefixing or escaping instead of
  control-byte framing): rejected — a breaking wire-protocol change (epoch
  bump, golden regeneration, rolling-upgrade hazard) to defend against an
  input class that should never legitimately occur. Confirmed by the
  maintainer during issue #21 triage: ingress restriction preserves the
  framing guarantee without a protocol change.
- **Sanitize IDs at ingest** (strip control bytes): rejected — two distinct
  IDs could sanitize to the same string, recreating the collision one level
  up and hiding it from operators.
