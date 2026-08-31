# ADR-0001: Minimalist assumptions are contract

- Status: accepted
- Date: 2026-08-26 (backfilled; principle in force since docs/design-assumptions.md, PR #28)
- Binds: docs/design-assumptions.md (A1–A6), SECURITY.md, every package document

## Context

DART trades generality for simplicity and speed (target: 100 Gbps/node) by
designing every package under a small set of assumptions: a trusted read-only
origin as the single source of truth (A1), a trusted deployment network (A2),
uniform cluster configuration (A3), finite weights (A4), sane operator
configuration (A5), and a write-once cache (A6). During the 2026-08 audit
campaign, several reported "defects" were in fact behavior outside these
assumptions (e.g. origin-side content mutation, untrusted-network
credential steering) — see issues #6, #8, #13, and the discussion on #22.

Without an explicit rule, every such report re-opens the same design debate,
and well-meaning hardening PRs add defensive machinery that erodes the
simplicity the assumptions were adopted to buy.

## Decision

DART's documented assumptions are contract: behavior outside them is out of
scope **by design, not by omission**, and a violation must be answered with an
explicit configuration or contract extension — never with silent degradation
or with defensive code added inside the assumed boundary.

## Consequences

- Triage of any correctness/security finding must first check whether it
  contradicts an assumption; if so the fix is at most documentation, not code.
- Hardening that only helps outside-assumption deployments (allowlist network
  config, origin revalidation, per-entry TTL) is rejected by default; it needs
  an ADR of its own that weakens the relevant assumption (T2 trigger).
- design-assumptions.md becomes load-bearing: each entry states what it buys
  and what breaks on violation, and package docs link the entries they rely on.

## Alternatives considered

- **Defend everywhere** (validate origin bytes on read, optional mTLS,
  per-cache-entry TTL): rejected — recurring cost on the hot path and in
  complexity, to serve deployments the project explicitly does not target.
- **Handle violations case-by-case without a written rule**: rejected —
  re-litigates the same debate per issue and lets defensive code creep in
  review-by-review.
