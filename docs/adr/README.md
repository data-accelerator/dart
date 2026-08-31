# Architecture Decision Records

This directory holds DART's **Architecture Decision Records (ADRs)** — the
project's legislative history. Each ADR captures one contract-level decision:
the forces behind it, the rule it establishes, and the alternatives that were
considered and rejected.

## Two layers, one source of truth each

- **Current law** lives in `docs/design-assumptions.md`, the package documents
  (`docs/<pkg>.md`), and `AGENTS.md`. Those files always describe the rule as
  it stands *today*; reading them is sufficient to work on the code.
- **Case law** lives here. An ADR is a frozen snapshot of *why* a rule exists
  and what was rejected. Once accepted, an ADR's body never changes (status
  lines, links, and typos excepted).

When the current-law wording is ambiguous, the **newest accepted ADR wins**.

## When an ADR is required (trigger list)

An ADR file must ride in the same PR as the change it governs when the change:

- **T1** — modifies the wire protocol (`Hash64`, the HRW `score` formula,
  `ChunkKey`, or the epoch framing bytes);
- **T2** — adds, drops, or weakens an entry in `docs/design-assumptions.md`;
- **T3** — changes the Stability Contract section of any package document;
- **T4** — establishes a cross-package convention: a rule that **two or more
  packages depend on** and that is not written in any one package's contract
  sections (e.g. "no `init()` side effects", "user callbacks are never invoked
  while holding a lock");
- **T5** — rejects a significant alternative without adopting it
  (decision-by-exclusion, e.g. "DART does not ship allowlist-style network
  configuration"); the ADR exists so the alternative is not re-proposed.

An ADR is **not** required for: bug fixes that restore documented behavior,
test additions, documentation clarifications, or internal refactors that
preserve contracts. Bureaucracy resistance is a design goal: a one-paragraph
ADR is a good ADR.

## Format

Files are `NNNN-kebab-title.md`; numbers are zero-padded, sequential, and
**never reused** — including rejected records. Reserve the number in the index
table below inside the same PR that adds the file.

```markdown
# ADR-NNNN: <imperative, decision-shaped title>

- Status: proposed | accepted | superseded by ADR-XXXX | rejected
- Date: YYYY-MM-DD (of the status)
- Supersedes: <ADR-XXXX or "none"> (required: the lifecycle's supersession
  audit keys off this line; it must pair with the old record's status change)
- Binds: <files/sections this decision constrains>

## Context
<forces and constraints; link issues/PRs>

## Decision
<one enforceable, imperative sentence — the load-bearing line>

## Consequences
<what becomes easier/harder; which code paths are bound>

## Alternatives considered
<rejected options and why — the anti-relitigation record>
```

## Lifecycle

```
proposed ──(PR merges)──> accepted ──(a new ADR overrides it)──> superseded
    └──(review declines)──> rejected (kept on file)
```

- **proposed → accepted** happens when the PR carrying the ADR merges. The
  decision and its implementation land atomically and are reviewed together.
- **Weakening or reversing** an accepted ADR requires a **new ADR** whose
  `Supersedes` line names the old one. The superseding PR must update every
  document in the old ADR's `Binds` list — that list is the audit surface.
  The old file stays, its status becomes `superseded by ADR-XXXX`.
- **rejected** records are kept permanently: they are the strongest
  anti-re-proposal material the project has.
- If two PRs race for the same number, the merge conflict in the index below
  forces a renumber. At this repository's change rate that is sufficient.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](./0001-minimalist-assumptions-as-contract.md) | Minimalist assumptions are contract | accepted |
| [0002](./0002-visible-ascii-member-ids.md) | Visible-ASCII member IDs instead of an epoch reframe | accepted |
| [0003](./0003-shutdown-safe-join-contract.md) | Shutdown safe-join: never close the store under a live handler | accepted |
