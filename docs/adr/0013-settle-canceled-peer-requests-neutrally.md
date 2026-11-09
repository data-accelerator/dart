# ADR-0013: Settle canceled peer requests without inferring health

- Status: accepted
- Date: 2026-11-09
- Supersedes: none
- Binds: docs/peer.md circuit breaking and delegated-origin lifecycle

## Context

[#104](https://github.com/data-accelerator/dart/issues/104) demonstrates that an
already-canceled request can close a half-open circuit without issuing HTTP.
Releasing a probe slot by reporting success conflates admission cleanup with
positive health evidence. Caller abandonment is common during hedging and must
neither quarantine healthy peers nor erase real failures.

## Decision

Settle caller-aborted peer requests neutrally, releasing their own admission
without supplying success or failure evidence, and recover a half-open cohort
only after a genuine current-generation answer and completion of every admitted
data probe without failure.

## Consequences

All-canceled cohorts remain half-open and can admit new probes. A real success
freezes further probe admission until its siblings finish; cancellation of the
last sibling can complete that recovery. A failure invalidates older permits.
Roster discovery still bypasses admission and cannot consume data-probe slots.

Block, roster and delegated-origin caller deadlines share this policy. Closing
an unfinished delegated-origin body is neutral and settles exactly once.
HEAD and explicitly empty responses can complete at headers without a body read.
Client-owned header or idle-read deadlines remain failures. Completed origin
responses, including source refusals and source-body errors, remain honest peer
answers: origin stream integrity is not cached-block protocol integrity.
No peer wire fields, routing order or configuration change is required.

## Alternatives considered

Treating cancellation as success falsely recovers unreachable peers. Treating it
as failure penalizes hedging losers and disconnected clients. Skipping settlement
leaks probe slots. Requiring every admitted probe to succeed prevents recovery
when a real success is followed by canceled siblings; retaining that success
within the bounded cohort provides recovery without losing a sibling's veto.
