# ADR-0003: Shutdown safe-join — never close the store under a live handler

- Status: accepted
- Date: 2026-08-26 (backfilled; implemented in PR #48, issue #45)
- Binds: docs/node.md §3.1 (handler-lifetime contract), internal/node serverSet/finishRun

## Context

On a timed-out graceful drain, `http.Server.Close` closes connections but does
not join handler goroutines. The store's own contract says it "must not be
used" after `Close`. A shutdown path that returns with a handler still live —
and then runs the deferred store close — creates use-after-close. Issue #45
also found the tracking primitive in use (atomic.Bool + sync.WaitGroup)
formally outside the WaitGroup contract: an `Add` after the counter hit zero
raced `Wait`.

## Decision

Every server handler runs behind a closeable admission gate (admission and
counting serialized with gate closure under one lock); on shutdown the store
and cache-dir lock are closed only after every admitted handler has exited —
if the final join grace expires with a handler still live, the process returns
with those resources deliberately left open rather than closed under the
handler.

## Consequences

- Late handler entries get a 503 at the gate without touching the store;
  entries that passed the gate are already counted and are waited on.
- The abandon path leaks the store and the directory lock by design: the
  shipped commands exit right after `Run` returns, so the OS reclaims them,
  and the held flock prevents another node from opening the cache dir while a
  handler may still write.
- In practice every DART handler unwinds on request-context cancellation, so
  the abandon path is only reachable by a handler that ignores cancellation
  past 10s drain + force-close + 2s grace.

## Alternatives considered

- **Unbounded join** (wait forever for entered handlers): rejected — a handler
  wedged despite connection close and context cancellation would hang shutdown
  indefinitely, inviting SIGKILL with resources in an unknown state; that is
  worse than a deliberate, documented leak.
- **Bounded wait, then close anyway** (the pre-#48 behavior): rejected — it
  closes the store under a potentially live handler, contradicting the store's
  own contract while claiming the opposite in the lifecycle documentation.
