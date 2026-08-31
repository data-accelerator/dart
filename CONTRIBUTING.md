# Contributing to DART

Thanks for your interest in DART. This document covers what you need to know
before opening a pull request.

By contributing you agree that your contribution is licensed under the
Apache-2.0 license, per section 5 of [LICENSE](./LICENSE).

## Getting started

DART has **no external dependencies** and no code generation. A recent Go
toolchain is all that is required (the module targets `go 1.22`):

```bash
git clone https://github.com/data-accelerator/dart.git
cd dart
go build ./...
go test ./...
```

To try a cluster on one machine, `deploy/local-cluster.sh up` starts three nodes,
waits for membership to converge, and reports what they are doing. See the
[README](./README.md#trying-it-locally-with-three-nodes).

## Before you open a pull request

All three must pass (verbatim copy of the canonical list in AGENTS.md — edit
both or neither):

<!-- CANONICAL-COPY source="AGENTS.md" id="verify-trio" -->
```bash
go vet ./...
go test ./... -race -count=1
go test ./... -cover -count=1
```
<!-- /CANONICAL-COPY source="AGENTS.md" id="verify-trio" -->

CI runs exactly these, so a green local run should mean a green CI run. See
AGENTS.md's "Authority and canonical sources" for how rule conflicts between
repository documents are resolved.

Alongside the code, please include:

- **Tests.** Extend or add them; table-driven where it fits. For anything
  determinism-sensitive, pin the behavior with golden values computed by an
  *independent* implementation rather than by the Go code under test.
- **Documentation.** Every package has a `docs/<pkg>.md` with a fixed set of
  sections; a change to a package's behavior is expected to update its document.
  See [docs/README.md](./docs/README.md) for the template and the rationale. A new
  package also gets a row in that index.

## Things that are easy to break

DART has no coordinator: every node independently derives placement and tree
structure from the same inputs, and they must agree byte-for-byte. A silent
divergence does not raise an error — it just sends a request to the wrong node.
So:

- The `Hash64` construction and the HRW `score` formula in `internal/hashring`,
  and `ChunkKey` in `internal/chunk`, are effectively **part of the wire
  protocol**. Changing any of them reshuffles placement across the whole cluster.
  That is a breaking change: it must bump the cluster epoch, and the golden values
  must be regenerated independently.
- Orderings must stay a **strict total order** (score descending, then ID
  ascending). Never rely on input order, or on a non-stable sort happening to
  produce a canonical result.
- Node identity must be stable across restarts. Never derive it from something
  ephemeral such as a pod IP.

The cache is **read-only** by design. The origin is the only source of truth, so
an evicted or cold block is merely a re-fetch, never data loss. Please do not
introduce write-back or mutable-origin behavior without discussing the design
first — a lot of the system's simplicity depends on that assumption.

Blocks are also validated against the object geometry before being cached,
because the block cache is write-once per key: wrong bytes admitted once could
not be repaired by a later correct fetch. Keep new ingestion paths validated.

## Data-path performance

DART targets 100 Gbps per node where the network and media allow. When touching
the data path, preserve the properties that make that possible: do not wrap a
zero-copy path in buffering, and keep the data plane on HTTP/1.1 over pooled TCP
(no HTTP/2 there).

## Style

- Match the surrounding code: naming, comment density, idioms.
- Prefer the standard library. New dependencies need a clear justification.
- Comments and identifiers in English.
- Comments should explain *why*, not restate *what* the code does.
- No `init()` side effects, and no hidden global mutable state on hot paths.
- Document goroutine-safety and input-mutation/ownership in the package doc.

## Commits and pull requests

- Concise English commit messages: an imperative subject line, and a body that
  explains why the change is being made.
- Keep commits focused; unrelated changes belong in separate commits.
- Describe in the pull request what you verified and how.

## Reporting bugs

Please include the DART version or commit, the flags the node was started with,
what you expected, what happened, and any relevant log lines or metrics from
`/metrics` and `/admin/stats`. Redact credentials — note that an upstream URL's
query string can itself be a presigned credential.

For security issues, do **not** open a public issue; see
[SECURITY.md](./SECURITY.md).
