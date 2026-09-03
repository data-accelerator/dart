# DART Documentation

This directory holds DART's **official documentation** (tracked in git). It targets both implementers and users, and covers, for every package: the public API, semantics, conventions, call permissions (concurrency contract), testing status, and stability guarantees.

> Scope note: this `docs/` tree is the **living, tracked documentation** and must stay in sync with the code. All content here is written in **English** for community use.

## Index

| Document | Scope | Status |
|---|---|---|
| [hashring.md](./hashring.md) | `internal/hashring` — weighted HRW placement + preorder distribution tree | Implemented, tests passing |
| [cluster.md](./cluster.md) | `internal/cluster` — membership, lifecycle state, weight, deterministic epoch | Implemented, tests passing |
| [chunk.md](./chunk.md) | `internal/chunk` — object/chunk/block addressing, keying, range decomposition | Implemented, tests passing |
| [store.md](./store.md) | `internal/store` — block cache: mem hot set, hybrid, owned/borrowed budgets, TinyLFU | Implemented, tests passing |
| [fetch.md](./fetch.md) | `internal/fetch` — origin HTTP range fetch + singleflight coalescing | Implemented, tests passing |
| [engine.md](./engine.md) | `internal/engine` — read-through orchestration (cache/peer/origin), relay, cut-through, hedging | Implemented, tests passing |
| [peer.md](./peer.md) | `internal/peer` — node-to-node block transport (server + pooled client) | Implemented, tests passing |
| [tracker.md](./tracker.md) | `internal/tracker` — per-file active reader set (leases, tick freeze) | Implemented, tests passing |
| [registry.md](./registry.md) | `internal/registry` — container registry pull-through mirror (OverlayBD/containerd path) | Implemented, tests passing |
| [observability.md](./observability.md) | `internal/metrics` + `internal/admin` — Prometheus exporter, admin endpoints | Implemented, tests passing |
| [node.md](./node.md) | `internal/node` — node assembly: flags, store/engine wiring, discovery-scheme dispatch | Implemented, tests passing |
| [dart.md](./dart.md) | `cmd/dart` — the node binary: registry mirror + OverlayBD prefix API, P2P, admin | Implemented, tests passing |
| [k8s.md](./k8s.md) | `providers/k8s` (separate module) — EndpointSlice-watch discovery; the `dart-k8s` variant | Implemented, tests passing |
| [design-assumptions.md](./design-assumptions.md) | cross-cutting — the minimalist assumptions every package relies on (trusted read-only origin, trusted domain, uniform config, ...) | Living contract |
| [adr/](./adr/README.md) | Architecture Decision Records — the legislative history behind contract-level rules | Living record |

(Register one row here for every new package.)

## Documentation Policy (mandatory)

**Any new or changed implementation code MUST come with an updated package document.** This is a hard rule; documentation is treated as part of the code.

### Required sections for every package document

1. **Overview** — what problem the package solves and where it sits in the system.
2. **Concepts** — core terms and models used by the package (e.g. chunk/block, HRW, preorder tree).
3. **Public API** — for **every exported type, function, and method**:
   - exact signature;
   - parameter and return-value semantics;
   - time/space complexity;
   - boundary and error behavior (empty input, out-of-range, zero values, etc.);
   - a minimal runnable example where useful.
4. **Invariants & Guarantees** — properties callers may rely on (determinism, total order, minimal disruption, tree properties, ...).
5. **Concurrency & Call Permissions**:
   - goroutine safety;
   - presence of any package-level / instance-level mutable state;
   - whether inputs are mutated, and ownership of returned values;
   - preconditions callers must satisfy (who may call, in what order, holding which locks).
6. **Stability Contract** — which changes are breaking (e.g. swapping the hash reshuffles the whole ring and must bump the cluster epoch); compatibility policy.
7. **Testing** — the test list with each test's intent; coverage number; `go vet` / `-race` results; how to run; sources of any golden / cross-validated values.
8. **Limitations & TODO**.

### Sanctioned document shapes

The 8-section template above is the default **package** shape. Three shapes
are sanctioned; every document's shape is assigned here (unlisted documents
default to `package`). The machine-readable block is the enforcement source
for `scripts/check-docs.sh` — keep it in sync with the prose.

<!-- DOC-SHAPES
package: Overview; Concepts; Public API; Invariants & Guarantees; Concurrency & Call Permissions; Stability Contract; Testing; Limitations & TODO
cmd: Overview; Usage; Behavior & guarantees (with Concurrency & lifecycle and Stability Contract as subsections); Testing; Limitations & TODO
multi: Overview; per-package API sections; Concurrency & Call Permissions; Stability Contract; Testing; Limitations & TODO
policy: free-form policy document (no required sections)
assignments: docs/dart.md=cmd; docs/observability.md=multi; docs/design-assumptions.md=policy
heading-aliases: Concepts <=> Wire form; Determinism / Stability Contract <=> Stability Contract
-->

- **package** (default): the 8 required sections in §"Required sections".
- **cmd** (assigned: `docs/dart.md`): a binary document — `Usage` replaces
  the API section; Concurrency and Stability may live as subsections of
  "Behavior & guarantees".
- **multi** (assigned: `docs/observability.md`): one document covering
  several packages uses per-package API sections instead of unified
  Concepts/Public-API/Invariants sections.
- **policy** (assigned: `docs/design-assumptions.md`): a free-form policy
  document; the section template does not apply.
- **Heading aliases**: the Concepts section may be titled by its domain
  content (e.g. peer.md's "Wire form"); the Stability Contract may be titled
  "Determinism / Stability Contract".

### Enforcement tiers

The floor enforced by `scripts/check-docs.sh` is split into two tiers, by one
principle: **a claim that is false or broken blocks; a demand for more
documentation advises.**

- **Hard gate (fails CI):** index ↔ files sync (check 1); documented test
  names exist (2); ADR integrity (5); canonical-copy lockstep (6); written
  evidence anchors resolve (8). These catch something that *is* wrong — a
  name that does not exist, a copy that drifted, an anchor that does not
  resolve.
- **Advisory notes (never fail CI; reported as a single sticky PR
  comment):** exported-symbol naming (check 3); required sections (4); ADR
  back-references (7). These ask for *more* documentation, and their
  satisfaction involves judgment (where a symbol is best named, where an
  ADR's back-reference belongs).

**Dispositions.** A PR may merge with open advisory notes only when each
note has a disposition in the PR thread: fixed, or waived with a stated
reason. Waivers are explicit, auditable decisions — never silent merges.
Repository-internal work (maintainer and agents) holds itself to the same
rule: fix or waive in-thread, do not let notes accumulate.

### What "Testing" must record

"Testing" is not a single line saying "tests exist". It must let the reader judge **trustworthiness**: list the specific property each test guards, the coverage number, and whether `vet`/`-race` pass, plus reproduction commands. If a function is left uncovered, state why explicitly.

### Experimental: evidence anchors (pilot)

A contract-level entry (e.g. a Public API or Stability Contract entry) may
carry an **evidence anchor** — a backticked
"path::symbol" reference (file + symbol, **never a line number**: line
numbers drift on every edit; symbol names are stable and their existence is
grep-checkable, the same granularity as the test-name and exported-symbol
checks). `scripts/check-docs.sh` check 8 keeps written anchors valid: the
file must exist and declare the symbol (function, method, or
`type`/`const`/`var`). Anchors are **opt-in** — the check never requires
writing one; it only stops contract prose from silently outliving the symbol
it describes.

Status: **pilot**, currently only in `docs/fetch.md` (see the `Anchor:`
sentences there for live examples) — evaluating signal-to-noise before any
wider adoption. Promotion to a repository-wide convention, if it happens,
goes through an ADR. Point-in-time historical claims anchor a commit SHA
instead of a live symbol — frozen is a feature there. ADR bodies never carry
live anchors (see `docs/adr/README.md`).

## Running the tests locally

DART has no external dependencies, so the standard commands are all that is
needed:

<!-- CANONICAL-COPY source="AGENTS.md" id="verify-trio" -->
```bash
go vet ./...
go test ./... -race -count=1
go test ./... -cover -count=1
```
<!-- /CANONICAL-COPY source="AGENTS.md" id="verify-trio" -->

(canonical list: AGENTS.md; the copies here and in CONTRIBUTING.md are marked
verbatim copies kept in lockstep by `scripts/check-docs.sh`)

If you work in a sandbox that only permits writes under the workspace, redirect
the Go caches there first (these directories are gitignored):

```bash
export GOCACHE=$PWD/.gocache GOPATH=$PWD/.gopath \
       GOMODCACHE=$PWD/.gopath/pkg/mod GOTMPDIR=$PWD/.gotmp GOTOOLCHAIN=local
```

Tests that write files use `t.TempDir()`, which honors `TMPDIR`.
