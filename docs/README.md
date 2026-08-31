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

### What "Testing" must record

"Testing" is not a single line saying "tests exist". It must let the reader judge **trustworthiness**: list the specific property each test guards, the coverage number, and whether `vet`/`-race` pass, plus reproduction commands. If a function is left uncovered, state why explicitly.

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
