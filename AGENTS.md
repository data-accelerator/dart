# AGENTS.md

Guidance for AI agents contributing to **DART** (DADI Accelerated Resource
Transfer). Read this before making changes.

## What DART is

A distributed **read-only** cache with node-to-node **P2P hot-content
distribution**, written in Go, deployed on Kubernetes (optionally via Fluid).
It accelerates read-only artifact loading (container images / npm / pypi / yum /
oss) and is designed to let open-source **OverlayBD** use it as a P2P
acceleration proxy over plain HTTP. Target throughput: 100 Gbps/node where
network and media allow.

Core ideas: cloud-native peer discovery (K8s only, all peers equal, TCP only, no
broadcast); weighted Rendezvous Hashing (HRW) for chunk placement; a preorder
full k-ary tree (derived from the HRW order, over the active reader set) for P2P
distribution; two-tier cache (owned shards vs borrowed hot copies); mem/disk/
hybrid tiers.

## Repository layout

```
internal/hashring/   weighted HRW placement + preorder distribution tree
internal/cluster/    membership, lifecycle state, weight, epoch, discovery
internal/chunk/      object/chunk/block addressing, keying, range decomposition
internal/store/      block cache: disk arena, owned/borrowed budgets, mem tier
internal/fetch/      origin HTTP range fetch + singleflight coalescing
internal/engine/     read-through orchestration, relay, cut-through, hedging
internal/peer/       node-to-node block transport + circuit breaking
internal/tracker/    per-file active reader set the tree is built over
internal/registry/   Registry v2 pull-through mirror + token auth
internal/metrics/    dependency-free Prometheus exporter
internal/admin/      admin/diagnostic endpoints
internal/node/       node assembly (flags, wiring, discovery-scheme dispatch)
cmd/dart/            the node binary (thin shell over internal/node)
providers/k8s/       SEPARATE MODULE: EndpointSlice discovery (client-go) +
                     cmd/dart-k8s; keeps the main module dependency-free
deploy/              Kubernetes manifests + local multi-node rig
  helm/dart/         chart for the DaemonSet shape (discovery.mode=dns|k8s)
  fluid/             Fluid ThinRuntimeProfile + Dataset samples
docs/                official, git-tracked documentation (English)
.gitignore           excludes Go cache dirs and the untracked design/ notes
go.mod               module github.com/data-accelerator/dart (go 1.22)
```

Every package above is implemented with tests and a `docs/<pkg>.md`. Deliberately
absent for now: Helm/Fluid packaging (see the plan's W4) and a `dartctl` CLI.

The multi-module rule: the main module's go.sum stays empty. External
dependencies (client-go today) live only in `providers/<name>/` modules, which
pin the sibling main module with `replace ../..` and are wired into binaries by
explicit `node.Run(..., schemes...)` registration — never `init()`.

## Documentation

- **`docs/`** is the tracked, **English** documentation for the community. Every
  package must have a `docs/<pkg>.md`. See `docs/README.md` for the mandatory
  section template and the documentation policy. Documentation is part of a
  change, not optional.

## Mandatory workflow for any code change

1. **Implement** following the surrounding style (match naming, comment density,
   idioms). Prefer the standard library; avoid new dependencies unless clearly
   justified.
2. **Test**: add/extend tests. Aim for high coverage and table-driven tests. For
   anything determinism-sensitive, pin behavior with **golden values computed by
   an independent implementation** (not by the Go code itself).
3. **Verify** (all must pass before you consider the change done):
   ```bash
   go vet ./...
   go test ./... -race -count=1
   go test ./... -cover -count=1
   ```
4. **Document**: add/update `docs/<pkg>.md` per the required sections, and add a
   row to the `docs/README.md` index for a new package. Documentation is part of
   the change, not optional.
5. **Commit** locally (see Git rules below).

## Build & test environment

- Go: the module targets `go 1.22`; any newer toolchain works. Plain
  `go vet ./...` and `go test ./...` are all that is required — there are no
  external dependencies and no code generation.
- If you are working in a sandbox that only permits writes under the workspace,
  redirect the Go caches there first:
  ```bash
  export GOCACHE=$PWD/.gocache GOPATH=$PWD/.gopath \
         GOMODCACHE=$PWD/.gopath/pkg/mod GOTMPDIR=$PWD/.gotmp GOTOOLCHAIN=local
  ```
  Those directories (`.gocache/ .gopath/ .gotmp/ .toolchain/`) are gitignored.
  Tests that write files use `t.TempDir()`, which honors `TMPDIR`.

## Determinism is load-bearing — do not break it

DART has no coordinator; every node must derive byte-identical placement and
tree structure from the same inputs. Therefore:

- The `Hash64` construction and the HRW `score` formula in `internal/hashring`
  are **part of the wire protocol**. Changing either reshuffles the whole ring /
  changes tree shapes — a **breaking change** that must bump the cluster epoch,
  and you must regenerate the golden values with an independent implementation.
- Keep orderings a **strict total order** (score desc, then ID asc). Never rely
  on input order or non-stable sorts producing a canonical result by accident.
- Node identity (`Node.ID`) must be stable and cluster-consistent; never derive
  it from ephemeral values (e.g. PodIP).

## Coding conventions

- Keep functions pure and stateless where feasible; document goroutine-safety
  and input-mutation/ownership explicitly in the package doc.
- No `init()` side effects; no hidden global mutable state on hot paths.
- Respect the 100 Gbps design constraints when touching data-path code:
  preserve zero-copy (`sendfile`/`splice`) — do not wrap zero-copy links in
  buffering; prefer HTTP/1.1 over pooled TCP (no HTTP/2 on the data plane).
- Comments and identifiers in code: English.

## Git rules

- Do **not** modify git config. Use whatever identity the working copy already
  has; if you must set one for a single commit, use `-c` flags rather than
  persisting config.
- Commit messages: concise English, imperative subject line, body explaining the
  why. Keep commits focused.
- Never run destructive/irreversible git commands. Never commit the Go cache
  directories (they are gitignored; keep it that way).

## Safety

- Do not run destructive shell commands (`rm -rf`, etc.) or anything requiring
  `sudo`. Use the dedicated file tools for file operations.
- Read-only cache semantics are a core assumption; do not introduce write-back or
  mutable-origin behavior without explicit design agreement.
