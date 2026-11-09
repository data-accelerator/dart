# `cmd/dart`

The DART node binary: an HTTP server that serves object byte ranges through the
read-through engine (cache, then peers, then origin), with peer discovery, a
registry pull-through mirror, the OverlayBD prefix API, and an admin/metrics
plane. Configured without peers or discovery it degrades to a single-node caching
proxy.

- Source: `cmd/dart/main.go` (a thin shell; all assembly lives in `internal/node` — see [node.md](./node.md))
- Tests: `cmd/dart/main_test.go` (scheme wiring); the behavioral tests live in `internal/node`
- Build: `go build ./cmd/dart`
- Variant: `dart-k8s` (`providers/k8s/cmd/dart-k8s`) adds EndpointSlice discovery — see [k8s.md](./k8s.md)

## 1. Overview

`dart` wires `internal/store` + `internal/engine` (which itself uses
`internal/chunk` + `internal/fetch`) behind an HTTP listener. A client request's
`Range` is decomposed into blocks, each served from the local cache or fetched
from origin and cached, and streamed back with an explicit `Content-Length`
(never chunked). With `-peers` configured the node also runs a peer block server
and pulls missing blocks from the owning peer before origin (M2 star topology).

## 2. Usage

```
dart [flags]
```

| Flag | Default | Meaning |
|---|---|---|
| `-version` | `false` | print the build version (stamped via `-ldflags "-X main.version=..."`) and exit |
| `-listen` | `:8080` | listen address |
| `-origin` | `""` | fixed upstream origin URL; empty enables prefix passthrough |
| `-prefix` | `dart` | path prefix for passthrough mode |
| `-cache-mode` | `hybrid` | cache backend: `memory`, `disk`, or `hybrid` |
| `-cache-dir` | `./dart-cache` | disk-cache directory; ignored in memory mode |
| `-cache-size` | `1 GiB` | total backing cache capacity in every mode; split owned/borrowed; accepts a unit suffix |
| `-chunk-size` | `256 MiB` | chunk size (placement/tree unit) |
| `-block-size` | `4 MiB` | block size (transfer/cache unit) |
| `-prefetch-window` | `2` | covering blocks in flight per client range, including demand; 1 disables, maximum 64 |
| `-prefetch-bytes` | `64 MiB` | node-wide reserved speculative payload, including completed queued results |
| `-prefetch-loads` | `16` | node-wide outstanding speculative loads/results; exhaustion preserves demand progress |
| `-receive-bytes` | `0` (auto) | live temporary block receive payload; auto selects max(256 MiB, block-size); explicit limit must fit one block |
| `-receive-loads` | `64` | live temporary block receive buffers, including retained shared results; separate from cache/prefetch capacity |
| `-namespace` | `dart` | chunk-key namespace |
| `-role` | `ordinary` | `ordinary`, `root-only` (no client listener), or `non-root` (delegate all origin access); restricted roles require P2P |
| `-self-id` | `""` | this node's cluster ID (required with `-peers`) |
| `-peer-listen` | `:9000` | peer block-server listen address (P2P mode) |
| `-peers` | `""` | fixed membership: comma-separated `id@host:port#role` (incl. self; role defaults to ordinary). Mutually exclusive with `-discover` |
| `-discover` | `""` | **maintain** membership by discovery: `<scheme>:<spec>` — `dns:<name>:<port>` (a headless Service), `static:<a>,<b>,...`, or a bare address list; the `dart-k8s` variant adds `k8s:<namespace>/<service>[/<port>]` |
| `-peer-advertise` | `""` | `host:port` peers should dial; wildcard values are rejected. Defaults to `-peer-listen`, resolving a wildcard host in order: `DART_ADVERTISE_HOST` → `POD_IP` → hostname |
| `-discover-interval` | `5s` | how often to re-resolve seeds and re-exchange rosters |
| `-forget-after` | `60s` | how long a member must be unseen **and** unreachable before removal |
| `-fanout` | `2` | distribution-tree branching factor (children per node) |
| `-admin` | `:9100` | admin/metrics listen address; empty disables |
| `-reader-tree` | `true` | build the distribution tree over the active reader set (per-file tracker) |
| `-tracker-tick` | `3s` | how often a tracker republishes a reader set |
| `-replicas` | `1` | HRW candidates that authoritatively hold a chunk (the owned budget) |
| `-owned-fraction` | `0.8` | owned share in [0,1]; 0 is borrowed-only, 1 owned-only; invalid/non-finite values fail startup |
| `-mem-size` | `256 MiB` | Hybrid hot-set size; ignored in memory/disk modes; below one block disables the Hybrid front tier |

| `-hedge` | `true` | hedge a slow peer fetch to the grandparent/root once it exceeds the estimated p99 |
| `-hedge-ratio` | `0.05` | max share of peer fetches allowed to hedge |
| `-peer-timeout` | `30s` | per-request timeout for peer block fetches |
| `-breaker-failures` | `5` | consecutive peer failures that open its circuit; 0 disables breaking |
| `-breaker-cooldown` | `5s` | how long a peer circuit stays open before a probe |
| `-registry` | `""` | also serve a Registry v2 pull-through mirror for this registry on `/v2/` |
| `-registry-auth` | `""` | JSON file mapping registry host to `{username,password}` for private upstreams |
| `-oci-digest-only` | `false` | only treat `<algo>:<hex>` paths as content-addressed (disables the Distribution object-store layout) |

**Size units.** `-cache-size`, `-chunk-size`, `-block-size`, `-prefetch-bytes`, `-receive-bytes` and `-mem-size` accept
either a plain byte count or a suffix, following the Kubernetes convention so these
values can sit beside container resource limits without a second convention: an `i`
selects a power of two (`8GiB`, `512MiB`, also `Gi`/`Mi`) and its absence a power of
ten (`8GB`, `500MB`). A trailing `B` is optional and matching is case-insensitive.
`1GiB` and `1GB` are deliberately **not** the same number.

### Cache modes

`-cache-mode=hybrid` preserves the default: a disk Tiered backend sized by
`-cache-size`, plus a RAM hot set sized by `-mem-size`. A hot set smaller than one
block preserves the existing disk-only behavior. `-cache-mode=disk` explicitly
selects the disk backend, ignoring `-mem-size`.

`-cache-mode=memory -cache-size=2GiB` uses only RAM-backed owned/borrowed budgets.
It never creates or opens a cache directory, arena or directory lock; `-cache-dir`
and `-mem-size` are unused. The existing `-owned-fraction` split and borrowed
TinyLFU admission remain, so single-node owned traffic can fill only its reserved
share. Capacity rounds down to whole blocks. Split budgets require at least two
blocks; `-owned-fraction=0` (borrowed-only) or `=1` (owned-only) requires only one.
Disabled-class writes are skipped in all modes, including Hybrid's RAM hot set,
without taking capacity from the other class. Cache bypass does not fail a read.
Roles do not set budgets automatically: a dedicated root-only peer commonly uses
`-owned-fraction=1`, and a non-root peer uses `-owned-fraction=0` (ADR-0009).
Invalid modes, capacities and fractions fail clearly. RAM payloads
are managed in lazy arena segments; cache capacity excludes metadata, temporary buffers and
in-flight reads/prefetch, so it is not a process RSS limit. Restart starts cold.
Temporary receives are bounded separately by `-receive-bytes` and
`-receive-loads` until the last reader releases each result. Exhaustion waits up
to 2 ms before failing the load; speculative temporary fills skip immediately,
leaving headroom for one demand block. Cached hits and arena-backed fills do not
consume this temporary allowance. Defaults are capacity guardrails, not measured
optimal concurrency; zero-cache-class workloads may need larger receive limits.

Backend choice is node-local and can mix within a uniform-geometry cluster
(ADR-0009); backend choice itself does not change paths, keys or placement.

```bash
dart -cache-mode=memory -cache-size=2GiB -origin https://example.com/blob
```

**Memory budgets.** RAM arena segments are allocated by Go and count toward
`GOMEMLIMIT`: include the memory-mode cache capacity or Hybrid RAM hot set,
metadata, in-flight/temporary buffers and runtime state in that budget.
`GOMEMLIMIT` is a soft runtime memory limit, not a cache eviction mechanism or
a process RSS cap; garbage collection cannot reclaim live cache payloads.
Leave further headroom in the container memory limit for memory outside the Go
runtime's accounting. Disk arena files and their OS page cache are separate
from the RAM payload budget, but page cache can still count against container
memory. See [Go's memory-limit contract](https://pkg.go.dev/runtime/debug#SetMemoryLimit).

### Origin resolution modes

- **Fixed origin** (`-origin URL`): every request is served from that one
  upstream. `GET http://dart:8080/<anything>` → `URL`.
- **Prefix passthrough** (default, `-origin` empty): the request path after
  `/<prefix>/` is the full upstream URL, **including scheme**:

  ```
  GET http://dart:8080/dart/https://registry.example.com/v2/lib/nginx/blobs/sha256:...
  ```

  This matches overlaybd's `p2pConfig` address form. The resolver reads the raw
  `RequestURI` (not `URL.Path`) so the embedded `https://` is not collapsed by
  path cleaning.

### Examples

```bash
# Mirror one upstream:
dart -origin https://registry-1.docker.io -listen :8080 -cache-dir /var/cache/dart

# Generic passthrough (overlaybd p2pConfig: address = dart-host:8080/dart):
dart -listen :8080 -cache-dir /var/cache/dart
# then:
curl -r 0-4095 "http://localhost:8080/dart/https://example.com/big.tar"
```

### P2P (multi-node)

Give every node the same member list (`id@peer-addr`, including itself) and its
own `-self-id`; each node runs a peer server on `-peer-listen`. On a cache miss a
node pulls the block from the owning peer (weighted HRW over the members) before
falling back to origin.

```bash
dart -listen :8080 -peer-listen :9000 -self-id A \
     -peers A@10.0.0.1:9000,B@10.0.0.2:9000 -cache-dir /var/cache/dart   # node A
dart -listen :8080 -peer-listen :9000 -self-id B \
     -peers A@10.0.0.1:9000,B@10.0.0.2:9000 -cache-dir /var/cache/dart   # node B
```

## 3. Behavior & guarantees

Long ranges use bounded ordered in-range prefetch (ADR-0008). The byte/load
limits cover speculative payload/results, not cache memory, foreground reads or
transient copies. Prefetch workers are canceled and joined before handler exit.
`-prefetch-window=1` selects serial block reads; no peer handler expands its range.


- Serves `GET`/`HEAD`; other methods → `405`.
- `Resolve` failure → `400`; origin/size probe failure → `502`.
- Standard Range semantics: `200` (full), `206` + `Content-Range` (range),
  `416` for unsatisfiable ranges. Responses are always Content-Length framed
  (non-chunked), per the design requirement.
- Uses `http.Server` **without** `ServeMux`, so raw request paths (embedded
  upstream URLs) are preserved.
- Graceful shutdown on `SIGINT`/`SIGTERM` (10s drain).

### 3.6 Discovery vs a fixed peer list

`-peers` states membership once; `-discover` maintains it. They are alternatives and
passing both is an error.

With `-discover`, a node resolves seed **addresses** and then exchanges **rosters**
with them over its own peer port to learn stable identities and any other members
those peers know. One reachable neighbour therefore suffices to join, which is what
makes a DaemonSet workable: the node set is unknown when the manifest is written.

Two timings matter and are asymmetric on purpose:

- `-discover-interval` (seconds) governs how fast a *new* peer is noticed. Cheap.
- `-forget-after` (a minute) governs how long before an absent peer is *removed*.
  Removal re-runs placement and moves ownership of ~1/N of the keyspace, whereas
  routing around a dead peer already happens in about a second via the circuit
  breaker. Setting `-forget-after` near `-discover-interval` would make a flapping
  node churn the whole cluster.

`-peer-advertise` exists because a listen address is often not a reachable one:
`:19146` and `0.0.0.0:19146` name every interface, and a peer told to dial `0.0.0.0`
would dial itself.

The scheme set is a property of the binary: `dart` ships `dns` and `static`
(plus the bare-list shorthand), and the `dart-k8s` variant adds `k8s`, which
watches EndpointSlices instead of polling DNS — same convergence afterwards,
but immediate and readiness-aware (see [k8s.md](./k8s.md)). `-discover`'s help
text lists exactly what the running binary was linked with.


### 3.7 Concurrency & lifecycle

All servers (client, peer, admin) run concurrently; every write to the
operator's `out` stream is serialized through a locked writer. Handler
lifetime follows the node-level admission-gate contract — nothing is closed
under a live handler (docs/node.md §3.1, ADR-0003). The binary's version is
stamped at build time (`-ldflags "-X main.version=..."`) and reported by
`-version` and on startup.

### 3.8 Stability contract

- The flag set, defaults, and their validation rules are the operator-facing
  contract; changing a default is a breaking change for existing deployments
  and must be called out in the changelog.
- Wire-visible behavior (range semantics, the registry-mirror path set, peer
  headers) is governed by the Stability Contract sections of the engine,
  registry, and peer package documents — this binary adds no wire rules of
  its own beyond the wiring documented here.
- The discovery-scheme set is a property of the binary (`dns`+`static` for
  `dart`, plus `k8s` for `dart-k8s`); embedding applications choose it via
  `node.Run`'s scheme registration.

Memory reads borrow pinned blocks and supported refills receive directly into
unpublished slots (ADR-0008). A pin delays slot reuse after eviction. Slot pressure
waits at most 2 ms with cancellation and then serves through temporary buffers
without cache insertion. This exceptional path may allocate/copy. Existing cache
flags and owned/borrowed policy remain; no persistent shared memory is provided.

## 4. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: the behavioral coverage moved to `internal/node` (**77.0%**) with
  the code; what remains here is `main` and the scheme table, guarded by
  `TestSchemes`. The test list below therefore describes tests that now live in
  `internal/node` (`node_test.go`, `discover_test.go`, `size_test.go`).
- **Note**: tests use `t.TempDir()`; in the sandbox export `TMPDIR=$PWD/.gotmp`.
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./cmd/dart/ ./internal/node/ -v -count=1
go test ./cmd/dart/ ./internal/node/ -race -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestParseByteSize` | byte counts and `KiB/MiB/GiB/TiB` plus `KB/MB/GB/TB`, fractions, case, spacing |
| **`TestParseByteSizeBinaryVsDecimal`** | **`1GiB` != `1GB`; the `i` is what selects a power of two** |
| `TestParseByteSizeRejects` | empty, unknown unit, trailing junk, negative, overflowing |
| `TestSizeFlagWiring` | the default applies when the flag is absent; a bad value fails parsing |
| **`TestSizeFlagsAcceptedByDart`** | **the exact size flags used by `deploy/k8s/` parse — before the size parser they failed at startup, so every such pod crash-looped** |
| `TestResolverFixedOrigin` | fixed-origin resolver returns the configured URL |
| `TestResolverPrefixPassthrough` | prefix stripping; scheme required; missing prefix errors |
| `TestResolverPreservesDoubleSlash` | embedded `https://` not collapsed (RequestURI, not URL.Path) |
| `TestNewHandlerValidation` | invalid chunk config / cache < block rejected |
| `TestEndToEndPrefix` | client → dart → origin range fetch, 206, exact bytes, non-chunked |
| `TestParseFlagsDefaults` | flag defaults and parsing |
| `TestNewHandlerBuildsForTmpDir` | handler builds and creates the cache dir |
| `TestRunErrors` | flag/config errors propagate from run |
| `TestParsePeers` | member-list parsing (`id@host:port`) and rejects |
| `TestBuildP2PWiring` | P2P off → nil peer handler; `-peers` needs `-self-id`; both set → peer handler present |

## 5. Limitations & TODO

- **P2P tree**: a miss routes to the parent in the preorder tree; relay nodes
  receive and validate whole blocks before forwarding, and cache best-effort. With `-reader-tree`
  (default) the owner anchors the tree, with eligible readers below it via a per-file
  tracker served on the peer listener (see docs/tracker.md). `-hedge` bounds tail
  latency by racing a slow parent against its grandparent, and `-peer-timeout`
  bounds every peer request. A per-peer circuit breaker
  (`-breaker-failures`/`-breaker-cooldown`) stops re-dialing a sick peer and lets
  routing walk further up the tree around it. Missing: `splice`-level zero copy,
  and hedging on the streaming relay path.
- **Discovery schemes**: `-peers` fixes membership; `-discover` maintains it via
  a scheme — `dns` and `static` in this binary, `k8s` (EndpointSlice watch) in
  the `dart-k8s` variant (see [k8s.md](./k8s.md)). Schemes are registered by the
  binary, so the plain `dart` build keeps an empty `go.sum`.
- **Registry mirror and prefix API coexist**: `-registry` adds a Registry v2
  pull-through mirror on `/v2/` (blobs cached by digest, manifests always passed
  through — see docs/registry.md) for containerd, while `-prefix` keeps serving
  OverlayBD's p2pConfig API on `/<prefix>/<upstream-url>`. Both share one engine
  and one cache, so a layer fetched either way is stored once. `-registry-auth`
  supplies per-host credentials for private upstreams, including the Distribution
  token exchange. Still future proxy-layer work: forward-proxy mode, TLS/MITM, and
  per-request (per-client) upstream credentials.
- **Cold cache on restart**: all backends start empty. Disk/Hybrid modes rebuild
  sparse arenas under `-cache-dir`, bounded by `-cache-size`, and acquire the
  directory lock before opening them: a second instance fails startup. Memory
  mode uses no cache files/directory lock. All modes preserve isolated
  owned/borrowed budgets and borrowed TinyLFU admission (ADR-0007).
- **Observability**: `-admin` serves `/metrics` (Prometheus), `/healthz`, and
  `/admin/{stats,members,ring}`; see docs/observability.md. Bind it to a private
  interface (no auth).
