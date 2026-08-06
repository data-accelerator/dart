# `cmd/dart`

The DART node binary: an HTTP server that serves object byte ranges through the
read-through engine (cache, then peers, then origin), with peer discovery, a
registry pull-through mirror, the OverlayBD prefix API, and an admin/metrics
plane. Configured without peers or discovery it degrades to a single-node caching
proxy.

- Source: `cmd/dart/main.go`
- Tests: `cmd/dart/main_test.go`
- Build: `go build ./cmd/dart`

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
| `-cache-dir` | `./dart-cache` | directory for the block cache file (`blocks.dat`) |
| `-cache-size` | `1 GiB` | block cache capacity; accepts a unit suffix (see below) |
| `-chunk-size` | `256 MiB` | chunk size (placement/tree unit) |
| `-block-size` | `4 MiB` | block size (transfer/cache unit) |
| `-namespace` | `dart` | chunk-key namespace |
| `-self-id` | `""` | this node's cluster ID (required with `-peers`) |
| `-peer-listen` | `:9000` | peer block-server listen address (P2P mode) |
| `-peers` | `""` | fixed membership: comma-separated `id@host:port` (incl. self). Mutually exclusive with `-discover` |
| `-discover` | `""` | **maintain** membership by discovery: `dns:<name>:<port>` (a headless Service) or `static:<a>,<b>,...` |
| `-peer-advertise` | `""` | `host:port` peers should dial; defaults to `-peer-listen`, substituting `POD_IP`/`DART_ADVERTISE_HOST`/hostname when the host is a wildcard |
| `-discover-interval` | `5s` | how often to re-resolve seeds and re-exchange rosters |
| `-forget-after` | `60s` | how long a member must be unseen **and** unreachable before removal |
| `-fanout` | `2` | distribution-tree branching factor (children per node) |
| `-admin` | `:9100` | admin/metrics listen address; empty disables |
| `-reader-tree` | `true` | build the distribution tree over the active reader set (per-file tracker) |
| `-tracker-tick` | `3s` | how often a tracker republishes a reader set |
| `-replicas` | `1` | HRW candidates that authoritatively hold a chunk (the owned budget) |
| `-owned-fraction` | `0.8` | share of the cache reserved for owned blocks (0<f<1) |
| `-mem-size` | `256 MiB` | in-memory hot-set size; below one block disables the memory tier |

**Size units.** `-cache-size`, `-chunk-size`, `-block-size` and `-mem-size` accept
either a plain byte count or a suffix, following the Kubernetes convention so these
values can sit beside container resource limits without a second convention: an `i`
selects a power of two (`8GiB`, `512MiB`, also `Gi`/`Mi`) and its absence a power of
ten (`8GB`, `500MB`). A trailing `B` is optional and matching is case-insensitive.
`1GiB` and `1GB` are deliberately **not** the same number.
| `-hedge` | `true` | hedge a slow peer fetch to the grandparent/root once it exceeds the estimated p99 |
| `-hedge-ratio` | `0.05` | max share of peer fetches allowed to hedge |
| `-peer-timeout` | `30s` | per-request timeout for peer block fetches |
| `-breaker-failures` | `5` | consecutive peer failures that open its circuit; 0 disables breaking |
| `-breaker-cooldown` | `5s` | how long a peer circuit stays open before a probe |
| `-registry` | `""` | also serve a Registry v2 pull-through mirror for this registry on `/v2/` |
| `-registry-auth` | `""` | JSON file mapping registry host to `{username,password}` for private upstreams |
| `-oci-digest-only` | `false` | only treat `<algo>:<hex>` paths as content-addressed (disables the Distribution object-store layout) |

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


## 4. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **75.4%** of statements. The uncovered code is the blocking
  serve loop, signal handling, and `main` (integration-level, validated by a
  manual run/preview); the testable logic — flag parsing, size parsing, origin
  resolution (incl. the `//` preservation), handler construction, and an end-to-end
  prefix-passthrough round trip — is covered.
- **Note**: tests use `t.TempDir()`; in the sandbox export `TMPDIR=$PWD/.gotmp`.
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./cmd/dart/ -v -count=1
go test ./cmd/dart/ -race -count=1
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
  stream cut-through (forward while receiving) and cache. With `-reader-tree`
  (default) the tree spans only the nodes reading the object, via a per-file
  tracker served on the peer listener (see docs/tracker.md). `-hedge` bounds tail
  latency by racing a slow parent against its grandparent, and `-peer-timeout`
  bounds every peer request. A per-peer circuit breaker
  (`-breaker-failures`/`-breaker-cooldown`) stops re-dialing a sick peer and lets
  routing walk further up the tree around it. Missing: `splice`-level zero copy,
  and hedging on the streaming relay path.
- **Static membership only**: `-peers` is a fixed list; the Kubernetes
  EndpointSlice provider (dynamic membership) is future work.
- **Registry mirror and prefix API coexist**: `-registry` adds a Registry v2
  pull-through mirror on `/v2/` (blobs cached by digest, manifests always passed
  through — see docs/registry.md) for containerd, while `-prefix` keeps serving
  OverlayBD's p2pConfig API on `/<prefix>/<upstream-url>`. Both share one engine
  and one cache, so a layer fetched either way is stored once. `-registry-auth`
  supplies per-host credentials for private upstreams, including the Distribution
  token exchange. Still future proxy-layer work: forward-proxy mode, TLS/MITM, and
  per-request (per-client) upstream credentials.
- **Cold cache on restart**: the store does not persist across restarts and the
  arena is rebuilt empty on start, so no stale bytes linger on disk (safe under
  read-only semantics — the origin is the source of truth; see docs/store.md §8).
  Disk use is bounded by `-cache-size`, not by uptime or restart count, and the
  backing files are sparse (`ls -l` shows the reservation, `du` the actual use).
  **Run one instance per `-cache-dir`**: a second one wipes the first's cache.
  The cache is a RAM hot set (`-mem-size`) in front of disk, which is split into
  owned/borrowed budgets (`-owned-fraction`) with TinyLFU admission on the
  borrowed side, so peer churn cannot evict this node's authoritative shards.
- **Observability**: `-admin` serves `/metrics` (Prometheus), `/healthz`, and
  `/admin/{stats,members,ring}`; see docs/observability.md. Bind it to a private
  interface (no auth).
