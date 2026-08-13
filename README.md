# DART

**D**ADI **A**ccelerated **R**esource **T**ransfer — a read-only cache that turns a
cluster of nodes into a peer-to-peer distribution tree, so pulling the same bytes
onto many machines costs the origin roughly one fetch instead of one per node.

It is aimed at the case where a lot of machines suddenly want the same immutable
data: container image layers, model weights, datasets, package archives.

```
                      origin (registry / object store / OSS)
                            ▲
                            │  one fetch, not one per node
                     ┌──────┴──────┐
                     │   node A    │        each node caches what it pulls and
                     └──┬───────┬──┘        relays to its children while still
                        │       │           receiving (cut-through)
                 ┌──────▼──┐ ┌──▼──────┐
                 │ node B  │ │ node C  │
                 └──┬───┬──┘ └─────────┘
                    │   │
                  ...   ...
```

> ### Status: working, validated on a live cluster; hardening in progress
>
> The data path and peer discovery are implemented and tested (390 tests,
> race-clean), and the image and manifests have been verified on a live
> Kubernetes cluster (`deploy/verify.sh`). What remains before calling it
> production-ready is throughput work toward the 100 Gbps-per-node target and
> operational hardening. See [Roadmap](#roadmap).

## Why

A 200-node rollout of a 2 GB image pulls 400 GB from the registry, and every node
waits on the same bottleneck. The usual fixes each give something up: a bigger
registry costs money and still centralizes; a plain local cache per node does not
help the first puller on each node; BitTorrent-style swarms add a tracker and lose
the ability to serve a precise byte range on demand.

DART's angle is that this data is **read-only**, which removes the need for
coordination. Two nodes disagreeing about cluster membership can only cause a
suboptimal route — an extra hop, or a fetch from origin — never a wrong byte. That
one property is what lets the whole system run without consensus, without a
tracker, and without a leader.

## How it works

Three granularities, because one size cannot serve all three jobs:

| Level | Size | Decides |
|---|---|---|
| **object** | whole blob | identity — a digest when one can be recovered from the URL, so the same layer from two registries is cached once |
| **chunk** | 256 MiB | who owns it and what the distribution tree looks like |
| **block** | 4 MiB | the unit actually transferred and cached |

- **Placement** uses weighted [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)
  (HRW). Removing one node of N moves only ~1/N of the keyspace, so a node failure
  does not reshuffle the cluster.
- **The distribution tree** is that same HRW ranking read as a pre-order traversal
  of a k-ary tree. Parent and child are computed arithmetically from the ranking —
  no tree is built or agreed upon, and every node derives the same one.
- **Cut-through relay**: an intermediate node forwards bytes downstream while it is
  still receiving them, and caches them on the way past, so a deep chain pipelines
  instead of storing-and-forwarding at each hop.
- **Two cache budgets, physically separated**: blocks this node *owns* (its share of
  the keyspace) cannot be evicted by blocks it merely *borrowed* while relaying.
  Admission to the borrowed budget goes through a TinyLFU filter so a one-shot read
  cannot evict a genuinely hot block.
- **Tail latency**: a slow parent is hedged to its grandparent under a rate limit; a
  *failed* parent fails over immediately without one. Conflating those two is a
  mistake that makes a single dead node very expensive.

Arbitrary HTTP `Range` requests are served from block boundaries, so a client that
reads 8 KiB from the middle of a 2 GB layer transfers one block, not the object.

## Quick start

Needs Go 1.22+. No external dependencies — `go.sum` is empty and stays that way.

```sh
go build ./cmd/dart
```

Serve an origin through DART. Two front ends share one listener and one cache:

```sh
# Passthrough: /dart/<full upstream URL>  (this is how overlaybd talks to DART)
./dart -listen=:8145 -admin=:8147 -prefix=dart \
       -cache-dir=/tmp/dart-cache -cache-size=1GiB
```

```sh
curl -o layer.bin 'http://127.0.0.1:8145/dart/https://example.com/blob.bin'
```

The cache is doing something:

```sh
$ curl -s 127.0.0.1:8147/metrics | grep block_source
dart_block_source_total{source="cache"} 0
dart_block_source_total{source="peer"} 0
dart_block_source_total{source="origin"} 6      # cold: 6 blocks fetched

# ... read the same object again ...
dart_block_source_total{source="cache"} 6       # warm: served locally
dart_block_source_total{source="peer"} 0
dart_block_source_total{source="origin"} 6      # unchanged — no return to origin
```

### Two nodes, sharing

```sh
PEERS=A@127.0.0.1:9201,B@127.0.0.1:9202

./dart -self-id=A -peers=$PEERS -listen=:9101 -peer-listen=:9201 -admin=:9301 \
       -cache-dir=/tmp/dart-A -cache-size=1GiB &
./dart -self-id=B -peers=$PEERS -listen=:9102 -peer-listen=:9202 -admin=:9302 \
       -cache-dir=/tmp/dart-B -cache-size=1GiB &
```

Ask **A** for an object neither node has. HRW makes **B** the owner, so A fetches
from B, and B is the only one that talks to the origin:

```
node A:  source="peer"   6      source="origin" 0
node B:  source="origin" 6      source="cache"  6
                                ↑
         6 origin fetches for a 6-block object read by both nodes, not 12
```

`-self-id` must be a **stable** identity (a node name, not a pod IP): HRW keys are
derived from it, so an identity that changes on restart reshuffles the keyspace.

### Or let them find each other

Instead of listing peers, give each node a seed to start from. It resolves seed
addresses, then asks those peers for their identities and for whoever else they know,
so **one reachable neighbour is enough to join**:

```sh
./dart -self-id=$NODE_NAME -discover=dns:dart.default.svc.cluster.local:9000 \
       -peer-advertise=$POD_IP:9000 -peer-listen=:9000 ...
```

Adding and removing a member are deliberately not symmetric: a new peer is picked up
within `-discover-interval` (5s), while one that vanishes is only *removed* after
`-forget-after` (60s), because removal re-runs placement and moves ownership of ~1/N
of the keyspace. Requests stop going to a dead peer within about a second regardless,
via its circuit breaker.

### Registry mirror

For containerd, DART also speaks the Registry v2 pull-through API on the same
listener:

```sh
./dart -listen=:8145 -registry=https://registry-1.docker.io -cache-dir=/tmp/dart-cache
```

Only digest-addressed blobs are cached; manifests are always passed through, since
a tag can be repointed at any time. Writes are refused — this is a pull-through
mirror, not a registry.

## Trying it locally with three nodes

[`deploy/local-cluster.sh`](./deploy/local-cluster.sh) starts a cluster on one
machine, waits for it to converge, and reports what it is doing. It configures no
origin: in prefix mode a request carries the whole upstream URL, so point it at
whatever real origin you have.

```sh
deploy/local-cluster.sh up            # 3 nodes, discovered peers, prints the URLs
deploy/local-cluster.sh status        # hit ratio, wire bytes, epoch agreement
deploy/local-cluster.sh watch         # throughput between samples
deploy/local-cluster.sh down
```

```
  NODE READY  MEMBERS     CACHE      PEER    ORIGIN   DELIVERED    UPSTREAM
  A    ok     3               0        31         0   122.1 MiB         0 B
  B    ok     3               0        31         0   122.1 MiB         0 B
  C    ok     3               0         0        93   122.1 MiB   122.1 MiB
  ALL                         0        62        93   366.2 MiB   122.1 MiB

  origin offload: 40% of block reads were satisfied inside the cluster
  amplification: 33% of delivered bytes were pulled from upstream
```

Three nodes each read the whole object; the origin transferred it once.

## Kubernetes

Manifests are in [`deploy/k8s/`](./deploy/k8s), a Helm chart packages the
production shape in [`deploy/helm/dart/`](./deploy/helm/dart), and
[`deploy/verify.sh`](./deploy/verify.sh) asserts on a live cluster the things unit
tests cannot: that the image runs unprivileged, that peer hits happen across a real
network hop, and that origin fetches stay bounded as instances multiply.

```sh
docker build -t dart:dev .
deploy/verify.sh -i dart:dev
```

- `daemonset.yaml` — the production shape: one instance per node, reachable by
  node-local clients over `hostPort`, discovering peers through the headless
  Service.
- `statefulset.yaml` — the verification shape: a fixed three replicas (a
  DaemonSet's size depends on the cluster) so `verify.sh` can assert exact
  membership and fetch counts. Discovery is the same DNS-plus-roster
  mechanism as production.
- `rbac.yaml` — only for the `dart-k8s` image variant (see below).

The chart mirrors `daemonset.yaml`; one value selects the discovery
mechanism — and, for `k8s`, the image variant and its RBAC automatically:

```sh
helm install dart deploy/helm/dart                            # DNS discovery, no RBAC
helm install dart deploy/helm/dart --set discovery.mode=k8s   # EndpointSlice watch
```

`deploy/helm/dart/values.yaml` documents the tunables (cache sizing, ports,
resources, registry upstream).

The default `dart` image needs **no RBAC and never contacts the Kubernetes API** —
discovery is DNS plus roster exchange, so it runs where Kubernetes specifics are
unavailable. Operators who want immediate, readiness-aware discovery can instead
use the **`dart-k8s` variant** (`docker build --target dart-k8s`), which watches
EndpointSlices via `-discover=k8s:<namespace>/<service>`; it is a separate module
(`providers/k8s`, documented in [`docs/k8s.md`](./docs/k8s.md)) so the client-go
dependency never enters the main build.

### Fluid (dataset-level orchestration)

[`deploy/fluid/`](./deploy/fluid) carries a `ThinRuntimeProfile` and a sample
`Dataset`/`ThinRuntime` pair that put DART behind
[Fluid](https://github.com/fluid-cloudnative/fluid)'s thin-runtime
orchestration: application pods mount a PVC, and the profile's fuse
container (a read-only HTTP-to-FUSE adapter) serves it from the DART
instance on the same node, so artifact reads are cached and P2P-shared
cluster-wide. Prerequisites — Fluid ≥ 1.1 with ThinRuntime support and DART
on every node at `hostPort` 19145 — and the known-path-read limitation of
HTTP-backed mounts are documented in the files' headers.

## Documentation

[`docs/`](./docs) has a reference document per package — API, semantics,
concurrency contract, test status and known limitations. Start with
[`docs/README.md`](./docs/README.md) for the index, or
[`docs/dart.md`](./docs/dart.md) for every flag.

Worth reading if you plan to touch the internals:
[`docs/hashring.md`](./docs/hashring.md) (placement and the tree),
[`docs/store.md`](./docs/store.md) (the two budgets and admission),
[`docs/peer.md`](./docs/peer.md) (transport, timeouts, circuit breaking).

## Roadmap

Implemented: block cache (disk arena, in-memory hot set, hybrid), owned/borrowed
budgets with TinyLFU admission, weighted HRW placement, pre-order distribution tree
with multi-hop relay, cut-through streaming, active-reader-set tree, fetch
coalescing, hedging, per-peer circuit breaking, Prometheus metrics and an admin
plane, registry mirror with private-registry authentication, presigned
object-storage upstreams.

…and peer discovery: DNS seeding via a headless Service plus roster exchange over
the existing peer connections, with asymmetric add/remove timing — or, in the
`dart-k8s` variant, an EndpointSlice watch feeding the same convergence.

Packaging: a multi-stage distroless image (plus the `dart-k8s` variant), raw
manifests and a Helm chart for the per-node DaemonSet shape, and Fluid
thin-runtime samples for dataset-level orchestration.

Next:

1. Policy engine: per-origin rules, mutability classification.
2. Throughput work toward the 100 Gbps-per-node target (`sendfile`/`splice` are
   already viable on the current data path; this is measurement and tuning).

Not planned: writable caching, cross-region cache coherence, membership consensus.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md) for the
setup, the checks CI runs, and the invariants that are easy to break. A few
conventions that are enforced rather than suggested:

- **Every code change updates the package document in `docs/`.** Documentation is
  treated as part of the code, not an afterthought.
- **Tests are deterministic.** No sleeps standing in for synchronization, no
  dependence on wall-clock timing or host performance.
- `gofmt`, `go vet` and `go test -race` must all be clean.
- **No external dependencies** without a discussion first. The standard library has
  been enough so far, and that has real value for a component that sits on the
  critical path of every image pull.

By contributing you agree that your contribution is licensed under Apache-2.0, per
section 5 of the [LICENSE](./LICENSE).

## Security

Please do not file security issues publicly; use private vulnerability reporting
as described in [SECURITY.md](./SECURITY.md). That document also states the trust
model you should assume when deploying — notably that the peer and admin planes
are unauthenticated and belong inside the cluster network.

## License

[Apache License 2.0](./LICENSE).
