# `providers/k8s`

Kubernetes-native peer discovery: a `cluster.Seeder` backed by an EndpointSlice
watch, shipped as the `dart-k8s` binary variant. This is a **separate Go
module** so its `client-go` dependency tree never enters the main module — the
plain `dart` binary and image keep an empty `go.sum` and need no RBAC.

- Source: `providers/k8s/seeder.go`, `providers/k8s/cmd/dart-k8s/main.go`
- Tests: `providers/k8s/seeder_test.go`, `providers/k8s/cmd/dart-k8s/main_test.go`
- Module: `github.com/data-accelerator/dart/providers/k8s` (pins the sibling
  main module via a `replace` directive; the two are tagged together)
- Deploy: `deploy/k8s/rbac.yaml` (required by this variant only)

## 1. Overview

DNS discovery works everywhere with zero credentials, but a headless Service's
answer lags endpoint changes by resolver TTLs, truncates past a response size
limit, and cannot express endpoint readiness. Watching the Service's
**EndpointSlices** fixes all three: changes arrive as they happen, and the
seeder reads each endpoint's `ready` condition directly.

Only the address source changes. Turning addresses into stable identities
(roster exchange), judging liveness (circuit breaker) and forgetting dead
members (forget-after grace) stay in `internal/cluster`'s `DynamicProvider`,
exactly as with DNS — a `Seeder` answers only "who might be out there", so the
`Joining/Ready/Suspect/Leaving` semantics and removal hysteresis are the same
code path on every platform.

The wiring uses the scheme mechanism from [node.md](./node.md): `dart-k8s`
registers `k8s.Scheme` with `node.Run`, enabling
`-discover=k8s:<namespace>/<service>[/<port>]`. The optional port segment is a
port name or number; absent, the port named `peer` is used (matching
`deploy/k8s/daemonset.yaml`, which names its peer Service port `peer`).

`dart-k8s` adds no flags of its own; the full set is in
[dart.md](./dart.md). Two are load-bearing here: `-self-id` (the node's
cluster identity — placement keys derive from it, so it must be stable and
cluster-unique; the DaemonSet wires it from the node name via the downward
API) and
`-peer-advertise` (peers dial this; `DART_ADVERTISE_HOST`/`POD_IP` resolve a
wildcard listen host, in that order).

## 2. Concepts

| Term | Meaning |
|---|---|
| EndpointSlice | The Kubernetes API object listing a Service's endpoints (~100 per slice), labeled `kubernetes.io/service-name=<svc>`. |
| ready condition | `endpoint.conditions.ready`; `nil` means ready (API default), explicit `false` excludes the endpoint. |
| snapshot | The sorted address set recomputed from the informer store on every add/update/delete event. |

## 3. Public API

```go
var Scheme = node.DiscoveryScheme{Name: "k8s", ...}   // register with node.Run

func NewSeeder(spec string) (cluster.Seeder, error)                 // in-cluster config, kubeconfig fallback
func NewSeederWithClient(client kubernetes.Interface, spec string) (cluster.Seeder, error)

func (s *Seeder) Run(ctx context.Context)            // drives the informer; started by node.Run
func (s *Seeder) Seeds(ctx context.Context) ([]string, error)
```

- Spec syntax: `<namespace>/<service>[/<port>]`; errors on missing segments, a
  numeric port outside 1–65535, or extra segments.
- `Run` blocks until `ctx` is cancelled; `Seeds` only reads the cached snapshot
  (never blocks on the API server) and before the first sync reports "nothing
  found right now", which membership treats like a DNS miss: keep known peers.
- `NewSeederWithClient` is how tests inject the fake clientset, and how an
  embedder with its own client lifecycle wires one in.

## 4. Invariants & Guarantees

- **Read-only, namespaced privilege**: the informer needs only `list`/`watch`
  on `endpointslices` in one namespace — a shared informer never issues
  single-object `get`s (see `deploy/k8s/rbac.yaml`); DART never writes to the
  API.
- **Snapshots are sorted**, so `Seeds` output is deterministic regardless of
  informer store order.
- **A slice without the selected port is skipped**, not an error — a rollout can
  legitimately transiently mix port layouts; a Service that never exposes the
  port simply yields an empty seed set (same handling as an empty DNS answer).
- **Eventual parity with DNS**: any membership reachable via DNS discovery is
  reachable here, because both feed the same roster-exchange convergence.

## 5. Concurrency & Call Permissions

- `Seeder` is safe for concurrent use (`Seeds` reads under RWMutex; `recompute`
  is called from informer event handlers).
- `Run` must be called at most once per seeder; `node.Run` arranges this.

## 6. Stability Contract

- The spec syntax and the `k8s` scheme name are the operator-facing contract.
- The module follows the main module's tags; the `replace ../..` directive means
  it always builds against the sibling checkout, never a stale released version.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **80.8%** of statements (`NewSeeder`'s client-config probing is
  the main uncovered part — it needs a real API server or kubeconfig).
- **Reproduce**:

```bash
cd providers/k8s
go test ./... -race -cover -count=1
```

| Test | Property guarded |
|---|---|
| `TestParseSpec` | spec syntax: defaults, named/numeric ports, malformed specs rejected |
| `TestSeedsBeforeSyncIsEmpty` | no stale/empty claim before the first informer sync |
| `TestReadyEndpointsOnly` | `ready:false` excluded, `nil` ready included, addressless excluded, output sorted |
| `TestEndpointChurn` | endpoint add (scale-out) and slice delete (scale-in) reflected live |
| `TestPortSelection` | default/explicit named and numeric port selection; missing port skips the slice |
| `TestSchemeShape` | the scheme registers as `k8s` with a constructor and usage text |
| `TestSchemes` (`cmd/dart-k8s`) | the variant wires exactly dns + static + k8s |

## 8. Limitations & TODO

- The informer resyncs never (`resyncPeriod = 0`): watch events plus relist-on-
  reconnect are the update path; a silently missed event would persist. A slow
  periodic resync is a candidate hardening.
- **A partitioned apiserver means frozen membership, served silently**: the
  informer keeps reconnecting and `Seeds` serves the last-known snapshot
  indefinitely — by design (membership loss is worse than staleness for a
  read-only cache). There is no in-process staleness gauge: "time since last
  event" cannot distinguish a quiet cluster from a broken watch, and watch
  errors stay inside client-go's retry loop. Detect an apiserver partition via
  cluster-side signals (apiserver latency, other controllers), not via DART.
- Weights are not read from Kubernetes metadata (all members self-report weight
  1); if heterogeneous capacities become real, pod annotations are the natural
  source, plus hysteresis at the provider level (see cluster.md §8).
- Only IPv4/IPv6 single-address-per-endpoint is used (first address); dual-stack
  preference is undefined.
