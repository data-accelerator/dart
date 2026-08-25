# `internal/metrics` and `internal/admin`

DART's observability: a dependency-free Prometheus exporter plus the admin /
introspection endpoints.

- Source: `internal/metrics/metrics.go`, `internal/admin/admin.go`
- Tests: `internal/metrics/metrics_test.go`, `internal/admin/admin_test.go`
- Import paths: `github.com/data-accelerator/dart/internal/{metrics,admin}`

## 1. Overview

`internal/metrics` implements the Prometheus text exposition format directly
(no client_golang dependency), keeping the binary small and the data path free of
third-party allocation behavior. `internal/admin` serves `/metrics` plus
read-only JSON views for debugging placement and membership, intended for a
separate non-public listener (`cmd/dart -admin`).

The engine's instrumentation lives in `internal/engine/metrics.go`
(`engine.NewMetrics(registry)`, passed via `engine.Options.Metrics`). A nil
`*Metrics` disables instrumentation, so tests and embedders need not wire a
registry.

## 2. `internal/metrics` API

```go
r := metrics.NewRegistry()
c := r.NewCounter("dart_x_total", "help", metrics.LabelPair{Name: "k", Value: "v"})
g := r.NewGauge("dart_y", "help")
h := r.NewHistogram("dart_z_seconds", "help", []float64{0.1, 1, 10})
err := r.Render(w) // Prometheus text format
```

| Type | Methods | Notes |
|---|---|---|
| `Counter` | `Inc()`, `Add(int64)`, `Value() uint64` | monotonic; negative `Add` ignored |
| `Gauge` | `Set(float64)`, `Value() float64` | atomic float bits |
| `Histogram` | `Observe(float64)` | cumulative `le` buckets + `_sum`/`_count`; bounds sorted/de-duplicated; `+Inf` implicit |
| `Registry` | `NewCounter/NewGauge/NewHistogram`, `NewGaugeFunc/NewCounterFunc`, `Render(io.Writer)` | `HELP`/`TYPE` emitted once per metric name |

`NewGaugeFunc`/`NewCounterFunc` register a metric whose value is **sampled at
scrape time** by a callback. That is how state owned elsewhere — cache occupancy,
open circuits — is exported without the owning component having to push an update
on every insert, eviction, or failure on the hot path. The callback must be cheap
and safe for concurrent use, since it runs on the scrape path.

- Metric and label names must match `[a-zA-Z_][a-zA-Z0-9_]*`; an invalid name
  **panics at registration** (a startup programming error, not a runtime path).
- Label values are escaped (`\`, `"`, newline) per the text format.
- Registering the same metric name with different labels produces multiple series
  under one `HELP`/`TYPE` header (the normal Prometheus pattern).

## 3. Exported metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `dart_block_source_total` | counter | `source=cache\|peer\|origin` | blocks served, by where they came from |
| `dart_bytes_total` | counter | `direction=client\|peer_in\|origin_in` | bytes written to clients / pulled from peers / pulled from origin |
| `dart_relay_total` | counter | `result=served\|declined` | relay requests handled for peers |
| `dart_block_fetch_seconds` | histogram | `source=peer\|origin` | block-path fetch latency: cache fills and peer reads only — Size probes and the uncached passthrough path are **not** observed here (`dart_passthrough_total` counts the latter). A coalesced fetch still observes its latency (the caller genuinely waited) even though its bytes are not double-counted in `dart_bytes_total` |
| `dart_hedge_total` | counter | `event=fired\|primary_won\|backup_won` | tail-latency hedges: duplicates launched, and which contender served the block |
| `dart_peer_failover_total` | counter | — | definite peer failures escalated to the next ancestor (not rate limited, unlike hedges) |
| `dart_passthrough_total` | counter | `reason=range_unsupported` | requests proxied verbatim to origin, bypassing cache and P2P (Range-ignoring origin; see docs/engine.md §3.9) |
| `dart_peer_circuits_open` | gauge | — | peers whose circuit is currently open |
| `dart_store_blocks` | gauge | `class=owned\|borrowed\|mem` | cached blocks per cache class |
| `dart_store_slots` | gauge | `class=owned\|borrowed\|mem` | capacity in blocks per cache class |
| `dart_store_admit_rejected_total` | counter | — | borrowed candidates refused by TinyLFU admission |

Cache hit ratio is `dart_block_source_total{source="cache"}` over the sum of all
sources; P2P effectiveness is the `peer` share versus `origin`. For hedging,
compare `dart_hedge_total{event="backup_won"}` against `event="fired"`: a high
ratio means the duplicates are earning their bandwidth, a low one means the
trigger is too eager.

For the cache, `dart_store_blocks / dart_store_slots` per class shows how full
each budget is; a rising `dart_store_admit_rejected_total` against a full borrowed
budget means admission is doing its job (or that the budget is too small).
`dart_peer_circuits_open` above zero for long means a peer is genuinely sick, not
just briefly slow.

The store and breaker metrics are wired by `engine.RegisterStoreMetrics(reg, store)`
and `engine.RegisterPeerMetrics(reg, breaker)`; `cmd/dart` calls both.

## 4. `internal/admin` endpoints

| Method | Path | Response |
|---|---|---|
| GET | `/healthz` | `ok` (text) |
| GET | `/metrics` | Prometheus text format (`text/plain; version=0.0.4`) |
| GET | `/admin/stats` | JSON: `self_id`, `cached_blocks` |
| GET | `/admin/members` | JSON: `self_id`, `epoch`, `members[]{id,addr,weight,state}` |
| GET | `/admin/ring?key=<uint64>&n=<topN>` | JSON: HRW placement order for a chunk key (`n` default 8) |

```go
h := admin.Handler(admin.Options{
    Registry: reg, Store: st, Cluster: prov, SelfID: "A",
})
```

Behavior notes:

- Every dependency is optional: an endpoint whose dependency is nil returns
  `503` rather than preventing startup.
- `/admin/ring` requires a decimal `uint64` key; anything else is `400`. It ranks
  only **Ready** members (matching placement) and reports each node's address.
- `epoch` and `key` are rendered as JSON **strings** to avoid float64 precision
  loss for large uint64 values.
- The admin handler uses `http.ServeMux`: safe here because no admin path carries
  an embedded URL (unlike the client data plane, which must avoid path cleaning).

## 5. Concurrency & Call Permissions

- All metric types are safe for concurrent use (atomics for Counter/Gauge, a
  mutex for Histogram); `Render` snapshots under lock. Verified with `-race`
  (concurrent observers plus a concurrent renderer).
- `admin.Handler` is safe for concurrent use; it only reads from the registry,
  store, and the immutable cluster `View`.
- `engine.Metrics` helpers are nil-safe no-ops when instrumentation is disabled.

## 6. Stability Contract

- Metric names/labels are a **user-facing contract** (dashboards and alerts bind
  to them). Renaming or re-labeling is a breaking change; prefer adding new
  series. The exposition format itself is standard 0.0.4 text.
- Admin JSON shapes are debugging aids, not a stable API, but avoid gratuitous
  changes.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: `internal/metrics` **95.5%**, `internal/admin` **98.2%**.
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./internal/metrics/ ./internal/admin/ -v -count=1
go test ./internal/metrics/ ./internal/admin/ -race -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `metrics.TestCounter` / `TestGauge` | values and rendered HELP/TYPE/value lines |
| `metrics.TestLabels` | multiple label series share one HELP/TYPE header |
| `metrics.TestHistogramBuckets` | cumulative `le` buckets, `_sum`, `_count`, `+Inf` |
| `metrics.TestHistogramBoundsSortedAndDeduped` | bounds normalized |
| `metrics.TestInvalidNamePanics` | invalid metric/label names rejected |
| `metrics.TestLabelValueEscaping` | quotes/backslashes/newlines escaped |
| `metrics.TestFormatFloatSpecials` | `+Inf`/`-Inf`/`NaN` rendering |
| `metrics.TestEmptyRegistry` | renders nothing |
| `metrics.TestConcurrent` | concurrent observe + render (`-race`) |
| `metrics.TestGaugeFuncSampledAtRender` | a func metric resamples on every scrape |
| `metrics.TestCounterFuncTypedAsCounter` | func counters emit `TYPE counter` |
| `metrics.TestFuncMetricsWithLabels` | label series share one HELP/TYPE header |
| `metrics.TestFuncMetricsRejectNilAndBadNames` | nil callbacks and invalid names rejected |
| `admin.TestHealthz` | liveness |
| `admin.TestMetricsEndpoint` | scrape body and content type |
| `admin.TestStats` | `cached_blocks` reflects the store |
| `admin.TestMembers` | epoch, members, state names |
| `admin.TestRing` | Ready-only ranking, ranks, `n` limit, determinism |
| `admin.TestRingBadKey` | missing/non-numeric/negative key → 400 |
| `admin.Test*Unconfigured` | nil dependency → 503 |

## 8. Limitations & TODO

- **No process/Go runtime metrics** (goroutines, GC, RSS) — the official
  collector would add a dependency; a small hand-rolled set can be added.
- **No pprof** endpoint yet.
- **No per-peer latency series**: `dart_peer_circuits_open` is an aggregate; a
  per-peer breakdown (state, latency) would need a label per address, which is
  cardinality the exporter deliberately avoids for now.
- **No admin auth**: bind `-admin` to a private interface; it is read-only but
  reveals topology.

### 3.9 Reading the two kinds of counter

`dart_block_source_total` and `dart_bytes_total` answer different questions and do
**not** divide into one another. Mixing them up produces figures that are wrong by
the concurrency factor.

| Metric | Counts | When two readers coalesce onto one upstream fetch |
|---|---|---|
| `dart_block_source_total{source="origin"}` | block **reads** attributed to a source | **2** — both were local misses, and both belong in a hit ratio |
| `dart_bytes_total{direction="origin_in"}` | **wire** bytes from upstream | **1 block's worth** — the transfer happened once |

The byte counter deliberately skips a coalesced result (`fetch.Range.Coalesced`).
Counting per caller would report singleflight's savings as *extra* traffic: measured
against a real origin, three concurrent readers of one 122 MiB object made the origin
serve 122 MiB, while per-caller accounting claimed 366 MiB.

So: use the block counters for hit ratio, and the byte counters for load on the
origin and for throughput.

