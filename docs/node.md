# `internal/node`

Node assembly: everything a DART command needs — flag parsing, store/engine
construction, the client/peer/admin HTTP planes, peer discovery wiring — behind
one `Run` function, so that a binary is a thin shell that only chooses which
discovery schemes it links.

- Source: `internal/node/node.go`, `internal/node/scheme.go`, `internal/node/discover.go`, `internal/node/size.go`
- Tests: `internal/node/node_test.go`, `internal/node/scheme_test.go`, `internal/node/discover_test.go`, `internal/node/size_test.go`
- Import path: `github.com/data-accelerator/dart/internal/node`
- Used by: `cmd/dart`, `providers/k8s/cmd/dart-k8s`

## 1. Overview

`cmd/dart` used to own node construction directly. It was extracted here so that
**discovery can be extended by binaries, not by this repository's dependency
list**: the Kubernetes EndpointSlice scheme needs `client-go`, which the main
module refuses to carry (its `go.sum` stays empty). The split that makes this
work:

- `internal/node` holds all assembly logic and the *scheme dispatch point*
  (`DiscoveryScheme`), and depends only on the standard library plus
  `internal/*`.
- A command (`cmd/dart`, `providers/k8s/cmd/dart-k8s`) passes the schemes it
  ships to `Run`. Because Go's `internal/` visibility is decided by import-path
  prefix, a module at `github.com/data-accelerator/dart/providers/k8s` may
  import `internal/node` — no public API surface had to be exported.

Flags, origin-resolution modes, the registry mirror dispatch and P2P wiring
behave exactly as documented in [dart.md](./dart.md); this document covers only
what is new: the assembly API and the scheme mechanism.

## 2. Concepts

| Term | Meaning |
|---|---|
| scheme | The `<name>` in `-discover=<name>:<spec>`; selects a `DiscoveryScheme`. |
| spec | The scheme-specific remainder of the `-discover` value (`<name>:<port>` for dns, `<namespace>/<service>` for k8s). |
| seeder lifecycle | A seeder that watches something (an informer) needs a background goroutine; the convention below starts it. |

Registration is **explicit**: schemes are a `Run` argument, never an `init()`
side effect or a global registry, so what a binary accepts is visible in its
`main` and testable as data.

## 3. Public API

### 3.1 `Run`

```go
func Run(args []string, out io.Writer, version string, schemes ...DiscoveryScheme) error
```

The node's whole lifecycle: parse flags (`args`, without argv[0]; parse errors
and usage go to `out`), build the node, serve until `SIGINT`/`SIGTERM`, then
shut down with a 10s drain **per server, concurrently** (a slow client drain
cannot starve the peer/admin shutdowns). If one server dies early (e.g. a bind
failure), the siblings are shut down before `Run` returns — nothing keeps
serving over the closed store. Discovery diagnostics go to `out`, throttled to
one line per minute with a suppression count. `version` is what `-version`
prints — commands stamp
theirs via `-ldflags "-X main.version=..."` and pass it through.

**Handler-lifetime contract** (the precise statement behind "nothing keeps
serving over the closed store"): every server handler runs behind a closeable
**admission gate** — a mutex-serialized counter, not a WaitGroup, so "admit +
count" and "close admission" cannot interleave (an `Add` after the count hit
zero racing `Wait` would be outside the WaitGroup contract). A server whose
drain exceeds its budget is force-closed: the gate closes first (new requests
get 503 without touching the store), connections close, then the handlers that
had already been admitted are joined for up to a 2s grace — `Server.Close`
closes connections but does not join handler goroutines, hence the explicit
wait. If the grace expires with a handler still live, `Run` returns **without
closing the store or releasing the cache-dir lock** and logs the abandon:
closing a store under a live handler would be use-after-close (its contract
says "must not be used afterwards"), while leaving them open is reclaimed by
the OS when the command exits — and the held lock keeps another node from
opening the cache dir meanwhile. In practice every dart handler unwinds on
request-context cancellation, so the abandon path is only reachable by a
handler that ignores cancellation past 10s drain + force-close + 2s grace.

`schemes` is the discovery-scheme table for `-discover`; see §3.2. Errors from
flag parsing, validation, store/engine construction and `ListenAndServe` are
returned, not logged-and-continued.

### 3.2 `DiscoveryScheme`

```go
type DiscoveryScheme struct {
    Name  string                                  // as written in -discover=<Name>:<spec>
    Usage string                                  // help-text rendering; empty -> "<Name>:<spec>"
    New   func(spec string) (cluster.Seeder, error)
}

var DNSScheme    = DiscoveryScheme{Name: "dns", ...}    // -discover=dns:<name>:<port>
var StaticScheme = DiscoveryScheme{Name: "static", ...} // -discover=static:<a>,<b>,...
```

Dispatch: the value of `-discover` is cut at the first `:`; if the head names a
registered scheme, its `New` receives the spec. Otherwise the value falls back
to `cluster.ParseSeeder`, which keeps the historical behaviors working with no
registration at all: `dns:`/`static:` resolve, and a bare `a:port,b:port` list
is a static list. The fallback can never reach code the module does not contain
— `k8s:` only gains meaning when its scheme is registered (it otherwise parses
as one odd static address whose dial failures surface via discovery's error
reporting).

**Seeder lifecycle convention**: a seeder built by `New` that needs a background
lifecycle should expose `Run(context.Context)`; `node.Run` starts it alongside
the discovery loop and expects it to return on cancellation. The assertion is
structural (`interface{ Run(context.Context) }`), so seeders need no extra
import. Seeders without one (DNS, static) implement only `cluster.Seeder`.

### 3.3 `RosterRoute`

```go
const RosterRoute = peer.RosterPath
```

The exact path the roster server is mounted on (no trailing slash, so a
`ServeMux` does not shadow the `/peer/` block routes).

## 4. Invariants & Guarantees

- **What a binary accepts is enumerable**: `-discover`'s help text is rendered
  from the registered schemes (`Usage` fields), so `--help` always reflects the
  linked set.
- **Backward compatibility**: existing `-discover=dns:...`, `=static:...` and
  bare-list values resolve identically with zero registered schemes.
- **No hidden global state**: no `init()` registration; two `Run` callers with
  different scheme sets do not interfere.
- One `-cache-dir` still means one node (the store lock predates this package;
  see dart.md §5).
- **The origin HTTP client is bounded**: the transport requires response headers
  within 30s (an origin that accepts and goes silent must fail, not hang), and
  each coalesced flight is bounded by `fetch.MaxFlight` (default 10m). The body
  itself has no whole-request timeout — whole-object passthrough streams may
  legitimately run long.
- **The startup banner never prints credentials**: userinfo embedded in
  `-origin`/`-registry` is stripped before the banner is written to stdout
  (container logs).

## 5. Concurrency & Call Permissions

- `Run` is not reentrant per cache directory and is expected to be called once,
  from `main`; it blocks until shutdown.
- `DiscoveryScheme.New` is called at build time (single goroutine). A seeder's
  `Run` and `Seeds` are invoked concurrently once serving starts and must be
  goroutine-safe.
- Returned seeders are owned by the node; callers must not retain or mutate
  them.

## 6. Stability Contract

- The flag set, `-discover` value syntax and the fallback behavior are the
  command-line contract; changing them breaks deployments.
- `DiscoveryScheme` is `internal/`: its shape may evolve with the in-repo
  providers. It is deliberately *not* a public plugin API — an out-of-repo
  provider would need a deliberately narrowed exported seam (e.g. a future
  `pkg/discovery`), which is a separate design decision.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **77.0%** of statements (the tests moved here with the code from
  `cmd/dart`; the uncovered remainder is the blocking serve loop and signal
  handling).
- **Reproduce**:

```bash
go test ./internal/node/ -v -count=1
go test ./internal/node/ -race -count=1
```

### Test list (property each guards)

The pre-extraction tests are listed in [dart.md §4](./dart.md); they now live in
this package (`node_test.go`, `discover_test.go`, `size_test.go`). Added with
the extraction:

| Test | Property guarded |
|---|---|
| `TestResolveSeederDispatch` | registered scheme receives the spec; dns/static/bare fall back unregistered; a nil constructor is a loud error, not a panic |
| `TestUnregisteredK8sSpecDoesNotMatch` | `k8s:` without the scheme never reaches Kubernetes code (static fallback) |
| `TestSchemeUsage` | help text lists registered schemes, historical text when none |
| `TestRunPrintsVersion` | `-version` prints the version passed to `Run` |
| `TestSelfIDRejectsControlBytes` / `TestParsePeersRejectsControlByteIDs` | control-byte member IDs rejected at `-self-id` and `-peers` (epoch-framing safety) |
| `TestWildcardAdvertiseRejected` | explicit wildcard `-peer-advertise` values are rejected (incl. IPv6 long/zoned forms); loopback stays valid |
| `TestOwnedFractionFlagValidation` | an explicit out-of-range or non-finite `-owned-fraction` fails startup |
| `TestParseByteSizeExact` | plain byte counts are exact above 2^53; overflow errors |
| `TestDiscoveryErrorsRoutedAndThrottled` | discovery errors go to `out`, one line/minute with a suppression count |
| `TestOutWriterIsSerialized` | all out writes are serialized through the lockedWriter |
| `TestEarlyListenerFailureShutsSiblings` | an early bind failure shuts down the peer/admin servers before Run returns |
| `TestAdmissionGateRejectsAfterCloseWithoutCounting` / `TestAdmissionGateStress` | the admission gate rejects post-close entries without touching the count; 20 rounds of concurrent enter/exit/close/wait under -race |
| `TestServerSetAbandonsWedgedHandler` / `TestServerSetJoinsCooperativeHandler` | a handler ignoring cancellation is abandoned (not closed-under) after drain+grace; a cooperative handler is joined cleanly |
| `TestFinishRunLeavesStoreOpenWhenAbandoned` | abandon path: store/cache-dir lock deliberately left open (LockDir still fails); clean path: lock released |
| `TestRedactURLUserinfo` | the startup banner strips URL userinfo (no credentials in logs) |
| `TestBuildKeepsSeederForLifecycle` | the built node retains its seeder and its optional `Run(ctx)` stays assertable |
| `TestSchemes` (in `cmd/dart` and `providers/k8s/cmd/dart-k8s`) | each binary wires exactly its intended scheme set |

## 8. Limitations & TODO

- The serve loop and signal handling remain integration-tested only (a manual
  run / `deploy/verify.sh`), as before the extraction.
- Scheme dispatch cuts at the first `:`; a spec therefore cannot itself start
  with something that looks like a scheme prefix (no current scheme needs one).
