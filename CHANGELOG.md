# Changelog

All notable changes to DART are documented here. This project follows
[Semantic Versioning](https://semver.org/); the wire protocol (`Hash64`, the
HRW score formula, `ChunkKey`, and epoch framing) is called out explicitly in
every release's upgrade notes.

## [v0.2.0] — 2026-08-26

The first release since v0.1.0: **30 merged fix PRs** (#23–#44, #47, #48,
#56–#61) plus packaging and discovery additions that landed as direct commits.
No breaking changes; rolling upgrades from v0.1.0 are safe for well-formed
inputs (see Upgrade notes).

### Highlights

- First tag containing the **Helm chart** (`deploy/helm/dart`) and **Fluid**
  ThinRuntimeProfile/Dataset samples (`deploy/fluid`).
- First tag containing the **Kubernetes EndpointSlice discovery** module
  (`providers/k8s`, a separate Go module keeping the main module
  dependency-free) and the `dart-k8s` command.
- The minimalist design assumptions the cache semantics rest on are now
  documented in one tracked place (`docs/design-assumptions.md`, #28).

### Additions

Four changes were committed directly (no PR) on 2026-08-13:

- `2a53b2b` — `internal/node` extracted from `cmd/dart`; EndpointSlice
  discovery added as the separate `providers/k8s` module with the `dart-k8s`
  binary.
- `8e6a6a3` — Helm chart for the DaemonSet shape (discovery.mode=dns|k8s) and
  Fluid ThinRuntimeProfile + Dataset samples.
- `b2b7d5b` — engine passes non-range origins straight through, uncached.
- `f21f05d` — tracker evicts idle file entries.

### Fixes

- **engine / fetch data path**: validate relayed block length against geometry
  before caching (#23); fail the size probe loudly when a 206 hides the total
  (#25); bound the shared flight, cancel-aware joiner retry (#26); six audit
  fixes — relay decline, hedge metrics, estimator, reader cache, stream
  fallback, empty object (#33); past-EOF blocks error loudly, RFC-clamped tail
  206 accepted (#38); reject suffix ranges on empty objects (#56); wait
  coalescing joiners inline, one worker goroutine per flight (#60); reject peer
  block indices that overflow signed range geometry (#61); redact credentials
  from transport errors and the startup banner (#29).
- **peer transport**: stop charging caller cancellation to the circuit
  breaker; bound breaker state (#30); hard-bound the breaker map under
  failed-address churn (#58); reject invalid `X-DART-Hop` values at both
  servers (#57).
- **cluster / membership**: credit liveness to the answering member's ID, not
  the dialed address (#24); hearsay liveness, publication races, and
  dedup-priority fixes (#35); member-ID alphabet guards the epoch framing;
  lifecycle/tracker docs (#44); report invalid roster IDs via OnError; assert
  the epoch-collision regression unconditionally (#47).
- **store / metrics**: scope the TinyLFU estimator to borrowed traffic;
  class-migration and admission fixes (#34); O(1) MemStore.Len via an atomic
  counter and HELP text escaped per the Prometheus text format (#41).
- **node lifecycle**: lifecycle hardening, flag validation, exact byte sizes,
  routed+throttled diagnostics (#40); closeable admission gate — the store is
  never closed under a live handler (#48).
- **registry mirror**: document the real credential rule for the token realm
  (#32); client-credential precedence, digest-classifier alignment, path
  encoding, token-cache hygiene (#39); detach the token singleflight from the
  leader's context (#59).
- **tracker**: clamp client-supplied lease TTL; guard duration overflow (#27).
- **wire-adjacent hardening**: separator exclusion, fallback query stripping,
  score-comment honesty (#36); golden reference implementations committed for
  hashring/chunk/cluster, weighted-rank goldens, zero-alloc tree navigation
  (#37).
- **docs / posture**: trusted-origin assumption for digest-keyed caching (#31);
  the design-assumptions document (#28); `dart_block_fetch_seconds` inclusion
  rules (#42); least-privilege RBAC, staleness posture, identity-flags pointer
  (#43).

### Upgrade notes

Rolling upgrades from v0.1.0 are safe: all wire-protocol derivations — the
`Hash64` construction, the HRW `score` formula, the `ChunkKey` mix sequence,
and the epoch framing bytes — are **byte-identical** to v0.1.0 for well-formed
inputs. Two derivations changed for edge inputs only:

- Unparseable or scheme-less fallback URLs now cut the query/fragment, and
  derived object identities strip a literal 0x1F byte (reachable via a
  percent-encoded `%1F` in the path). A mixed-version cluster may split cache
  identity for such malformed URLs — extra origin fetches, no correctness
  impact (content still comes from the same origin; digest-addressed content
  stays verified).
- The duplicate-ID dedup winner in `NewView` now follows the lifecycle rank
  Ready > Suspect > Joining > Leaving. This is only reachable when the same
  member ID appears with different states in one input — realistically a
  duplicated ID in a static `-peers` list.

Peer-facing rejections (`X-DART-Hop` validation, the `MaxBlockIndex` bound)
only affect traffic a v0.1.0 peer never legitimately produces.

No configuration changes are required. The Helm chart bumps `appVersion` to
`v0.2.0`; chart version moves to 0.2.0.

### Verification

At the tagged commit: `go vet ./...` clean; `go test ./... -race -count=1` —
all packages pass; `go test ./... -cover -count=1` — all pass; GitHub Actions
CI green.

## [v0.1.0] — 2026-08-06

Initial tagged release: read-only P2P cache node with weighted-HRW placement,
preorder distribution tree, two-tier block store, registry pull-through
mirror, dependency-free Prometheus exporter, and K8s/DNS discovery wiring.
