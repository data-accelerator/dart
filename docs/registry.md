# `internal/registry`

DART as a container registry **pull-through mirror**, so containerd can be
pointed at it with a `hosts.toml` entry.

- Source: `internal/registry/registry.go`
- Tests: `internal/registry/registry_test.go`
- Import path: `github.com/data-accelerator/dart/internal/registry`

## 1. Overview and how it relates to OverlayBD

This is the front-end for **containerd's own pulls**: manifest resolution and
layer blobs for ordinary images. containerd sends ordinary Registry v2 requests
carrying the *upstream* paths, and DART decides per path whether the response is
cacheable:

```
GET /v2/library/nginx/blobs/sha256:<hex>   -> served from cache, range-capable
GET /v2/library/nginx/manifests/latest     -> passed through, never cached
GET /v2/                                   -> passed through
```

**This is not how OverlayBD fetches layer data.** OverlayBD uses its own
`p2pConfig` HTTP API, `GET /<APIKey>/<full upstream URL>`, which DART serves
through `engine.Handler`'s **prefix resolver** (`-prefix`, see docs/dart.md) — a
front-end that has existed since M1 and needs no registry knowledge at all.

The two are **complementary, not alternatives**, and `cmd/dart` runs both on one
listener:

| Client | Front-end | Path |
|---|---|---|
| containerd (resolve + pull) | registry mirror (`-registry`) | `/v2/...` |
| OverlayBD (layer ranges) | prefix API (`-prefix`) | `/<APIKey>/<upstream URL>` |

Because both key a blob on its **digest**, a layer fetched through either
front-end satisfies the other from cache. Requests are routed on `RequestURI`
rather than by an `http.ServeMux`, because the prefix paths embed a full URL and
a ServeMux would path-clean the `//` in `https://`.

## 2. What is cacheable, and why only that

**Only digest-addressed blobs are cached.** A blob URL names its own content
hash, so it is immutable: caching it can never serve the wrong bytes, and the
digest doubles as a cache key. Because `chunk.ObjectID` already prefers an
embedded digest, the same layer pulled through different registries or
repositories is stored **once**.

> **Trust assumption (see [design-assumptions.md](./design-assumptions.md) A1
> and A6):** the "can never serve the wrong bytes" invariant assumes an honest
> origin. DART does **not** recompute the digest or compare it with
> `Docker-Content-Digest` at ingest — deliberately, because hashing every byte
> would tax the 100 Gbps data path and the origin is the trusted source of
> truth. If a compromised origin serves wrong bytes for a digest, those bytes
> are cached under that digest permanently (the store is write-once; eviction
> or a wipe is the only invalidation) and re-served cluster-wide. The failure
> is still *loud* for content-addressed clients — containerd verifies the
> digest of what it pulled and errors — so the blast radius is a persistent
> pull failure, never silent execution. If digest verification at ingest is
> ever wanted, it belongs behind an explicit option, not a silent default.

**Manifests are deliberately never cached.** A manifest reference is usually a
tag, and tags are mutable — `:latest` points at different content over time.
Caching them would pin a stale image, which surfaces to users as "my deployment
did not pick up the new build". Manifests are small, so passing them through
costs little; the bytes that dominate pull time are the layers.

Manifests *by digest* are immutable, but they are still passed through: the
benefit is negligible and the cost of ever misclassifying a tag as a digest is
not, so the rule stays simple and conservative.

**Pull-only.** A mirror advertises `pull`/`resolve`, so write methods are
refused with `405` rather than proxied. Forwarding a push through a cache invites
a cache that disagrees with its upstream.

## 3. Public API

```go
type Options struct {
    Upstream  string             // registry to mirror, e.g. https://registry-1.docker.io
    Engine    *engine.Engine     // serves cached blob bytes (required)
    Transport http.RoundTripper  // pass-through transport; nil = http.DefaultTransport
}
func New(Options) (*Mirror, error)      // Mirror implements http.Handler
func BlobDigest(path string) (string, bool)
```

- `New` validates the upstream: it must parse, be `http`/`https`, and have a
  host. A trailing `/` is trimmed; a path prefix is preserved and prepended to
  every upstream request (for registries served under a subpath).
- `BlobDigest` classifies a path. It is anchored on the **last** `/blobs/`
  separator because a repository name may itself contain slashes, and it rejects
  anything it does not fully understand: upload paths
  (`/v2/<name>/blobs/uploads/...`), an empty name or digest, an extra path
  segment after the digest, a non-lowercase algorithm, or a leading/trailing
  separator in the algorithm.
- `Mirror.ServeHTTP` is the single dispatch point: blob paths go to the engine
  (when the engine declines — e.g. the origin cannot serve ranges — it answers
  via a direct passthrough proxy to the upstream), everything else passes
  through. It is safe for concurrent use, as any `http.Handler` must be.
- Blob responses carry `Docker-Content-Digest` (echoed from the path, which *is*
  the digest) and `Content-Type: application/octet-stream`.
- Pass-through uses `httputil.ReverseProxy` with `SetXForwarded`, and rewrites
  the outgoing `Host` to the upstream's — registries route on `Host` and TLS
  needs the right SNI.

## 4. Invariants & Guarantees

1. **Never serves stale mutable content**: anything that is not a digest-addressed
   blob reaches the upstream on every request.
2. **Byte-exact blobs**: blob bodies come from the engine, so they are exactly the
   upstream bytes; `Range` is honored and responses are `Content-Length` framed
   (never chunked) **on the block-served path**. An upstream that ignores
   `Range` is proxied verbatim via the engine's passthrough, which forwards the
   upstream's own framing — chunked included (see docs/engine.md §3.9).
3. **Digest dedup**: two upstreams or repositories serving the same digest share
   one cache entry. (The digest is trusted as the content's name — ingest does
   not verify bytes against it; see the trust assumption in §2.)
4. **No writes**: non-`GET`/`HEAD` never reaches the upstream.

## 5. Authentication and the trust model

Private registries reject anonymous reads, and most of them (Docker Hub, GCR,
GAR, Quay, Harbor in token mode) do it with the Distribution **token flow**
rather than plain Basic:

1. the request is answered `401` with
   `WWW-Authenticate: Bearer realm=...,service=...,scope=...`;
2. the client GETs that realm with its username/password to obtain a token;
3. the request is retried with `Authorization: Bearer <token>`.

`AuthTransport` implements this as an `http.RoundTripper`:

```go
func LoadCredentials(path string) (map[string]Credential, error)
func NewAuthTransport(base http.RoundTripper, creds map[string]Credential) *AuthTransport
// AuthTransport.RoundTrip attaches/caches/exchanges the bearer token per
// request (singleflight per realm+scope, conditional drop-on-rejection) and
// otherwise delegates to base. Safe for concurrent use.
```

**Why a RoundTripper rather than threading a credential through the read path.**
Two reasons, the second decisive:

- no signature changes, and the *same* transport covers both the mirror's
  pass-through proxy and the engine's cached blob fetches;
- `fetch.Coalescing` runs a shared fetch on a **bounded background context**, so
  a request-scoped credential carried in the context would be silently dropped
  for every deduplicated caller.

The credential is therefore a property of the **upstream**, configured once —
which is how a pull-through cache normally authenticates.

Behavior:

- **Credentials are keyed by host — and are also sent to the token realm the
  upstream names.** Resource requests carry a credential only to a host present
  in the map: registries redirect blob downloads to a CDN, the transport is
  invoked again for that host, and sending the registry credential along would
  leak it. The **token exchange is different by design**: it GETs whatever
  `realm` URL the upstream's `WWW-Authenticate` header designates, on whatever
  host that is (e.g. `registry-1.docker.io` → `auth.docker.io`), carrying the
  operator credential. The realm is **trusted as delivered by the upstream**;
  DART deliberately does not validate it against an allowlist — per-vendor
  token topologies differ, and such configuration would be a permanent
  maintenance burden (see [design-assumptions.md](./design-assumptions.md) A2
  and SECURITY.md). Securing the path to the upstream is the operator's
  responsibility: a MITM on a plain-http upstream hop could steer the token
  exchange, so plain-http upstreams belong on trusted networks only.
  `TestAuthTransportTokenRealmIsTrustedAsDelivered` pins this behavior.
- **Tokens are cached** per (host, repository), keyed by a scope **derived from
  the request path** rather than by the scope the registry advertised. The cache
  is consulted before any challenge is seen, so only a request-computable key
  keeps store and lookup symmetric; keying on the advertised scope would make a
  registry that formats it differently miss the cache on every block.
- **Token exchanges are singleflighted** per cache key, and the shared flight
  runs on a **bounded background context** (1 minute) — the same semantics as
  `fetch.Coalescing`: a caller's own context bounds only how long that caller
  waits, never the exchange itself. A cancelled leader therefore cannot poison
  still-live followers with `context.Canceled` (issue #51), a completed flight
  warms the cache even if no waiter remains, and a stalled token endpoint
  cannot pin the cache key beyond the bound.
- **Near-expiry tokens are not reused** (30 s leeway), and a token the registry
  stops accepting triggers re-authentication rather than a failed read.
- **A `Basic` challenge is satisfied directly**, no token endpoint (Harbor and
  several managed registries accept Basic on `/v2/`).
- **A failed authentication surfaces the registry's own `401`**, not an opaque
  transport error.

Credential file (`-registry-auth`), keyed by `URL.Host` including any non-default
port:

```json
{
  "registry-1.docker.io": {"username": "u", "password": "p"},
  "harbor.internal:5000": {"username": "robot$ci", "password": "..."}
}
```

**One cache per trust domain.** Blobs are keyed by digest, so every client of a
DART deployment shares cached layers. That is the intent for a cluster-internal
accelerator (it matches how containerd's own content store behaves locally), but
it means DART must not be shared across tenants that are not allowed to read each
other's images — all the more so now that a mirror-level credential can fetch
private content on their behalf. Per-tenant isolation would require keying the
cache namespace by credential.

## 6. Deployment

Run DART on each node. Both front-ends share the listener and the cache, so one
instance serves containerd and OverlayBD together:

```bash
dart -registry https://registry-1.docker.io -prefix dart -listen :8080 \
     -registry-auth /etc/dart/creds.json     # omit for public registries
```

`/etc/containerd/certs.d/docker.io/hosts.toml`:

```toml
server = "https://registry-1.docker.io"

[host."http://127.0.0.1:8080"]
  capabilities = ["pull", "resolve"]
```

OverlayBD's `p2pConfig` points at the same address with the matching API key
(`-prefix`), and reads layers as
`http://127.0.0.1:8080/dart/https://registry-1.docker.io/v2/...`.

`-prefix` must not be `v2`, which would shadow the registry API; `cmd/dart`
refuses that combination at startup. containerd falls back to `server`
automatically if the mirror is unavailable, so a DART outage degrades to a direct
pull rather than a failed one. Add `-peers`/`-self-id` to enable P2P between
nodes (see docs/dart.md).

## 7. Concurrency & Call Permissions

- `Mirror.ServeHTTP` is safe for concurrent use, as any `http.Handler` must
  be; it holds no per-request mutable state of its own — blob serving defers
  to the engine's own concurrency contract (docs/engine.md §5).
- `AuthTransport.RoundTrip` is safe for concurrent use: the token cache is
  mutex-guarded; token exchanges are singleflight-shared per (registry host,
  path-derived scope) — the cache and inflight maps key on `tokenKey(host,
  scope)`, the challenge realm is trusted as delivered (§5);
  a stored cache entry is never mutated, and drop-on-rejection is conditional
  on the rejected value (`dropTokenIf`), so a concurrent fresh store always
  survives.
- The pass-through reverse proxy is stateless between requests.
- Call order: `New` first; there is no `Close` — the mirror owns no resources
  beyond its transport.

## 8. Stability Contract

- **Breaking**: widening or narrowing the path set `BlobDigest` accepts as
  cacheable. The classifier must stay exactly aligned with `chunk.IsDigest`
  (§3.4.1 of docs/chunk.md); changing either side without the other splits
  cache identity between the mirror and content-addressed clients.
- **Breaking**: the blob response contract — `Docker-Content-Digest` echoed
  from the path, `Content-Type: application/octet-stream`, and range
  semantics inherited from the engine (200/206/416, Content-Length framed).
- **Contract (assumption-backed)**: the trust model of §5 — the trusted
  read-only origin (A1) and the realm-as-delivered rule — is part of this
  stability contract. Weakening it is a T2/T3-triggered change requiring an
  ADR (docs/adr/README.md).
- Pass-through behavior (Host rewrite, X-Forwarded-* via `SetXForwarded`) is
  observable to upstreams and treated as stable.

## 9. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **86.4%** of statements.
- **Reproduce**:

```bash
export TMPDIR=$PWD/.gotmp   # plus the cache dirs from docs/README.md
go test ./internal/registry/ -v -count=1
go test ./internal/registry/ -race -count=1
```

Tests run against a fake Registry v2 upstream with per-path request counters, so
"cached" and "passed through" are demonstrated by upstream hit counts rather than
asserted.

| Test | Property guarded |
|---|---|
| `TestBlobDigestClassification` | 4 cacheable and 16 non-cacheable path shapes, incl. upload paths, empty name/digest, extra segments, bad algorithms |
| `TestNewValidatesOptions` | missing engine/upstream, unparseable URL, non-HTTP scheme, missing host |
| **`TestBlobIsCached`** | **the second pull of a blob does not touch the upstream** |
| `TestBlobRangeRequest` | `206` + correct `Content-Range`, correct bytes, never chunked |
| `TestBlobHead` | `HEAD` returns size and digest without a body |
| **`TestManifestNotCached`** | **after the tag's content changes, the next pull returns the new content** (a cache would return the old) |
| `TestManifestByDigestAlsoPassesThrough` | manifests are never cached, digest or not |
| `TestPingPassesThrough` | `/v2/` reaches the upstream |
| `TestProxySetsUpstreamHost` | the upstream sees its own `Host`, not the mirror's |
| `TestProxyForwardsAuthorization` | client credentials reach the upstream on the pass-through path |
| `TestClientAuthorizationTakesPrecedence` | a client's own Authorization is never overwritten/substituted by the operator credential |
| `TestConcurrentColdRequestsShareOneExchange` | token exchange is singleflighted per repository |
| **`TestLeaderCancellationDoesNotPoisonFollowers`** | **a cancelled singleflight leader cannot fail still-live followers; the cohort still costs one exchange** |
| `TestCancelledFollowerDoesNotAbortFlight` | a departing follower does not cancel the shared exchange |
| `TestExpiredTokensPurged` | the token cache sweeps expired entries and stays bounded |
| `TestPercentEncodedPathReachesUpstreamVerbatim` | a %3F in a repository name is never decoded into a query |
| `TestPullOnly` | POST/PUT/PATCH/DELETE → `405` with `Allow`, and never reach the upstream |
| `TestNonV2PathRejected` | non-registry paths → `404` |
| `TestUpstreamPathPrefixPreserved` | an upstream subpath is prepended |
| **`TestBlobDedupAcrossRegistries`** | **the same digest via two mirrors/repos is fetched once** |

Authentication (`auth_test.go`, plus the issue-pinning `issue16_test.go` and
`issue51_test.go`), against a fake registry that speaks the token
flow and derives its advertised scope per repository:

| Test | Property guarded |
|---|---|
| `TestAuthTransportTokenExchange` | a `401` Bearer challenge becomes a token and a retry, transparently |
| `TestAuthTransportTokenRealmIsTrustedAsDelivered` | the token request carries the operator credential to the realm host verbatim, cross-host, by design (§5) |
| **`TestAuthTransportCachesToken`** | **5 requests trigger 1 token exchange** (otherwise every block pays a 401) |
| `TestAuthTransportRefreshesRejectedToken` | a rotated/rejected token is re-fetched rather than failing the read |
| `TestAuthTransportShortTTLNotCached` | a token expiring inside the leeway is not reused |
| **`TestAuthTransportNeverLeaksCredentialToOtherHosts`** | **a redirect to a CDN carries no registry credential** |
| `TestAuthTransportUnknownHostUntouched` | unconfigured hosts get no `Authorization` |
| `TestAuthTransportBasicChallenge` | a Basic challenge is satisfied without a token endpoint |
| `TestAuthTransportSurfacesRegistry401` | a bad operator credential surfaces the registry's `401` |
| `TestAuthTransportScopePerRepository` | tokens are per repository, not shared across them |
| **`TestAuthTransportCachesDespiteScopeFormat`** | **an advertised scope we would not derive still caches** |
| `TestScopeFor` / `TestParseChallenge` | scope derivation; challenge parsing incl. a comma inside a quoted scope |
| `TestAuthTransportConcurrent` | concurrent authenticated requests (`-race`) |
| `TestMirrorWithAuthTransport` | a private blob is served through the mirror and cached |

In `cmd/dart`:

| Test | Property guarded |
|---|---|
| **`TestBothFrontEndsShareOneCache`** | **a blob pulled through the mirror is then served to the OverlayBD prefix API with zero further upstream requests** |
| `TestPrefixCollidingWithRegistryPathRejected` | `-prefix v2` alongside `-registry` is refused at startup |
| `TestBuildRegistryMirror` | `-registry` mounts the mirror; a bad upstream fails the build without leaking the cache-dir lock |
| **`TestBuildRegistryAuth`** | **a token-demanding private upstream is served end-to-end; bad/missing credential files fail the build without leaking the lock** |

## 10. Limitations & TODO

- **No per-request credential**: authentication is per upstream, not per client.
  A client-supplied token is forwarded on the **pass-through** path but not used
  for a cached blob fetch, which is inherent to sharing a coalesced fetch and a
  digest-keyed cache between callers (see §5). **Precedence is explicit**: when
  a request carries its own `Authorization`, the transport neither overwrites
  it with a cached operator token nor substitutes the operator credential after
  a 401 — the client's credential reaches the upstream verbatim and its
  rejection is returned as-is.
- **Credentials are plaintext in the file**: `-registry-auth` reads a JSON file;
  protect it with file permissions. There is no integration with a secret store
  or with Docker's own `config.json` / credential helpers.
- **No token pre-fetch**: the first request to a repository still pays one `401`
  before the token is cached.
- **No manifest caching at all**: even immutable digest manifests are proxied.
  Caching those safely (and invalidating tag→digest mappings) is future work.
- **No upstream failover**: a single upstream per mirror; containerd's own
  fallback to `server` covers the outage case.
- **No response-header pass-through on the cached path**: only
  `Docker-Content-Digest` and `Content-Type` are set. Registry-specific headers
  from the upstream blob response are not relayed.
