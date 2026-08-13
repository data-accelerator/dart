# `internal/fetch`

Origin read-through: fetch object byte ranges over HTTP and coalesce duplicate
concurrent fetches (singleflight).

- Source: `internal/fetch/fetch.go`
- Tests: `internal/fetch/fetch_test.go`
- Import path: `github.com/data-accelerator/dart/internal/fetch`

## 1. Overview

On a cache miss, the serve layer maps the request to blocks (`internal/chunk`)
and, for each missing block, pulls the bytes from origin via a `Fetcher`, then
stores them (`internal/store`). This package provides the origin HTTP fetch and
a singleflight wrapper so a thundering herd for the same block hits origin once.

It is intentionally decoupled from `store`/`chunk` (it takes a URL and a byte
range) so it can be tested in isolation with `httptest`.

## 2. Concepts

| Term | Meaning |
|---|---|
| `Range` | A fetch result: `Data` bytes + `Total` object size (or -1 if unknown). |
| `Fetcher` | Retrieves an inclusive `[start,end]` byte range from a URL. |
| singleflight | Concurrent identical fetches share one origin request. |

## 3. Public API

### 3.1 `type Range` / `type Fetcher`

```go
type Range struct { Data []byte; Total int64; RangeIgnored bool; Coalesced bool }
type Fetcher interface {
    Fetch(ctx context.Context, url string, start, end int64) (Range, error)
}
```

A negative `start` requests the whole object (no `Range` header). `Total` is the
full object size when the origin reveals it (from `Content-Range`, or
`Content-Length` on a `200`), else -1. `RangeIgnored` is set when a ranged
request was answered with a `200` full body — the origin does not honor Range,
and every further per-block fetch to it would pull the whole object again (the
engine uses this to switch to verbatim proxying; see docs/engine.md §3.9).

### 3.2 `type HTTPFetcher`

```go
type HTTPFetcher struct {
    Client *http.Client // nil => http.DefaultClient
    Header http.Header  // applied to every request (e.g. Authorization); read-only
}
```

`Fetch` behavior:

- Sends `Range: bytes=start-end` unless `start < 0`.
- Accepts `206 Partial Content` and `200 OK`.
- On a **`206`** the body is trusted as-is, so it is validated first: the body
  length must equal the requested window (`end-start+1`) and, when the origin
  states a `Content-Range`, its start offset must equal the requested start. A
  mismatch is an **error** — an origin or intermediary that returns a short body
  or the wrong offset would otherwise have its bytes cached under the block's
  key, and the block cache is write-once per key, so that corruption would be
  permanent. `Content-Range` is also parsed for `Total`.
- If the origin **ignores** the Range and returns `200`, the window is sliced
  out of the stream **without buffering the whole object**: bytes before `start`
  are discarded, at most `end-start+1` are read, and the rest of the response is
  aborted. `Range.RangeIgnored` is set, and `Total` comes from `Content-Length`
  (-1 on a chunked response). This is also what an object store such as OSS
  returns when the requested range runs past end-of-file, so it is a normal
  path, not an error.
- Any other status is an error; a range `start` beyond the object size is an
  error.

### 3.3 `type Opener` / `HTTPFetcher.Open`

```go
type Opener interface {
    Open(ctx context.Context, url string, header http.Header) (*http.Response, error)
}
```

`Open` issues a GET and returns the **live** response for the caller to stream
and close; the fetcher's `Header` is applied first, then the per-request
`header` (e.g. the client's `Range`). Any status is returned as-is — the caller
is proxying verbatim. This backs the engine's passthrough fallback for
Range-ignoring origins. Time complexity is O(1); no body bytes are buffered.

### 3.4 `func FetchBlock(ctx, f Fetcher, url string, blockSize, blockIndex, size int64) (Range, error)`

Fetches one block: `start = blockIndex*blockSize`, `end = start+blockSize-1`.
When `size > 0`, the tail block's end is clamped to `size-1`; when `size <= 0`
(unknown), the natural block range is requested and `Range.Total` may reveal the
size.

### 3.5 `type Coalescing`

```go
type Coalescing struct { F Fetcher /* ... */ }
func (c *Coalescing) Fetch(ctx, url string, start, end int64) (Range, error)
func (c *Coalescing) Open(ctx, url string, header http.Header) (*http.Response, error)
```

Wraps a `Fetcher` with singleflight keyed by `(url, start, end)`: concurrent
identical fetches share one call to `F`. The **shared origin request runs on a
background context**, so one caller's cancellation does not abort it — the block
still completes for the other waiters (desirable for a cache). Each caller's own
`ctx` only bounds how long that caller waits (a cancelled caller returns
`ctx.Err()`).

`Open` implements `Opener` by delegating to the inner fetcher (erroring when it
cannot stream). Passthrough traffic is deliberately **not** coalesced: nothing
from it is cached, so sharing a flight would only couple unrelated clients'
failure modes.

```go
c := &fetch.Coalescing{F: &fetch.HTTPFetcher{Header: authHeader}}
r, err := fetch.FetchBlock(ctx, c, originURL, blockSize, blockIdx, size)
// then: store.Put(blockKey, r.Data)
```

### 3.6 Presigned upstreams: `Coalescing.Key`, `Redact`, `StatusError`

```go
type Coalescing struct {
    F   Fetcher
    Key func(url string) string   // dedup identity; nil = the URL itself
}
func Redact(rawURL string) string          // drops the query
type StatusError struct{ Code int; URL, Status string }
func (e *StatusError) Refused() bool       // 401/403
```

An upstream is often a **presigned** object-storage URL (a registry's backing
bucket), and that changes three things:

- **`Key` is required for coalescing to work.** The signature is re-issued on
  every redirect, so concurrent clients arrive with *different* URLs for the same
  block. Keyed by URL they would not coalesce at all and each would open its own
  origin fetch — exactly the thundering herd `Coalescing` exists to prevent. Set
  `Key` to the same content identity used for cache keys (`chunk.ObjectID`).
- **The signature is a credential.** `Redact` strips the query, and every error
  built here uses it, because these strings reach logs and HTTP responses.
- **A refusal is not a failure of ours.** `StatusError.Refused()` marks 401/403,
  which lets the relay path report "the origin rejected the caller's credential"
  instead of looking like the relay malfunctioned (see docs/peer.md §3.5).
- **A joiner retries with its own credential after a refusal.** Because `Key`
  ignores the query, callers holding *different* signatures for the same content
  share one flight — and the credential actually sent is whichever caller happened
  to lead it. In a P2P cluster every node gets its own presigned URL with its own
  expiry, so a node holding a stale signature would otherwise fail the reads of
  every node queued behind it, including nodes holding a perfectly valid one. So a
  caller that merely *joined* a flight which came back refused re-fetches alone
  with its own URL. The leader does not retry (its own credential was the one
  rejected), and a non-refusal error such as a 500 is still shared, because
  fanning that out into per-caller retries would pile load onto an origin that is
  already unwell.

  The converse is deliberately *not* addressed: a caller with an expired signature
  that joins a flight led by a valid one is served. Blocking that would buy
  nothing, since a cached block is served without consulting any signature at all
  (see §4.5 and SECURITY.md).

## 4. Invariants & Guarantees

1. **Correct range bytes**: on 206 the body is the requested range; on a
   range-ignored 200 the window is sliced out of the stream (bytes before
   `start` discarded, at most `end-start+1` read) — the full body is never
   buffered.
2. **Coalescing**: N concurrent `Coalescing.Fetch` for the same key ⇒ exactly 1
   call to the underlying `Fetcher`; all callers receive the same result. The one
   exception is an authorization refusal (401/403), where each joiner re-fetches
   once with its own credential (§3.6).
3. **Cancellation isolation**: a caller cancelling its `ctx` neither aborts the
   shared fetch nor affects other callers.
4. **Total discovery**: `Total` reflects the object size whenever the origin
   provides it (Content-Range, or Content-Length on a 200).
5. **A credential authorizes a fetch, not a cache hit.** This package is only
   consulted on a miss, so a signature is verified by the origin exactly once per
   block — the first time it is fetched. Afterwards the block is served from cache
   to any caller, including one presenting an expired signature or none at all.
   That is inherent to a cache and is part of DART's trust model, not a defect;
   see SECURITY.md.

## 5. Concurrency & Call Permissions

- `HTTPFetcher` is safe for concurrent use if `Header`/`Client` are not mutated
  after construction.
- `Coalescing` is safe for concurrent use; its internal singleflight group is
  mutex-guarded.
- `Range.Data` is a freshly read buffer owned by the caller. Note that
  coalesced callers share the **same** `Range.Data` slice; treat it as
  read-only (the store copies on Put, and Get returns copies).

## 6. Determinism / Stability Contract

- Nothing here is on the DART wire protocol; HTTP semantics (Range/Content-Range)
  are the external contract with origins. No cross-node format to keep stable.

## 7. Testing

- **Results**: `go vet` clean; `go test` all pass; `go test -race` clean.
- **Coverage**: **93.1%** of statements. The remaining uncovered lines are rare
  I/O-error branches (e.g. a mid-body read error) that need fault injection.
- **Reproduce**:

```bash
# cache dirs per docs/README.md
go test ./internal/fetch/ -v -count=1
go test ./internal/fetch/ -race -count=1
go test ./internal/fetch/ -cover -count=1
```

### Test list (property each guards)

| Test | Property guarded |
|---|---|
| `TestHTTPFetcherRange` | 206 range returns exact bytes + Total |
| `TestHTTPFetcherFullObject` | negative start fetches the whole object |
| `TestHTTPFetcherRangeIgnored` | 200 (Range ignored) sliced to requested range |
| `TestHTTPFetcherRangeIgnoredMarked` | 200 to a ranged request sets `RangeIgnored`; Total from Content-Length |
| `TestHTTPFetcherRangeIgnoredChunked` | chunked 200 ⇒ Total -1, window still sliced |
| `TestHTTPFetcherRangeIgnoredBoundedRead` | an unbounded 200 body is not read in full (window only, then abort) |
| `TestOpenPassthrough` | Open forwards Range, preserves status/ETag, streams the body; 404 as-is |
| `TestCoalescingOpen` | Open delegates to the inner fetcher; clear error when it cannot stream |
| `TestHTTPFetcherStatusError` | non-2xx is an error |
| `TestHTTPFetcherConnError` | transport error surfaces |
| `TestHTTPFetcherRangeBeyondSize` | range start past object size is an error |
| `TestHTTPFetcherHeaderApplied` | configured headers (auth) are sent |
| `TestFetchBlockClampsTail` | tail block clamped to size-1 |
| `TestFetchBlockUnknownSize` | unknown size fetches natural range; Total revealed |
| `TestTotalFromContentRange` | Content-Range total parsing incl. `*`/garbage |
| `TestCoalescingDedups` | N concurrent identical fetches ⇒ 1 origin call (`-race`) |
| `TestCoalescingDistinctKeys` | different url/range ⇒ separate calls |
| **`TestCoalescingKeyCollapsesPresignedURLs`** | **20 concurrent callers with distinct signatures for one block cause 1 origin fetch** |
| **`TestCoalescingJoinerRetriesAfterRefusal`** | **a joiner holding a valid signature is not failed by the leader's expired one: it retries with its own credential** |
| `TestCoalescingLeaderDoesNotRetry` | the refused leader does not resend its own rejected credential |
| `TestCoalescingNonRefusalDoesNotRetry` | a shared 500 is not fanned out into per-caller retries |
| `TestCoalescingWithoutKeyUsesURL` | the default keys by URL, so distinct signatures do not coalesce |
| `TestCoalescingKeyStillSeparatesRanges` | collapsing identity must not collapse distinct byte ranges |
| `TestRedactStripsSignature` / `TestFetchErrorsRedactSignature` | the query is dropped, and error strings never carry a signature |
| `TestStatusErrorRefusedClassification` | 401/403 are refusals; 404/500/502 are not |
| `TestCoalescingCallerCancel` | caller cancel returns ctx.Err(); shared fetch still completes |

## 8. Limitations & TODO

- **Readahead**: prefetching subsequent blocks belongs to the serve/engine layer
  (it knows the access pattern); not implemented here.
- **miss → fetch → store wiring**: the orchestration that combines
  `chunk`+`hashring`+`store`+peer+this package is a future `engine`/serve layer.
- **Range-ignoring origins**: handled at two levels. `Fetch` bounds a 200
  response to the requested window (no full-body buffering) and marks it via
  `RangeIgnored`; the engine uses the mark to stop per-block fetches entirely
  and proxy such objects verbatim (see docs/engine.md §3.9).
- **`Range.Coalesced`**: set when a caller rode along on a fetch another caller had
  already started, so no bytes crossed the network on its behalf. Wire-byte metrics
  must skip such a result; hit-ratio counters should still count the read. Without
  this, N coalesced callers each add a full length to a "bytes transferred" counter
  and report N times the traffic that occurred.
- **GET only, never HEAD**: an upstream may be a presigned object-storage URL, and
  both S3 and Aliyun OSS put the HTTP verb inside the signature, so such a URL is
  signed for GET alone and a HEAD returns 403. Size probes therefore ask for
  `bytes=0-0` and read the total from `Content-Range`. The `Range` header is *not*
  signed, so arbitrary block-aligned ranges are fine. `TestNeverSendsHeadUpstream`
  fails if this is ever "optimized" into a HEAD.
- **Authz caching**: token authorization (design §10) is a policy-layer
  concern; this package only applies static `Header`.
