// Package fetch retrieves object byte ranges from origin over HTTP and
// coalesces duplicate concurrent fetches (singleflight).
//
// It is the read-through path behind a cache miss: the caller maps a request to
// blocks (internal/chunk), and for each missing block calls a Fetcher to pull
// the bytes from origin, then stores them (internal/store). This package is
// decoupled from store/chunk so it can be tested in isolation with httptest.
//
// Readahead and the miss -> fetch -> store wiring live in the higher serve/
// engine layer, which knows the access pattern; see docs/fetch.md §8.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Range is the result of a range fetch: the bytes plus the total object size if
// the origin revealed it (from Content-Range or a full response), else -1.
type Range struct {
	Data  []byte
	Total int64
	// Coalesced reports that these bytes came from a fetch another caller had
	// already started, so nothing crossed the network on this caller's behalf.
	//
	// It exists so byte accounting can stay honest. Without it, N callers that
	// coalesce onto one fetch each see a full Range and would each add its length to
	// a "bytes transferred" counter, reporting N times the traffic that actually
	// occurred — which is precisely backwards, since coalescing is what *avoided* the
	// traffic. Hit-ratio counters should still count every read; only wire-byte
	// counters need to skip a coalesced one.
	Coalesced bool
}

// Redact returns rawURL without its query string, for embedding in errors, logs
// and metrics.
//
// This is not cosmetic. An upstream may be a **presigned** object-storage URL
// whose query carries the signature, and that signature is a time-limited
// credential: anyone who sees it can read the object. Upstream URLs therefore
// must never appear in output with their query intact.
func Redact(rawURL string) string {
	if i := strings.IndexByte(rawURL, '?'); i >= 0 {
		return rawURL[:i] + "?<redacted>"
	}
	return rawURL
}

// StatusError reports an unexpected HTTP status from an origin. It is a distinct
// type so callers can tell an upstream *refusal* (401/403 from an expired or
// wrong credential) from a transport failure — a distinction the relay path needs
// in order to avoid blaming a healthy peer for a client's expired signature.
type StatusError struct {
	// Code is the origin's HTTP status code.
	Code int
	// URL is the origin URL with its query redacted.
	URL string
	// Status is the origin's status line.
	Status string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("fetch: %s: unexpected status %s", e.URL, e.Status)
}

// Refused reports whether the origin rejected the request on authorization
// grounds, which usually means the caller's credential is wrong or expired
// rather than anything being broken.
func (e *StatusError) Refused() bool {
	return e.Code == http.StatusUnauthorized || e.Code == http.StatusForbidden
}

// Fetcher retrieves an inclusive byte range [start, end] from an origin URL. A
// negative start requests the whole object (no Range header).
//
// Implementations MUST use GET, never HEAD, even to probe for a size. An upstream
// may be a presigned object-storage URL, and both S3 and Aliyun OSS include the
// HTTP verb in the signature: such a URL is signed for GET alone, so a HEAD fails
// signature verification and returns 403. Ask for bytes=0-0 instead and read the
// total from Content-Range — "HEAD is cheaper than fetching one byte" is a natural
// but breaking optimization. The Range header itself is not signed, so arbitrary
// block-aligned ranges are fine.
type Fetcher interface {
	Fetch(ctx context.Context, url string, start, end int64) (Range, error)
}

// HTTPFetcher fetches byte ranges over HTTP/HTTPS.
type HTTPFetcher struct {
	// Client is the HTTP client; nil uses http.DefaultClient.
	Client *http.Client
	// Header, if set, is applied to every request (e.g. Authorization). It is
	// read-only and shared; do not mutate concurrently with Fetch.
	Header http.Header
}

var _ Fetcher = (*HTTPFetcher)(nil)

// Fetch issues a GET with a Range header (unless start < 0) and returns the
// body. It accepts 206 (partial) and 200 (full). If the origin ignores the
// Range and returns 200, the requested sub-range is sliced out of the full
// body. Any other status is an error.
func (f *HTTPFetcher) Fetch(ctx context.Context, url string, start, end int64) (Range, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Range{}, err
	}
	for k, vs := range f.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	ranged := start >= 0 && end >= start
	if ranged {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}

	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Range{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return Range{}, &StatusError{Code: resp.StatusCode, URL: Redact(url), Status: resp.Status}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Range{}, fmt.Errorf("fetch: %s: read body: %w", Redact(url), err)
	}

	total := int64(-1)
	cr := resp.Header.Get("Content-Range")
	if cr != "" {
		total = totalFromContentRange(cr)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		// The origin ignored the Range (or the range covered the whole object)
		// and returned the full body. This is exactly what an object store such
		// as OSS does when the requested range runs past end-of-file, so it is a
		// normal case, not an error: slice out the requested window ourselves.
		total = int64(len(body))
		if ranged {
			if start >= total {
				return Range{}, fmt.Errorf("fetch: %s: range start %d beyond size %d", Redact(url), start, total)
			}
			e := end
			if e >= total {
				e = total - 1
			}
			body = body[start : e+1]
		}
	case http.StatusPartialContent:
		// A 206 is trusted as-is (not re-sliced), so it MUST be exactly the window
		// we asked for. An origin or intermediary that returns a different length,
		// or the right length at the wrong offset, would otherwise have its bytes
		// cached under this block's key — a silent, permanent mismatch (the block
		// cache is write-once per key). Verify both the length and, when the
		// origin states it, the start offset before returning the bytes.
		if ranged {
			if want := end - start + 1; int64(len(body)) != want {
				return Range{}, fmt.Errorf("fetch: %s: 206 returned %d bytes, requested %d", Redact(url), len(body), want)
			}
			if s, ok := startFromContentRange(cr); ok && s != start {
				return Range{}, fmt.Errorf("fetch: %s: 206 Content-Range start %d, requested %d", Redact(url), s, start)
			}
		}
	}
	return Range{Data: body, Total: total}, nil
}

// totalFromContentRange parses the total size from a Content-Range value such
// as "bytes 0-99/12345"; returns -1 for "*" or on any parse error.
func totalFromContentRange(v string) int64 {
	i := strings.LastIndexByte(v, '/')
	if i < 0 {
		return -1
	}
	t := strings.TrimSpace(v[i+1:])
	if t == "*" {
		return -1
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// startFromContentRange parses the start offset from a Content-Range value such
// as "bytes 0-99/12345"; ok is false on any parse error (including a header with
// no byte range, e.g. "bytes */12345").
func startFromContentRange(v string) (int64, bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(v), "bytes ")
	if !found {
		return 0, false
	}
	rangePart, _, found := strings.Cut(rest, "/")
	if !found {
		return 0, false
	}
	startStr, _, found := strings.Cut(rangePart, "-")
	if !found {
		return 0, false
	}
	s, err := strconv.ParseInt(strings.TrimSpace(startStr), 10, 64)
	if err != nil {
		return 0, false
	}
	return s, true
}

// FetchBlock fetches block blockIndex (of blockSize bytes) from url. When size
// > 0 the final (tail) block's end is clamped to size-1; when size <= 0 the
// natural block range is requested and the returned Range.Total may reveal the
// size.
func FetchBlock(ctx context.Context, f Fetcher, url string, blockSize, blockIndex, size int64) (Range, error) {
	start := blockIndex * blockSize
	end := start + blockSize - 1
	if size > 0 && end > size-1 {
		end = size - 1
	}
	return f.Fetch(ctx, url, start, end)
}

// Coalescing wraps a Fetcher so that concurrent fetches for the same
// (url, start, end) share a single origin request (singleflight).
//
// The shared origin request runs on a background context, so one caller's
// cancellation does not abort it — the block still completes for the other
// waiters (desirable for a cache). Each caller's own ctx only bounds how long
// that caller waits.
type Coalescing struct {
	F Fetcher
	// Key maps an origin URL to the identity used for deduplication. Nil uses the
	// URL itself.
	//
	// Setting this is important whenever an upstream URL is not a stable name for
	// its content. A **presigned** object-storage URL is signed afresh on every
	// redirect, so N clients asking for the same block arrive with N distinct URLs:
	// keyed by URL they would not coalesce at all and each would open its own
	// origin fetch — precisely the thundering herd this type exists to prevent.
	// Callers should set it to the same content identity used for cache keys
	// (chunk.ObjectID).
	Key func(url string) string
	g   group
}

var _ Fetcher = (*Coalescing)(nil)

// identity returns the deduplication identity for url.
func (c *Coalescing) identity(url string) string {
	if c.Key == nil {
		return url
	}
	return c.Key(url)
}

// Fetch coalesces duplicate concurrent fetches. It returns ctx.Err() if the
// caller's context is cancelled before the shared fetch completes.
//
// If the shared fetch was refused on authorization grounds (401/403) and this
// caller merely joined it, the caller retries alone with its *own* URL. That
// matters because Key deliberately ignores the query string, so callers holding
// different presigned credentials for the same content share one flight, and the
// credential actually sent is whichever caller happened to lead it. Without the
// retry, one node's expired signature would fail the reads of every node that
// joined behind it, even those holding a perfectly valid one. Retrying only on a
// refusal keeps the thundering-herd protection intact on every normal path.
func (c *Coalescing) Fetch(ctx context.Context, url string, start, end int64) (Range, error) {
	key := c.identity(url) + "\x00" + strconv.FormatInt(start, 10) + ":" + strconv.FormatInt(end, 10)
	type result struct {
		r   Range
		err error
	}
	ch := make(chan result, 1)
	go func() {
		r, err, shared := c.g.Do(key, func() (Range, error) {
			return c.F.Fetch(context.Background(), url, start, end)
		})
		r.Coalesced = shared
		// Only a joiner retries: if we led the flight, the credential that was
		// refused was our own and asking again would change nothing.
		if shared && refused(err) {
			r, err = c.F.Fetch(context.Background(), url, start, end)
			r.Coalesced = false // these bytes did cross the network for us
		}
		ch <- result{r, err}
	}()
	select {
	case <-ctx.Done():
		return Range{}, ctx.Err()
	case res := <-ch:
		return res.r, res.err
	}
}

// refused reports whether err is an origin authorization refusal, i.e. the
// credential in the request was rejected rather than anything being broken.
func refused(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Refused()
}

// group is a minimal singleflight: concurrent Do calls with the same key share
// one execution of fn; all callers receive its result.
type group struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val Range
	err error
}

// Do executes fn once per in-flight key. shared reports whether the result was
// shared with a concurrent caller.
func (g *group) Do(key string, fn func() (Range, error)) (v Range, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err, false
}
