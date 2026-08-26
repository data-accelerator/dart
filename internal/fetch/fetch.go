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
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Range is the result of a range fetch: the bytes plus the total object size if
// the origin revealed it (from Content-Range or a full response), else -1.
type Range struct {
	Data  []byte
	Total int64
	// RangeIgnored reports that the origin answered a ranged request with 200
	// and a full body, i.e. it does not honor Range. The Data window was sliced
	// out of the stream without buffering the whole object (see Fetch). Total
	// comes from Content-Length and is -1 when the response was chunked.
	//
	// Callers that fetch per-block should treat this as a signal to stop: every
	// further block request to this origin would pull the whole object again.
	RangeIgnored bool
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
	// Userinfo is credential material too: "https://user:pass@host/..." must
	// not render either. (net/http's url.Error strips only the password.)
	if u, err := url.Parse(rawURL); err == nil && u.User != nil {
		u.User = nil
		rawURL = u.String()
	}
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
// body. It accepts 206 (partial) and 200 (full).
//
// A 200 to a ranged request means the origin ignored the Range (or, for an
// object store such as OSS, that the range ran past end-of-file): the window is
// sliced out of the stream without buffering the whole object — bytes before
// start are discarded, at most end-start+1 are read, the rest of the response
// is aborted — and Range.RangeIgnored is set. Any other status is an error.
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
	if start >= 0 && end < start {
		// An inverted window used to degrade silently to a full-object GET —
		// a caller bug becoming a whole-object transfer. Reject it loudly.
		return Range{}, fmt.Errorf("fetch: %s: invalid range %d-%d (end before start)", Redact(url), start, end)
	}
	ranged := start >= 0
	if ranged {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}

	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Range{}, redactTransportErr(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return Range{}, &StatusError{Code: resp.StatusCode, URL: Redact(url), Status: resp.Status}
	}

	if resp.StatusCode == http.StatusOK && ranged {
		return sliceIgnoredRange(resp, url, start, end)
	}

	// A 206 is bounded by the requested window (read one byte past it so an
	// over-long body is caught by the length check below rather than silently
	// truncated); a non-ranged 200 is read in full.
	var bodyReader io.Reader = resp.Body
	if ranged {
		bodyReader = io.LimitReader(resp.Body, end-start+2)
	}
	body, err := io.ReadAll(bodyReader)
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
		// Non-ranged request answered in full.
		total = int64(len(body))
	case http.StatusPartialContent:
		// A 206 is trusted as-is (not re-sliced), so it MUST be exactly the window
		// we asked for. An origin or intermediary that returns a different length,
		// or the right length at the wrong offset, would otherwise have its bytes
		// cached under this block's key — a silent, permanent mismatch (the block
		// cache is write-once per key). Verify both the length and, when the
		// origin states it, the start offset before returning the bytes.
		if ranged {
			if want := end - start + 1; int64(len(body)) != want {
				// RFC 7233 §4.2 lets the origin clamp the end to the object
				// size. Accept a clamped answer when Content-Range proves it:
				// same start, revealed total, end clamped to total-1, and the
				// body length matches the clamped window exactly. This is what
				// makes the unknown-size flow able to fetch the tail block.
				// Anything else stays a hard error (wrong bytes under a
				// write-once key would be a permanent cache mismatch).
				s, sok := startFromContentRange(cr)
				e, eok := endFromContentRange(cr)
				clamped := sok && eok && total > 0 && s == start && e == total-1 && e < end &&
					int64(len(body)) == e-s+1
				if !clamped {
					return Range{}, fmt.Errorf("fetch: %s: 206 returned %d bytes, requested %d", Redact(url), len(body), want)
				}
			}
			if s, ok := startFromContentRange(cr); ok && s != start {
				return Range{}, fmt.Errorf("fetch: %s: 206 Content-Range start %d, requested %d", Redact(url), s, start)
			}
		}
	}
	return Range{Data: body, Total: total}, nil
}

// sliceIgnoredRange extracts the window [start, end] from a 200 full-body
// response without buffering the whole object: skip to the window, read only
// the window, and let Body.Close abort the rest. The total is taken from
// Content-Length (-1 when the response is chunked).
func sliceIgnoredRange(resp *http.Response, url string, start, end int64) (Range, error) {
	if resp.ContentLength == 0 && start == 0 {
		// The window [0,0] of an empty object is the empty window, not an
		// error — some origins answer the size probe this way instead of with
		// a 416. Either response means "this object is empty".
		return Range{Data: []byte{}, Total: 0, RangeIgnored: true}, nil
	}
	if resp.ContentLength >= 0 && start >= resp.ContentLength {
		return Range{}, fmt.Errorf("fetch: %s: range start %d beyond size %d", Redact(url), start, resp.ContentLength)
	}
	if _, err := io.CopyN(io.Discard, resp.Body, start); err != nil {
		// A body that ends before start puts the range beyond the object.
		return Range{}, fmt.Errorf("fetch: %s: range start %d beyond body: %w", Redact(url), start, err)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, end-start+1))
	if err != nil {
		return Range{}, fmt.Errorf("fetch: %s: read body: %w", Redact(url), err)
	}
	return Range{Data: data, Total: resp.ContentLength, RangeIgnored: true}, nil
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
// endFromContentRange parses the inclusive end offset from a Content-Range
// value such as "bytes 0-99/12345"; ok is false on any parse error.
func endFromContentRange(v string) (int64, bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(v), "bytes ")
	if !found {
		return 0, false
	}
	rangePart, _, found := strings.Cut(rest, "/")
	if !found {
		return 0, false
	}
	_, endStr, found := strings.Cut(rangePart, "-")
	if !found {
		return 0, false
	}
	e, err := strconv.ParseInt(strings.TrimSpace(endStr), 10, 64)
	if err != nil {
		return 0, false
	}
	return e, true
}

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
//
// blockIndex must be non-negative and small enough that the whole block window
// [blockIndex*blockSize, blockIndex*blockSize+blockSize-1] is representable in
// int64 (the same bound chunk.Config.MaxBlockIndex states in grid terms).
// Anything else is a caller bug — on the peer relay path it is a malformed
// wire index, since the protocol carries the index as an unrestricted uint64:
// the int64 conversion or the start-offset multiplication would wrap, and a
// wrapped start either recycles to 0 (silently fetching the wrong block) or
// goes negative, where Fetch reads start < 0 as "no Range header" and a
// one-block fetch degrades into a whole-object GET (issue #52). Such geometry
// is rejected here, before any origin I/O.
func FetchBlock(ctx context.Context, f Fetcher, url string, blockSize, blockIndex, size int64) (Range, error) {
	if blockSize <= 0 || blockIndex < 0 || blockIndex > (math.MaxInt64-blockSize+1)/blockSize {
		return Range{}, fmt.Errorf("fetch: %s: block index %d out of range for block size %d", Redact(url), blockIndex, blockSize)
	}
	start := blockIndex * blockSize
	end := start + blockSize - 1
	if size > 0 {
		if start >= size {
			// The block lies wholly past the probed end of the object: a
			// caller bug (bad block index). Error rather than clamp to an
			// inverted range (which Fetch rejects) — fail one frame earlier.
			return Range{}, fmt.Errorf("fetch: %s: block %d starts at %d, past object size %d", Redact(url), blockIndex, start, size)
		}
		if end > size-1 {
			end = size - 1
		}
	}
	return f.Fetch(ctx, url, start, end)
}

// Opener is implemented by fetchers that can open a raw streaming response
// from an origin. It serves callers that proxy a response verbatim instead of
// fetching a block — the fallback for an origin that does not honor Range.
type Opener interface {
	// Open issues a GET for url and returns the live response; the caller
	// reads and closes Body. Any status is returned as-is: the caller is
	// proxying and forwards whatever the origin said.
	Open(ctx context.Context, url string, header http.Header) (*http.Response, error)
}

var _ Opener = (*HTTPFetcher)(nil)

// Open implements Opener. The fetcher's Header is applied first, then the
// per-request header (e.g. the client's Range).
func (f *HTTPFetcher) Open(ctx context.Context, url string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, vs := range f.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, redactTransportErr(err)
	}
	return resp, nil
}

// redactTransportErr rewrites a client.Do failure so the URL it renders cannot
// carry the query (presigned signature) or userinfo: net/http's *url.Error
// strips only the userinfo password, and fmt-wrapping it would still print the
// inner URL in full.
func redactTransportErr(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		redacted := *ue
		redacted.URL = Redact(ue.URL)
		return &redacted
	}
	return err
}

// Coalescing wraps a Fetcher so that concurrent fetches for the same
// (url, start, end) share a single origin request (singleflight).
//
// The shared origin request runs on a background context, so one caller's
// cancellation does not abort it — the block still completes for the other
// waiters (desirable for a cache). Each caller's own ctx only bounds how long
// that caller waits.
//
// A flight keeps exactly one worker goroutine — started by whoever leads it —
// regardless of how many callers join or abandon it: joiners wait inline on
// the flight's done channel, so a caller that stops waiting retains no
// goroutine of its own (issue #53). A cancellation storm on one stalled key
// therefore costs one worker, not one goroutine per abandoned caller.
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
	// MaxFlight bounds how long one shared flight may run before a later call
	// for the same key abandons it and leads a replacement (see group.acquire).
	// The flight's own context also expires at this bound. Zero means
	// DefaultMaxFlight.
	MaxFlight time.Duration
	g         group
}

var _ Fetcher = (*Coalescing)(nil)
var _ Opener = (*Coalescing)(nil)

// identity returns the deduplication identity for url.
func (c *Coalescing) identity(url string) string {
	if c.Key == nil {
		return url
	}
	return c.Key(url)
}

// DefaultMaxFlight bounds how long a shared flight may run before a later call
// for the same key abandons it and leads a replacement. It must be generous:
// a degenerate flight can carry a whole object (a Range-ignoring origin), not
// just a block. The bound exists so a stalled origin connection — accepted,
// then silent — cannot poison a cache key forever: the flight's context
// expires at the bound, and the key's map entry becomes evictable a short
// grace later (evictGrace) even if the leader's fetcher ignores context
// cancellation.
const DefaultMaxFlight = 10 * time.Minute

func (c *Coalescing) maxFlight() time.Duration {
	if c.MaxFlight > 0 {
		return c.MaxFlight
	}
	return DefaultMaxFlight
}

// Fetch coalesces duplicate concurrent fetches. It returns ctx.Err() if the
// caller's context is cancelled before the shared fetch completes — and, once
// it has returned, retains no goroutine: only the flight's single worker
// outlives its callers.
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
	for {
		flight, lead := c.g.acquire(key, c.maxFlight())
		if lead {
			// Exactly one worker goroutine per flight, started by its leader.
			// It deliberately outlives the leading caller: caller cancellation
			// must not abort a flight its peers may still be waiting on, so
			// the worker runs on a background context — bounded by the
			// flight's own deadline, so a stalled origin (accepted, then
			// silent) cannot pin it forever.
			go func() {
				flightCtx, cancel := context.WithDeadline(context.Background(), flight.end)
				defer cancel()
				r, err := c.F.Fetch(flightCtx, url, start, end)
				c.g.finish(key, flight, r, err)
			}()
		}
		// Wait inline rather than on a per-caller goroutine: a caller that
		// stops waiting — cancelled, or still waiting when the flight goes
		// stale — leaves nothing behind. The wait runs to flight.stale, a
		// grace margin past the flight's own deadline, so a ctx-respecting
		// worker's deadline delivery always wins over eviction; the stale
		// path exists only for a worker that ignores its context.
		remaining := time.Until(flight.stale)
		if remaining <= 0 {
			continue // the flight went stale between acquire and the wait
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Range{}, ctx.Err()
		case <-timer.C:
			continue // deadline passed: evict the stale flight or join its replacement
		case <-flight.done:
			timer.Stop()
		}
		r, err := flight.val, flight.err
		r.Coalesced = !lead
		// Only a joiner retries: if we led the flight, the credential that was
		// refused was our own and asking again would change nothing. The retry
		// serves this caller alone, so it uses the caller's context and is
		// skipped entirely when that caller is already gone.
		if !lead && refused(err) && ctx.Err() == nil {
			r, err = c.F.Fetch(ctx, url, start, end)
			r.Coalesced = false // these bytes did cross the network for us
		}
		return r, err
	}
}

// Open implements Opener by delegating to the inner fetcher. Passthrough
// traffic is deliberately not coalesced: nothing from it is cached, so sharing
// a flight would only couple unrelated clients' failure modes.
func (c *Coalescing) Open(ctx context.Context, url string, header http.Header) (*http.Response, error) {
	if o, ok := c.F.(Opener); ok {
		return o.Open(ctx, url, header)
	}
	return nil, errors.New("fetch: inner fetcher does not support streaming open")
}

// refused reports whether err is an origin authorization refusal, i.e. the
// credential in the request was rejected rather than anything being broken.
func refused(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Refused()
}

// group is a minimal singleflight keyed by string. One leader at a time owns
// a key's flight and runs the shared work on the flight's single worker
// goroutine; every other caller waits inline on the flight's done channel, so
// waiters hold no per-caller state beyond their own stack.
type group struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	done  chan struct{} // closed when the flight completes
	val   Range
	err   error
	end   time.Time // the flight context's deadline
	stale time.Time // after this, a caller evicts the flight and leads a replacement
}

// evictGrace is the margin between a flight's own deadline and the moment it
// becomes evictable. A ctx-respecting worker delivers its result right at the
// deadline; the margin guarantees that delivery lands before any waiter
// declares the flight dead, so eviction only ever fires on a worker that
// ignores its context.
func evictGrace(maxFlight time.Duration) time.Duration {
	g := maxFlight / 8
	if g < 25*time.Millisecond {
		g = 25 * time.Millisecond
	}
	if g > 30*time.Second {
		g = 30 * time.Second
	}
	return g
}

// acquire returns the in-flight call for key, or — when none is flying or the
// existing one has gone stale — creates a new one and reports lead=true. A
// caller that leads must run the shared work for the flight and then call
// finish with the returned call, exactly once. maxFlight must be positive
// (Coalescing.maxFlight guarantees this).
func (g *group) acquire(key string, maxFlight time.Duration) (c *call, lead bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		if time.Now().Before(c.stale) {
			return c, false
		}
		delete(g.m, key) // stale flight: abandon it and lead a replacement
	}
	end := time.Now().Add(maxFlight)
	c = &call{done: make(chan struct{}), end: end, stale: end.Add(evictGrace(maxFlight))}
	g.m[key] = c
	return c, true
}

// finish publishes a led flight's result and wakes its waiters: the writes
// happen-before the close of done, hence before any waiter's read. A
// late-finishing stale leader deletes only its own entry, never the
// replacement's.
func (g *group) finish(key string, c *call, v Range, err error) {
	c.val, c.err = v, err
	close(c.done)
	g.mu.Lock()
	if g.m[key] == c {
		delete(g.m, key)
	}
	g.mu.Unlock()
}
