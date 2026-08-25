package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blob is deterministic test content.
func blob(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// rangeServer serves content with proper Range support (206 + Content-Range).
func rangeServer(t *testing.T, content []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "blob", time.Unix(0, 0), bytes.NewReader(content))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPFetcherRange(t *testing.T) {
	content := blob(10000)
	srv := rangeServer(t, content)
	f := &HTTPFetcher{}
	got, err := f.Fetch(context.Background(), srv.URL, 100, 199)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got.Data, content[100:200]) {
		t.Errorf("range bytes mismatch")
	}
	if got.Total != int64(len(content)) {
		t.Errorf("Total = %d, want %d", got.Total, len(content))
	}
}

func TestHTTPFetcherFullObject(t *testing.T) {
	content := blob(500)
	srv := rangeServer(t, content)
	f := &HTTPFetcher{}
	got, err := f.Fetch(context.Background(), srv.URL, -1, -1) // whole object
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got.Data, content) || got.Total != 500 {
		t.Errorf("full object: len=%d total=%d", len(got.Data), got.Total)
	}
}

// TestHTTPFetcherRangeIgnored: a server that returns 200 with the full body,
// ignoring Range; the fetcher must slice out the requested sub-range.
func TestHTTPFetcherRangeIgnored(t *testing.T) {
	content := blob(1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	}))
	defer srv.Close()
	f := &HTTPFetcher{}
	got, err := f.Fetch(context.Background(), srv.URL, 200, 299)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got.Data, content[200:300]) {
		t.Errorf("range not sliced from 200 response")
	}
	if got.Total != 1000 {
		t.Errorf("Total = %d, want 1000", got.Total)
	}
}

// TestHTTPFetcher206ShortBody: a 206 whose body is shorter than the requested
// window must be rejected, not returned for caching under the block's key. A
// range-clamping proxy can produce exactly this, and the block cache is
// write-once per key, so a cached short block would be a permanent error.
func TestHTTPFetcher206ShortBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 100-199/10000")
		w.WriteHeader(http.StatusPartialContent)
		w.Write(blob(50)) // 100 bytes requested, 50 delivered
	}))
	defer srv.Close()
	f := &HTTPFetcher{}
	if _, err := f.Fetch(context.Background(), srv.URL, 100, 199); err == nil {
		t.Error("expected error on a short 206 body")
	}
}

// TestHTTPFetcher206WrongOffset: a 206 that returns the right length but at a
// different offset than requested must be rejected — caching it would map one
// block's bytes onto another block's key.
func TestHTTPFetcher206WrongOffset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-99/10000") // wrong start
		w.WriteHeader(http.StatusPartialContent)
		w.Write(blob(100))
	}))
	defer srv.Close()
	f := &HTTPFetcher{}
	if _, err := f.Fetch(context.Background(), srv.URL, 100, 199); err == nil {
		t.Error("expected error on a 206 with a mismatched Content-Range start")
	}
}

func TestStartFromContentRange(t *testing.T) {
	cases := []struct {
		in     string
		want   int64
		wantOK bool
	}{
		{"bytes 100-199/10000", 100, true},
		{"bytes 0-0/1", 0, true},
		{"bytes */10000", 0, false},
		{"100-199/10000", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := startFromContentRange(c.in)
		if ok != c.wantOK || (ok && got != c.want) {
			t.Errorf("startFromContentRange(%q) = %d,%v want %d,%v", c.in, got, ok, c.want, c.wantOK)
		}
	}
}

func TestHTTPFetcherStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()
	f := &HTTPFetcher{}
	if _, err := f.Fetch(context.Background(), srv.URL, 0, 10); err == nil {
		t.Error("expected error on 403")
	}
}

func TestHTTPFetcherConnError(t *testing.T) {
	f := &HTTPFetcher{}
	// Port 1 is (almost certainly) closed -> transport error.
	if _, err := f.Fetch(context.Background(), "http://127.0.0.1:1/x", 0, 10); err == nil {
		t.Error("expected a connection error")
	}
}

func TestHTTPFetcherRangeBeyondSize(t *testing.T) {
	content := blob(10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // ignores Range, full body of 10 bytes
		w.Write(content)
	}))
	defer srv.Close()
	f := &HTTPFetcher{}
	if _, err := f.Fetch(context.Background(), srv.URL, 20, 30); err == nil {
		t.Error("expected error when range start is beyond object size")
	}
}

func TestHTTPFetcherHeaderApplied(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Write(blob(10))
	}))
	defer srv.Close()
	f := &HTTPFetcher{Header: http.Header{"Authorization": {"Bearer xyz"}}}
	if _, err := f.Fetch(context.Background(), srv.URL, -1, -1); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotAuth.Load() != "Bearer xyz" {
		t.Errorf("Authorization header not applied: %v", gotAuth.Load())
	}
}

func TestFetchBlockClampsTail(t *testing.T) {
	content := blob(4500) // blockSize 4096 => block 0 full, block 1 tail of 404
	srv := rangeServer(t, content)
	f := &HTTPFetcher{}
	got, err := FetchBlock(context.Background(), f, srv.URL, 4096, 1, 4500)
	if err != nil {
		t.Fatalf("FetchBlock: %v", err)
	}
	if !bytes.Equal(got.Data, content[4096:4500]) {
		t.Errorf("tail block bytes mismatch: got %d bytes", len(got.Data))
	}
}

func TestFetchBlockUnknownSize(t *testing.T) {
	content := blob(5000)
	srv := rangeServer(t, content)
	f := &HTTPFetcher{}
	// size unknown (-1): request the natural block range; Total reveals size.
	got, err := FetchBlock(context.Background(), f, srv.URL, 4096, 0, -1)
	if err != nil {
		t.Fatalf("FetchBlock: %v", err)
	}
	if !bytes.Equal(got.Data, content[0:4096]) {
		t.Errorf("block 0 bytes mismatch")
	}
	if got.Total != 5000 {
		t.Errorf("Total = %d, want 5000", got.Total)
	}
}

func TestTotalFromContentRange(t *testing.T) {
	cases := map[string]int64{
		"bytes 0-99/12345": 12345,
		"bytes 0-99/*":     -1,
		"garbage":          -1,
		"bytes 0-99/":      -1,
	}
	for in, want := range cases {
		if got := totalFromContentRange(in); got != want {
			t.Errorf("totalFromContentRange(%q) = %d, want %d", in, got, want)
		}
	}
}

// gateFetcher blocks each call on a channel and counts total calls, to prove
// Coalescing collapses concurrent fetches into one.
type gateFetcher struct {
	calls  int64
	gate   chan struct{}
	result []byte
}

func (g *gateFetcher) Fetch(ctx context.Context, url string, start, end int64) (Range, error) {
	atomic.AddInt64(&g.calls, 1)
	<-g.gate // block until released
	return Range{Data: g.result, Total: int64(len(g.result))}, nil
}

func TestCoalescingDedups(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(64)}
	c := &Coalescing{F: gf}

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	datas := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Fetch(context.Background(), "u", 0, 63)
			errs[i], datas[i] = err, r.Data
		}(i)
	}
	time.Sleep(20 * time.Millisecond) // let callers reach the singleflight
	close(gf.gate)
	wg.Wait()

	if got := atomic.LoadInt64(&gf.calls); got != 1 {
		t.Errorf("underlying calls = %d, want 1 (coalesced)", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil || !bytes.Equal(datas[i], gf.result) {
			t.Errorf("caller %d: err=%v data-ok=%v", i, errs[i], bytes.Equal(datas[i], gf.result))
		}
	}
}

// TestCoalescingKeyCollapsesPresignedURLs is the fix for a real thundering-herd
// hole: a presigned upstream is signed afresh on every redirect, so concurrent
// clients arrive with *different* URLs for the same block. Keyed by URL they
// would not coalesce, and each would open its own origin fetch.
func TestCoalescingKeyCollapsesPresignedURLs(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(32)}
	// Identity = the URL without its query, which is where the signature lives.
	c := &Coalescing{F: gf, Key: func(u string) string {
		if i := strings.IndexByte(u, '?'); i >= 0 {
			return u[:i]
		}
		return u
	}}

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Same object, a distinct signature each time.
			url := fmt.Sprintf("https://bkt/obj?Expires=%d&Signature=sig%d", i, i)
			if _, err := c.Fetch(context.Background(), url, 0, 31); err != nil {
				t.Errorf("Fetch: %v", err)
			}
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(gf.gate)
	wg.Wait()

	if got := atomic.LoadInt64(&gf.calls); got != 1 {
		t.Errorf("origin fetched %d times for %d concurrent callers of one block, want 1", got, n)
	}
}

// credentialFetcher models an origin behind presigned URLs: it refuses any URL
// whose query marks it expired, and gates the first call so callers pile up
// behind one in-flight fetch.
type credentialFetcher struct {
	gate    chan struct{}
	result  []byte
	calls   int64
	refused int64

	mu   sync.Mutex
	seen []string // URLs actually sent to the origin, in order
}

func (f *credentialFetcher) Fetch(_ context.Context, url string, _, _ int64) (Range, error) {
	atomic.AddInt64(&f.calls, 1)
	f.mu.Lock()
	f.seen = append(f.seen, url)
	f.mu.Unlock()
	if f.gate != nil {
		<-f.gate
	}
	if strings.Contains(url, "expired") {
		atomic.AddInt64(&f.refused, 1)
		return Range{}, &StatusError{Code: http.StatusForbidden, URL: Redact(url), Status: "403 Forbidden"}
	}
	return Range{Data: f.result, Total: int64(len(f.result))}, nil
}

// objectIdentity keys on the URL without its query, i.e. exactly what cmd/dart
// does: the signature lives in the query and must not split the cache identity.
func objectIdentity(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	return u
}

// TestCoalescingJoinerRetriesAfterRefusal is the P2P scenario that motivates the
// retry: nodes hold *different* presigned credentials for the same object, so they
// coalesce onto one flight whose leader may be holding an expired one. A caller
// with a valid credential must not be failed by someone else's stale signature.
func TestCoalescingJoinerRetriesAfterRefusal(t *testing.T) {
	cf := &credentialFetcher{gate: make(chan struct{}), result: blob(48)}
	c := &Coalescing{F: cf, Key: objectIdentity}

	// The leader carries the expired signature.
	leaderErr := make(chan error, 1)
	go func() {
		_, err := c.Fetch(context.Background(), "https://bkt/obj?sig=expired", 0, 47)
		leaderErr <- err
	}()
	// Wait until it is genuinely in flight, so the rest are joiners.
	for atomic.LoadInt64(&cf.calls) == 0 {
		time.Sleep(time.Millisecond)
	}

	const joiners = 8
	var wg sync.WaitGroup
	errs := make([]error, joiners)
	datas := make([][]byte, joiners)
	for i := 0; i < joiners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			url := fmt.Sprintf("https://bkt/obj?sig=valid%d", i)
			r, err := c.Fetch(context.Background(), url, 0, 47)
			errs[i], datas[i] = err, r.Data
		}(i)
	}
	time.Sleep(20 * time.Millisecond) // let them all join the flight
	close(cf.gate)
	wg.Wait()

	if err := <-leaderErr; !refused(err) {
		t.Errorf("leader with the expired signature: err = %v, want a 403 refusal", err)
	}
	for i := 0; i < joiners; i++ {
		if errs[i] != nil {
			t.Errorf("joiner %d holding a valid signature failed: %v", i, errs[i])
			continue
		}
		if len(datas[i]) != 48 {
			t.Errorf("joiner %d got %d bytes, want 48", i, len(datas[i]))
		}
	}
	// Each refused joiner re-fetches with its own credential; the point is that it
	// happens only on the refusal path, never on a healthy one.
	if got := atomic.LoadInt64(&cf.refused); got != 1 {
		t.Errorf("origin refused %d times, want exactly 1 (the leader)", got)
	}
	cf.mu.Lock()
	defer cf.mu.Unlock()
	if len(cf.seen) < 2 || !strings.Contains(cf.seen[0], "expired") {
		t.Fatalf("expected the expired leader first, got %v", cf.seen)
	}
	for _, u := range cf.seen[1:] {
		if !strings.Contains(u, "valid") {
			t.Errorf("retry used %q; a joiner must retry with its own credential", u)
		}
	}
}

// TestCoalescingLeaderDoesNotRetry: when the caller that *led* the flight is the
// one refused, retrying would resend the same rejected credential, so it must not
// happen -- one refusal, one origin request.
func TestCoalescingLeaderDoesNotRetry(t *testing.T) {
	cf := &credentialFetcher{result: blob(16)}
	c := &Coalescing{F: cf, Key: objectIdentity}

	if _, err := c.Fetch(context.Background(), "https://bkt/obj?sig=expired", 0, 15); !refused(err) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if got := atomic.LoadInt64(&cf.calls); got != 1 {
		t.Errorf("origin called %d times, want 1: an uncontended refusal must not retry", got)
	}
}

// TestCoalescingNonRefusalDoesNotRetry: a transport-level failure is shared as
// before. Retrying it per joiner would multiply load on an origin that is already
// unwell, which is the opposite of what coalescing is for.
func TestCoalescingNonRefusalDoesNotRetry(t *testing.T) {
	var calls int64
	fe := fetcherFunc(func(_ context.Context, _ string, _, _ int64) (Range, error) {
		atomic.AddInt64(&calls, 1)
		time.Sleep(10 * time.Millisecond)
		return Range{}, &StatusError{Code: http.StatusInternalServerError, URL: "u", Status: "500 Internal Server Error"}
	})
	c := &Coalescing{F: fe, Key: objectIdentity}

	const n = 6
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := c.Fetch(context.Background(), fmt.Sprintf("https://bkt/obj?s=%d", i), 0, 9); err == nil {
				t.Error("expected the shared 500 to surface")
			}
		}(i)
	}
	wg.Wait()
	if got := atomic.LoadInt64(&calls); got > 2 {
		t.Errorf("origin called %d times for %d callers; a 500 must not fan out into retries", got, n)
	}
}

// fetcherFunc adapts a function to Fetcher.
type fetcherFunc func(context.Context, string, int64, int64) (Range, error)

func (f fetcherFunc) Fetch(ctx context.Context, url string, start, end int64) (Range, error) {
	return f(ctx, url, start, end)
}

// TestCoalescingWithoutKeyUsesURL documents the default and the hazard it carries:
// distinct signatures do not coalesce.
func TestCoalescingWithoutKeyUsesURL(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(8)}
	c := &Coalescing{F: gf} // no Key

	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = c.Fetch(context.Background(), fmt.Sprintf("https://bkt/obj?sig=%d", i), 0, 7)
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(gf.gate)
	wg.Wait()

	if got := atomic.LoadInt64(&gf.calls); got != n {
		t.Errorf("without Key, %d distinct URLs produced %d fetches, want %d", n, got, n)
	}
}

// TestCoalescingKeyStillSeparatesRanges: collapsing identities must not collapse
// different byte ranges of the same object.
func TestCoalescingKeyStillSeparatesRanges(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(8)}
	c := &Coalescing{F: gf, Key: func(string) string { return "same-object" }}

	ranges := [][2]int64{{0, 1}, {2, 3}, {4, 5}}
	var wg sync.WaitGroup
	for _, r := range ranges {
		wg.Add(1)
		go func(start, end int64) {
			defer wg.Done()
			_, _ = c.Fetch(context.Background(), "https://bkt/obj", start, end)
		}(r[0], r[1])
	}
	time.Sleep(20 * time.Millisecond)
	close(gf.gate)
	wg.Wait()

	if got := atomic.LoadInt64(&gf.calls); got != int64(len(ranges)) {
		t.Errorf("%d distinct ranges produced %d fetches, want %d", len(ranges), got, len(ranges))
	}
}

// TestRedactStripsSignature: an upstream signature is a credential and must never
// appear in an error, log line or metric label.
func TestRedactStripsSignature(t *testing.T) {
	cases := map[string]string{
		"https://bkt/obj?Signature=secret&Expires=1": "https://bkt/obj?<redacted>",
		"https://bkt/obj": "https://bkt/obj",
		"":                "",
	}
	for in, want := range cases {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestFetchErrorsRedactSignature: redaction must be applied on the paths that
// build error strings, since those reach logs and HTTP responses. The status must
// also stay inspectable, so a relay can tell a credential refusal from its own
// failure.
func TestFetchErrorsRedactSignature(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer srv.Close()

	f := &HTTPFetcher{}
	_, err := f.Fetch(context.Background(), srv.URL+"/obj?Signature=topsecret", 0, 10)
	if err == nil {
		t.Fatal("expected an error for 403")
	}
	if strings.Contains(err.Error(), "topsecret") {
		t.Errorf("error leaks the signature: %v", err)
	}
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("error is not a *StatusError: %T", err)
	}
	if se.Code != http.StatusForbidden || !se.Refused() {
		t.Errorf("StatusError{Code:%d}.Refused()=%v, want 403/true", se.Code, se.Refused())
	}
}

func TestStatusErrorRefusedClassification(t *testing.T) {
	for code, want := range map[int]bool{
		http.StatusUnauthorized:        true,
		http.StatusForbidden:           true,
		http.StatusInternalServerError: false,
		http.StatusNotFound:            false,
		http.StatusBadGateway:          false,
	} {
		if got := (&StatusError{Code: code}).Refused(); got != want {
			t.Errorf("StatusError{%d}.Refused() = %v, want %v", code, got, want)
		}
	}
}

func TestCoalescingDistinctKeys(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(8)}
	close(gf.gate) // never block
	c := &Coalescing{F: gf}
	_, _ = c.Fetch(context.Background(), "u", 0, 7)
	_, _ = c.Fetch(context.Background(), "u", 8, 15) // different range
	_, _ = c.Fetch(context.Background(), "v", 0, 7)  // different url
	if got := atomic.LoadInt64(&gf.calls); got != 3 {
		t.Errorf("calls = %d, want 3 (distinct keys)", got)
	}
}

// TestCoalescingCallerCancel: a caller that cancels gets ctx.Err() without
// waiting for the (background) shared fetch, which still completes for others.
func TestCoalescingCallerCancel(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(16)}
	c := &Coalescing{F: gf}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.Fetch(ctx, "u", 0, 15)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled caller err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled caller did not return")
	}
	// Release the shared fetch and confirm a fresh caller still gets the result.
	close(gf.gate)
	r, err := c.Fetch(context.Background(), "u", 0, 15)
	if err != nil || len(r.Data) != 16 {
		t.Errorf("post-cancel fetch: err=%v len=%d", err, len(r.Data))
	}
}

// TestCoalescedResultIsMarked underpins honest byte accounting. Callers that ride
// along on someone else's in-flight fetch receive the same bytes, but nothing
// crossed the network for them; a "bytes transferred" counter that added the length
// once per waiter would report singleflight's savings as extra traffic.
func TestCoalescedResultIsMarked(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(64)}
	c := &Coalescing{F: gf}

	const n = 8
	var wg sync.WaitGroup
	marks := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Fetch(context.Background(), "u", 0, 63)
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
				return
			}
			marks[i] = r.Coalesced
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(gf.gate)
	wg.Wait()

	if got := atomic.LoadInt64(&gf.calls); got != 1 {
		t.Fatalf("underlying calls = %d, want 1", got)
	}
	// Exactly one caller performed the fetch; the rest must be marked coalesced, so
	// summing wire bytes over callers yields one transfer rather than n.
	paid := 0
	for _, m := range marks {
		if !m {
			paid++
		}
	}
	if paid != 1 {
		t.Errorf("%d callers reported paying for the transfer, want exactly 1 (marks=%v)", paid, marks)
	}
}

// TestUncontendedFetchNotMarkedCoalesced: a lone caller did pay, so its bytes must
// be counted or origin traffic would read as zero.
func TestUncontendedFetchNotMarkedCoalesced(t *testing.T) {
	gf := &gateFetcher{gate: make(chan struct{}), result: blob(8)}
	close(gf.gate)
	c := &Coalescing{F: gf}
	r, err := c.Fetch(context.Background(), "u", 0, 7)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if r.Coalesced {
		t.Error("an uncontended fetch was marked coalesced; its bytes would go uncounted")
	}
}

// TestStaleFlightEvictedAfterMaxFlight pins issue #4: a flight whose leader
// hangs forever (ignoring context, as a pathological Fetcher might) used to
// pin its key permanently — every later fetch of the same key joined the dead
// flight and failed on its own timeout. After MaxFlight a new call must evict
// the stale flight and lead a replacement.
func TestStaleFlightEvictedAfterMaxFlight(t *testing.T) {
	var calls int64
	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) }) // release the stalled leader at test end
	f := fetcherFunc(func(ctx context.Context, url string, start, end int64) (Range, error) {
		if atomic.AddInt64(&calls, 1) == 1 {
			<-stall // stalled leader: returns only when the test ends, ignores ctx
		}
		return Range{Data: blob(8), Total: 8}, nil
	})
	c := &Coalescing{F: f, MaxFlight: 50 * time.Millisecond}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Fetch(ctx, "u", 0, 7); err == nil {
		t.Fatal("first caller: want its own ctx deadline, got nil")
	}
	time.Sleep(100 * time.Millisecond) // push the flight past its deadline

	r, err := c.Fetch(context.Background(), "u", 0, 7)
	if err != nil || len(r.Data) != 8 {
		t.Fatalf("replacement flight: err=%v len=%d", err, len(r.Data))
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("underlying calls = %d, want 2 (stale flight evicted and replaced)", got)
	}
}

// TestLateStaleLeaderKeepsReplacement: when a stale flight is evicted, its
// leader's late completion must not delete the replacement flight's entry —
// otherwise the key flaps between flights.
func TestLateStaleLeaderKeepsReplacement(t *testing.T) {
	var calls int64
	gate1 := make(chan struct{})
	gate2 := make(chan struct{})
	f := fetcherFunc(func(ctx context.Context, url string, start, end int64) (Range, error) {
		switch atomic.AddInt64(&calls, 1) {
		case 1:
			<-gate1
		case 2:
			<-gate2
		}
		return Range{Data: blob(8), Total: 8}, nil
	})
	c := &Coalescing{F: f, MaxFlight: 100 * time.Millisecond}

	first := make(chan error, 1)
	go func() { _, err := c.Fetch(context.Background(), "u", 0, 7); first <- err }()
	time.Sleep(20 * time.Millisecond)  // let flight 1 establish
	time.Sleep(110 * time.Millisecond) // push it past its deadline

	second := make(chan error, 1)
	go func() { _, err := c.Fetch(context.Background(), "u", 0, 7); second <- err }()
	time.Sleep(20 * time.Millisecond) // flight 2 (replacement) is now in flight

	close(gate1) // stale leader finishes late...
	time.Sleep(20 * time.Millisecond)

	// ...and a third caller must still join flight 2 (well within its own
	// deadline), not lead a flight 3.
	third := make(chan error, 1)
	go func() { _, err := c.Fetch(context.Background(), "u", 0, 7); third <- err }()
	time.Sleep(20 * time.Millisecond)
	close(gate2)

	for i, ch := range []chan error{first, second, third} {
		if err := <-ch; err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("underlying calls = %d, want 2 (late stale leader must not evict the replacement)", got)
	}
}

// TestFlightContextBoundsStalledOrigin: even a ctx-respecting fetcher used to
// run on context.Background() with no bound. The flight context now expires at
// MaxFlight, so a stalled origin fails every waiter within the bound.
func TestFlightContextBoundsStalledOrigin(t *testing.T) {
	f := fetcherFunc(func(ctx context.Context, url string, start, end int64) (Range, error) {
		<-ctx.Done()
		return Range{}, ctx.Err()
	})
	c := &Coalescing{F: f, MaxFlight: 50 * time.Millisecond}

	start := time.Now()
	if _, err := c.Fetch(context.Background(), "u", 0, 7); err == nil {
		t.Fatal("want the flight deadline error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("flight took %v; MaxFlight did not bound the stall", elapsed)
	}
}

// TestJoinerRetrySkippedWhenCallerGone pins the joiner half of issue #4: a
// caller whose context was cancelled while waiting must not fire a fresh
// origin fetch when the shared flight comes back refused — the retry serves
// only that caller, and the caller is gone.
func TestJoinerRetrySkippedWhenCallerGone(t *testing.T) {
	var calls int64
	gate := make(chan struct{})
	f := fetcherFunc(func(ctx context.Context, url string, start, end int64) (Range, error) {
		atomic.AddInt64(&calls, 1)
		<-gate
		return Range{}, &StatusError{Code: http.StatusUnauthorized}
	})
	c := &Coalescing{F: f}

	leader := make(chan error, 1)
	go func() { _, err := c.Fetch(context.Background(), "u", 0, 7); leader <- err }()
	time.Sleep(20 * time.Millisecond) // flight established

	joinerCtx, cancelJoiner := context.WithCancel(context.Background())
	joiner := make(chan error, 1)
	go func() { _, err := c.Fetch(joinerCtx, "u", 0, 7); joiner <- err }()
	time.Sleep(20 * time.Millisecond) // joiner attached to the flight

	cancelJoiner() // the joiner goes away...
	close(gate)    // ...then the flight comes back refused
	if err := <-leader; err == nil {
		t.Fatal("leader: want the 401 refusal")
	}
	if err := <-joiner; err == nil {
		t.Fatal("joiner: want ctx.Canceled")
	}
	time.Sleep(50 * time.Millisecond) // let any mistaken retry fire
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("underlying calls = %d, want 1 (a gone caller must not retry)", got)
	}
}

// TestAbandonedWaitersReleasedAtDeadline pins the review-found residual of
// issue #4: a joiner whose caller gave up must not block in the flight wait
// forever when the leader ignores cancellation — even when no later caller
// ever arrives for the key. At the flight deadline the waiter re-checks,
// evicts the stale flight, and leads a replacement.
func TestAbandonedWaitersReleasedAtDeadline(t *testing.T) {
	var calls int64
	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) }) // release all stuck fn calls at test end
	f := fetcherFunc(func(ctx context.Context, url string, start, end int64) (Range, error) {
		atomic.AddInt64(&calls, 1)
		<-stall // ignores ctx: simulates a permanently stuck origin
		return Range{Data: blob(8), Total: 8}, nil
	})
	c := &Coalescing{F: f, MaxFlight: 40 * time.Millisecond}

	// Two callers join the same stuck flight and give up; no later caller ever
	// arrives for the key.
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		if _, err := c.Fetch(ctx, "u", 0, 7); err == nil {
			t.Fatal("caller: want its own ctx deadline, got nil")
		}
		cancel()
	}

	// After the deadline the abandoned waiter must have been released and led
	// a replacement flight — not be blocked in the wait forever.
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got < 2 {
		t.Fatalf("underlying calls = %d, want >= 2 (abandoned waiter must be released at the deadline)", got)
	}
}

// TestTransportErrorRedactsCredentials pins issue #7: net/http's *url.Error
// strips only the userinfo password — never the query — so a failed fetch of a
// presigned URL used to leak the live signature into 502 bodies and logs.
// Fetch and Open must both return errors whose string carries neither the
// query nor the userinfo.
func TestTransportErrorRedactsCredentials(t *testing.T) {
	// Port 1 is never listening: client.Do fails before any request is sent.
	const url = "http://user:p%40ss@127.0.0.1:1/obj?X-Sig=SECRET123&b=2"
	f := &HTTPFetcher{}

	if _, err := f.Fetch(context.Background(), url, 0, 7); err == nil {
		t.Fatal("Fetch: want a transport error")
	} else if s := err.Error(); strings.Contains(s, "SECRET123") || strings.Contains(s, "user") && strings.Contains(s, "ss@") {
		t.Fatalf("Fetch error leaks credentials: %v", err)
	}
	if _, err := f.Open(context.Background(), url, nil); err == nil {
		t.Fatal("Open: want a transport error")
	} else if s := err.Error(); strings.Contains(s, "SECRET123") || strings.Contains(s, "user") && strings.Contains(s, "ss@") {
		t.Fatalf("Open error leaks credentials: %v", err)
	}
}

// TestRedactStripsUserinfo pins the other half of issue #7: Redact removed the
// query but kept userinfo, which is credential material too.
func TestRedactStripsUserinfo(t *testing.T) {
	got := Redact("https://user:pass@host.example/path?sig=1")
	if want := "https://host.example/path?<redacted>"; got != want {
		t.Fatalf("Redact = %q, want %q", got, want)
	}
	if got := Redact("https://host.example/no-userinfo"); got != "https://host.example/no-userinfo" {
		t.Fatalf("Redact(no creds) = %q", got)
	}
}
