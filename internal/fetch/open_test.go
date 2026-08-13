package fetch

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// rangeBlind serves the full body with 200 regardless of any Range header.
func rangeBlind(content []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	}
}

// TestHTTPFetcherRangeIgnoredMarked: a 200 full body to a ranged request must
// set RangeIgnored, return the sliced window, and take Total from
// Content-Length.
func TestHTTPFetcherRangeIgnoredMarked(t *testing.T) {
	content := blob(1000)
	srv := httptest.NewServer(rangeBlind(content))
	defer srv.Close()
	f := &HTTPFetcher{}
	got, err := f.Fetch(context.Background(), srv.URL, 200, 299)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !got.RangeIgnored {
		t.Error("RangeIgnored not set on a 200 to a ranged request")
	}
	if !bytes.Equal(got.Data, content[200:300]) {
		t.Errorf("range not sliced from 200 response")
	}
	if got.Total != 1000 {
		t.Errorf("Total = %d, want 1000 from Content-Length", got.Total)
	}
}

// TestHTTPFetcherRangeIgnoredChunked: a chunked 200 reveals no Content-Length,
// so Total must be -1 — but the window is still sliced and marked.
func TestHTTPFetcherRangeIgnoredChunked(t *testing.T) {
	content := blob(1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush() // commit headers: forces chunked encoding
		w.Write(content)
	}))
	defer srv.Close()
	f := &HTTPFetcher{}
	got, err := f.Fetch(context.Background(), srv.URL, 0, 9)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !got.RangeIgnored {
		t.Error("RangeIgnored not set")
	}
	if got.Total != -1 {
		t.Errorf("Total = %d, want -1 for a chunked response", got.Total)
	}
	if !bytes.Equal(got.Data, content[:10]) {
		t.Errorf("window mismatch")
	}
}

// infReader is an unbounded body used to prove a ranged 200 is not read in
// full: if Fetch tried to buffer the body, this test would never return.
type infReader struct{}

func (infReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestHTTPFetcherRangeIgnoredBoundedRead: against a Range-ignoring origin the
// fetcher must read only up to the end of the requested window, then abort.
func TestHTTPFetcherRangeIgnoredBoundedRead(t *testing.T) {
	f := &HTTPFetcher{Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        make(http.Header),
			Body:          io.NopCloser(infReader{}),
			ContentLength: 1 << 40,
		}, nil
	})}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := f.Fetch(ctx, "http://origin/blob", 5, 9)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(got.Data) != "xxxxx" {
		t.Errorf("Data = %q, want %q", got.Data, "xxxxx")
	}
	if got.Total != 1<<40 || !got.RangeIgnored {
		t.Errorf("Total = %d RangeIgnored = %v", got.Total, got.RangeIgnored)
	}
}

// TestOpenPassthrough: Open forwards the per-request Range header, returns
// the origin's status as-is (even an error status), and streams the body.
func TestOpenPassthrough(t *testing.T) {
	content := blob(100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.Error(w, "nope", http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Range"); got != "bytes=4-9" {
			t.Errorf("origin saw Range %q, want bytes=4-9", got)
		}
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	}))
	defer srv.Close()

	f := &HTTPFetcher{}
	resp, err := f.Open(context.Background(), srv.URL+"/blob", http.Header{"Range": []string{"bytes=4-9"}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") != `"abc"` {
		t.Errorf("status/headers not preserved: %s %v", resp.Status, resp.Header)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, content) {
		t.Errorf("body mismatch")
	}

	resp, err = f.Open(context.Background(), srv.URL+"/missing", nil)
	if err != nil {
		t.Fatalf("Open 404: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %s, want 404 forwarded as-is", resp.Status)
	}
}

// fetchOnly implements Fetcher but not Opener.
type fetchOnly struct{}

func (fetchOnly) Fetch(context.Context, string, int64, int64) (Range, error) {
	return Range{}, nil
}

// TestCoalescingOpen: Open delegates to the inner fetcher when it can stream,
// and reports a clear error when it cannot.
func TestCoalescingOpen(t *testing.T) {
	srv := httptest.NewServer(rangeBlind(blob(10)))
	defer srv.Close()

	c := &Coalescing{F: &HTTPFetcher{}}
	resp, err := c.Open(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	resp.Body.Close()

	c = &Coalescing{F: fetchOnly{}}
	if _, err := c.Open(context.Background(), srv.URL, nil); err == nil ||
		!strings.Contains(err.Error(), "streaming open") {
		t.Errorf("err = %v, want a streaming-open unsupported error", err)
	}
}
