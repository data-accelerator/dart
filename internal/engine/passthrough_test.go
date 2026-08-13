package engine

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
)

// rangeBlindOrigin serves the full body with 200 regardless of any Range
// header, counting requests.
func rangeBlindOrigin(t *testing.T, content []byte, counter *int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if counter != nil {
			atomic.AddInt64(counter, 1)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestHandlerPassthroughForRangeBlindOrigin: an origin that answers the size
// probe with 200 is marked Range-unsupported, and every client request is
// then proxied verbatim — no block fetches, nothing cached.
func TestHandlerPassthroughForRangeBlindOrigin(t *testing.T) {
	content := blob(100) // spans 7 blocks of testCfg, so block-serving would cost 7+1 origin hits
	var hits int64
	origin := rangeBlindOrigin(t, content, &hits)
	st := openStoreAt(t)
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, origin.URL)

	for i, want := range []int64{2, 3} { // probe+GET, then GET only
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://dart/blob", nil))
		resp := rec.Result()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || len(body) != len(content) {
			t.Fatalf("request %d: %s, %d bytes", i, resp.Status, len(body))
		}
		if got := atomic.LoadInt64(&hits); got != want {
			t.Fatalf("request %d: origin hits = %d, want %d", i, got, want)
		}
	}
	if !e.RangeUnsupported(origin.URL) {
		t.Error("origin not marked Range-unsupported after a 200 probe")
	}
	if _, ok, err := st.Get(blockKeyFor(origin.URL)); err != nil || ok {
		t.Errorf("block cached from a passthrough (ok=%v, err=%v)", ok, err)
	}
}

// TestPassthroughForwardsRangeVerbatim: the client's Range is forwarded, and
// the client receives exactly what the origin answered — a Range-ignoring
// origin means a full-body 200 even to a ranged request.
func TestPassthroughForwardsRangeVerbatim(t *testing.T) {
	content := blob(100)
	var sawRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRange = r.Header.Get("Range")
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	}))
	t.Cleanup(srv.Close)

	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, srv.URL)

	req := httptest.NewRequest(http.MethodGet, "http://dart/blob", nil)
	req.Header.Set("Range", "bytes=10-19")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	resp := rec.Result()
	if sawRange != "bytes=10-19" {
		t.Errorf("origin saw Range %q, want bytes=10-19", sawRange)
	}
	// Verbatim: the origin's 200 full body, not a 206 window.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %s, want the origin's verbatim 200", resp.Status)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != len(content) {
		t.Errorf("body = %d bytes, want the full %d", len(body), len(content))
	}
}

// TestPassthroughHEAD: a HEAD is served from a GET upstream (a presigned URL
// forbids HEAD), forwarding Content-Length with an empty body.
func TestPassthroughHEAD(t *testing.T) {
	content := blob(50)
	var hits int64
	origin := rangeBlindOrigin(t, content, &hits)
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, origin.URL)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "http://dart/blob", nil))
	resp := rec.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || len(body) != 0 {
		t.Errorf("HEAD: %s, %d bytes", resp.Status, len(body))
	}
	if got := resp.Header.Get("Content-Length"); got != "50" {
		t.Errorf("Content-Length = %q, want 50", got)
	}
}

// TestRangeCapableOriginNotMarked: a 206-capable origin must never be marked
// or passthrough-ed.
func TestRangeCapableOriginNotMarked(t *testing.T) {
	content := blob(100)
	var hits int64
	origin := countingOrigin(t, content, &hits)
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := e.Size(context.Background(), origin.URL); err != nil {
		t.Fatalf("Size: %v", err)
	}
	if e.RangeUnsupported(origin.URL) {
		t.Error("206-capable origin marked Range-unsupported")
	}
}

// TestPassthroughCountsMetrics: a proxied request increments
// dart_passthrough_total and moves bytes on both wires.
func TestPassthroughCountsMetrics(t *testing.T) {
	content := blob(100)
	origin := rangeBlindOrigin(t, content, nil)
	reg := metrics.NewRegistry()
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Metrics: NewMetrics(reg),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, origin.URL)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://dart/blob", nil))
	if rec.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %s", rec.Result().Status)
	}
	var out strings.Builder
	if err := reg.Render(&out); err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, want := range []string{
		`dart_passthrough_total{reason="range_unsupported"} 1`,
		`dart_bytes_total{direction="client"} 100`,
		`dart_bytes_total{direction="origin_in"} 100`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	// No block was fetched from origin: the block-source counter must not move.
	if !strings.Contains(out.String(), `dart_block_source_total{source="origin"} 0`) {
		t.Error("passthrough counted as an origin block fetch")
	}
}

// TestPassthroughUnavailableWithoutOpener: a fetcher that cannot stream makes
// the passthrough fallback fail cleanly with 502.
func TestPassthroughUnavailableWithoutOpener(t *testing.T) {
	content := blob(10)
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t),
		Fetcher: &noRangeOnlyFetcher{data: content},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, "http://origin/blob")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://dart/blob", nil))
	if rec.Result().StatusCode != http.StatusBadGateway {
		t.Errorf("status = %s, want 502", rec.Result().Status)
	}
}

// noRangeOnlyFetcher simulates a Range-ignoring origin but cannot stream.
type noRangeOnlyFetcher struct{ data []byte }

func (f *noRangeOnlyFetcher) Fetch(_ context.Context, _ string, start, end int64) (fetch.Range, error) {
	return fetch.Range{
		Data:         f.data[start : end+1],
		Total:        int64(len(f.data)),
		RangeIgnored: true,
	}, nil
}

// TestRelayDeclinesRangeBlindOrigin: a relay asked for a block of a
// Range-unsupported object declines (held=false), so the requester uses its
// own passthrough path instead of this node pulling the whole object per
// block on its behalf.
func TestRelayDeclinesRangeBlindOrigin(t *testing.T) {
	content := blob(100)
	origin := rangeBlindOrigin(t, content, nil)
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Mark the object via the probe, then ask this node (as a peer) for a block.
	if _, err := e.Size(context.Background(), origin.URL); err != nil {
		t.Fatalf("Size: %v", err)
	}
	src := e.PeerStreamSource()
	var buf strings.Builder
	n, held, err := src(context.Background(), peer.BlockRequest{
		Key: blockKeyFor(origin.URL), URL: origin.URL, Hop: 1,
	}, &buf, func(int64) {})
	if err != nil {
		t.Fatalf("source: %v", err)
	}
	if held || n != 0 || buf.Len() != 0 {
		t.Errorf("relay served a Range-unsupported object: held=%v n=%d", held, n)
	}
}
