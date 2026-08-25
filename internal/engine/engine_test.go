package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"net/http"
	"net/http/httptest"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/store"
)

func blob(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// countingOrigin serves content with Range support and counts requests.
func countingOrigin(t *testing.T, content []byte, counter *int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if counter != nil {
			atomic.AddInt64(counter, 1)
		}
		http.ServeContent(w, r, "blob", time.Unix(0, 0), bytes.NewReader(content))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// truncatingFetcher reveals the true object size on the size probe but delivers
// one byte fewer than requested for any real block fetch, simulating an origin
// or proxy short read.
type truncatingFetcher struct {
	size int64
	data []byte
}

func (f *truncatingFetcher) Fetch(_ context.Context, _ string, start, end int64) (fetch.Range, error) {
	if end <= start { // size probe (bytes=0-0): one byte plus the true total
		return fetch.Range{Data: f.data[start : start+1], Total: f.size}, nil
	}
	n := end - start // one byte short of the requested (end-start+1)
	return fetch.Range{Data: f.data[start : start+n], Total: f.size}, nil
}

// TestServeRejectsShortOriginBlock: an origin that returns a block one byte
// short must fail the read and must NOT cache the truncated block. The store is
// write-once per key, so a cached short block could never be repaired by a
// later correct fetch — only refusing it at ingestion keeps the cache clean.
func TestServeRejectsShortOriginBlock(t *testing.T) {
	cfg := testCfg()
	path := filepath.Join(t.TempDir(), "b.dat")
	st, err := store.Open(store.Options{Path: path, SlotSize: cfg.BlockSize, Slots: 1024})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	content := blob(100)
	e, err := New(Options{Chunk: cfg, Store: st, Fetcher: &truncatingFetcher{size: int64(len(content)), data: content}})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	const url = "http://origin/blob"
	var buf bytes.Buffer
	if err := e.Serve(context.Background(), &buf, url, 0, 15); err == nil {
		t.Fatal("expected an error serving a short origin block")
	}
	oid, _ := chunk.ObjectID(url)
	ck := chunk.ChunkKey("dart", oid, 0)
	if _, ok, _ := st.Get(store.BlockKey{Chunk: ck, Block: 0}); ok {
		t.Error("a truncated block was cached; it must be refused")
	}
}

// testCfg uses tiny sizes so ranges span multiple blocks and chunks:
// BlockSize 16, ChunkSize 64 => 4 blocks per chunk.
func testCfg() chunk.Config { return chunk.Config{ChunkSize: 64, BlockSize: 16} }

func newEngine(t *testing.T, cfg chunk.Config) *Engine {
	t.Helper()
	path := filepath.Join(t.TempDir(), "b.dat")
	st, err := store.Open(store.Options{Path: path, SlotSize: cfg.BlockSize, Slots: 1024})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	e, err := New(Options{Chunk: cfg, Store: st, Fetcher: &fetch.Coalescing{F: &fetch.HTTPFetcher{}}})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return e
}

func serve(t *testing.T, e *Engine, url string, start, end int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := e.Serve(context.Background(), &buf, url, start, end); err != nil {
		t.Fatalf("Serve[%d,%d]: %v", start, end, err)
	}
	return buf.Bytes()
}

func TestServeFullAndRange(t *testing.T) {
	content := blob(100)
	origin := countingOrigin(t, content, nil)
	e := newEngine(t, testCfg())

	if got := serve(t, e, origin.URL, 0, 99); !bytes.Equal(got, content) {
		t.Errorf("full serve mismatch (%d bytes)", len(got))
	}
	if got := serve(t, e, origin.URL, 20, 45); !bytes.Equal(got, content[20:46]) {
		t.Errorf("range serve mismatch")
	}
}

func TestServeAcrossBlocksAndChunks(t *testing.T) {
	content := blob(100)
	origin := countingOrigin(t, content, nil)
	e := newEngine(t, testCfg())
	// 60..70 crosses block 3 (chunk 0, bytes 48-63) into block 4 (chunk 1, 64-79).
	if got := serve(t, e, origin.URL, 60, 70); !bytes.Equal(got, content[60:71]) {
		t.Errorf("cross-chunk serve mismatch: %v", got)
	}
}

func TestServeEndClampAndBadStart(t *testing.T) {
	content := blob(100)
	origin := countingOrigin(t, content, nil)
	e := newEngine(t, testCfg())

	if got := serve(t, e, origin.URL, 90, 999); !bytes.Equal(got, content[90:100]) {
		t.Errorf("end-clamp serve mismatch")
	}
	if err := e.Serve(context.Background(), &bytes.Buffer{}, origin.URL, 200, 300); err != ErrRangeNotSatisfiable {
		t.Errorf("bad start err = %v, want ErrRangeNotSatisfiable", err)
	}
}

func TestSize(t *testing.T) {
	content := blob(12345)
	origin := countingOrigin(t, content, nil)
	e := newEngine(t, testCfg())
	sz, err := e.Size(context.Background(), origin.URL)
	if err != nil || sz != 12345 {
		t.Errorf("Size = %d, err=%v, want 12345", sz, err)
	}
}

// TestCacheHitAvoidsRefetch: a repeated identical Serve must not touch origin
// again (blocks cached, size cached).
func TestCacheHitAvoidsRefetch(t *testing.T) {
	content := blob(100)
	var cnt int64
	origin := countingOrigin(t, content, &cnt)
	e := newEngine(t, testCfg())

	_ = serve(t, e, origin.URL, 0, 99)
	first := atomic.LoadInt64(&cnt)
	if first == 0 {
		t.Fatal("expected origin requests on first serve")
	}
	got := serve(t, e, origin.URL, 0, 99)
	second := atomic.LoadInt64(&cnt)
	if second != first {
		t.Errorf("cache miss on second serve: origin count %d -> %d", first, second)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("second serve bytes mismatch")
	}
}

// TestConcurrentServe: overlapping concurrent reads produce correct bytes and
// are race-free (run with -race).
func TestConcurrentServe(t *testing.T) {
	content := blob(500)
	origin := countingOrigin(t, content, nil)
	e := newEngine(t, testCfg())

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			start := int64((g * 7) % 400)
			end := start + 60
			var buf bytes.Buffer
			if err := e.Serve(context.Background(), &buf, origin.URL, start, end); err != nil {
				t.Errorf("Serve: %v", err)
				return
			}
			if !bytes.Equal(buf.Bytes(), content[start:end+1]) {
				t.Errorf("g%d: bytes mismatch for [%d,%d]", g, start, end)
			}
		}(g)
	}
	wg.Wait()
}

func TestServeOriginError(t *testing.T) {
	e := newEngine(t, testCfg())
	// Port 1 is closed: the size probe (and thus Serve) fails.
	if err := e.Serve(context.Background(), &bytes.Buffer{}, "http://127.0.0.1:1/x", 0, 10); err == nil {
		t.Error("expected Serve to fail on unreachable origin")
	}
}

func TestNewValidation(t *testing.T) {
	st, _ := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "b.dat"), SlotSize: 16, Slots: 4})
	t.Cleanup(func() { st.Close() })
	if _, err := New(Options{Chunk: chunk.Config{ChunkSize: 10, BlockSize: 4}, Store: st, Fetcher: &fetch.HTTPFetcher{}}); err == nil {
		t.Error("expected invalid chunk config to fail")
	}
	if _, err := New(Options{Chunk: testCfg(), Store: nil, Fetcher: &fetch.HTTPFetcher{}}); err == nil {
		t.Error("expected nil store to fail")
	}
	if _, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: nil}); err == nil {
		t.Error("expected nil fetcher to fail")
	}
}

// TestSizeHiddenTotalIsProbeFailure pins issue #3: a 206 that hides the total
// ("Content-Range: bytes 0-0/*") must fail the probe loudly instead of caching
// a fabricated size (previously size=1 from len(probe body)). A fabricated
// size poisons every later read of the object — full GETs would silently
// serve one byte, larger ranges 416 — and the sizes cache is write-once for
// the process lifetime.
func TestSizeHiddenTotalIsProbeFailure(t *testing.T) {
	content := blob(64)
	var cnt int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&cnt, 1)
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[:1])
	}))
	defer origin.Close()

	e := newEngine(t, testCfg())
	if _, err := e.Size(context.Background(), origin.URL); err == nil {
		t.Fatal("Size succeeded on a 206 without a total; want a loud probe failure")
	}
	// Nothing cached: a second probe reaches origin again (a fixed origin
	// recovers without a restart)...
	if _, err := e.Size(context.Background(), origin.URL); err == nil {
		t.Fatal("second Size succeeded; the failure must not be cached either")
	}
	if got := atomic.LoadInt64(&cnt); got != 2 {
		t.Fatalf("origin probes = %d, want 2 (nothing cached)", got)
	}
	// ...and a plain GET errors instead of silently serving a 1-byte body.
	var buf bytes.Buffer
	if err := e.Serve(context.Background(), &buf, origin.URL, 0, 63); err == nil {
		t.Fatalf("Serve wrote %d bytes with no error; want the probe failure", buf.Len())
	}
}
