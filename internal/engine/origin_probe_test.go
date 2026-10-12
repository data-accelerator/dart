package engine

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/fetch"
)

func contentKeyedEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t),
		Fetcher: &fetch.Coalescing{F: &fetch.HTTPFetcher{}, Key: func(raw string) string {
			id, _ := chunk.ObjectID(raw)
			return id
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestSizeScopesRangeCapabilityToOrigin(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		for _, blindFirst := range []bool{false, true} {
			name := "known-total"
			if chunked {
				name = "unknown-total"
			}
			if blindFirst {
				name += "/blind-first"
			} else {
				name += "/ranged-first"
			}
			t.Run(name, func(t *testing.T) {
				content := blob(16)
				var rangedCalls, blindCalls int64
				blind := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					atomic.AddInt64(&blindCalls, 1)
					if chunked {
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
					}
					_, _ = w.Write(content)
				}))
				defer blind.Close()
				ranged := countingOrigin(t, content, &rangedCalls)
				path := "/v2/image/blobs/sha256:" + strings.Repeat("a", 64)
				urls := []string{ranged.URL + path, blind.URL + path}
				if blindFirst {
					urls[0], urls[1] = urls[1], urls[0]
				}
				e := contentKeyedEngine(t)
				for _, raw := range urls {
					got, err := e.Size(context.Background(), raw+"?Signature=first")
					want := int64(len(content))
					if strings.HasPrefix(raw, blind.URL) && chunked {
						want = -1
					}
					if err != nil || got != want {
						t.Fatalf("Size(%s) = %d, %v; want %d", raw, got, err, want)
					}
				}
				if !e.RangeUnsupported(blind.URL+path) || e.RangeUnsupported(ranged.URL+path) {
					t.Fatal("Range capability crossed origin endpoints")
				}
				if atomic.LoadInt64(&blindCalls) != 1 || atomic.LoadInt64(&rangedCalls) != 1 {
					t.Fatal("each origin must receive its own probe")
				}
				// Refreshed signatures share endpoint metadata, not a new probe.
				if _, err := e.Size(context.Background(), ranged.URL+path+"?Signature=renewed"); err != nil {
					t.Fatal(err)
				}
				if atomic.LoadInt64(&rangedCalls) != 1 {
					t.Fatal("signature refresh caused another endpoint probe")
				}
				got := serve(t, e, ranged.URL+path, 0, 15)
				if !bytes.Equal(got, content) || !e.store.Has(blockKeyFor(ranged.URL+path)) {
					t.Fatal("Range-capable origin did not populate the block cache")
				}
				// A third Range-capable endpoint still probes, but reuses the
				// already-cached digest block rather than fetching it again.
				var otherCalls int64
				other := countingOrigin(t, content, &otherCalls)
				if got := serve(t, e, other.URL+path, 0, 15); !bytes.Equal(got, content) || atomic.LoadInt64(&otherCalls) != 1 {
					t.Fatal("per-origin probes broke cross-origin content-cache dedup")
				}
			})
		}
	}
}

func TestConcurrentSizeProbesDoNotShareCapabilities(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	blind := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "abcd")
	}))
	defer blind.Close()
	defer close(release)
	ranged := countingOrigin(t, []byte("abcd"), nil)
	path := "/sha256:" + strings.Repeat("a", 64)
	e := contentKeyedEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := e.Size(ctx, blind.URL+path)
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first probe never reached its origin")
	}
	// The first origin is still blocked. A content-keyed probe flight would
	// make this independent endpoint wait for it and miss the deadline.
	probeCtx, probeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer probeCancel()
	if size, err := e.Size(probeCtx, ranged.URL+path); err != nil || size != 4 {
		t.Fatalf("independent probe = %d, %v; want 4", size, err)
	}
	// Release the blocked handler; the deferred close also covers failures.
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if e.RangeUnsupported(ranged.URL+path) || !e.RangeUnsupported(blind.URL+path) {
		t.Fatal("concurrent probes mixed endpoint capabilities")
	}
}
