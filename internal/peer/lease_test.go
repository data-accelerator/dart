package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/store"
)

func TestGetIntoBoundedAndGeneration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      int
		chunked   bool
		wantError bool
	}{
		{"complete", 16, false, false}, {"short", 15, true, true}, {"oversized", 17, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !tc.chunked {
					w.Header().Set("Content-Length", fmt.Sprint(tc.body))
				}
				w.WriteHeader(200)
				if tc.chunked {
					w.(http.Flusher).Flush()
				}
				w.Write(bytes.Repeat([]byte{7}, tc.body))
			}))
			defer srv.Close()
			pc := NewClient()
			pc.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 9})
			addr := strings.TrimPrefix(srv.URL, "http://")
			dst := make([]byte, 16)
			result, held, err := pc.GetInto(context.Background(), addr, BlockRequest{}, dst, nil)
			if tc.wantError {
				if !errors.Is(err, ErrBlockLength) || pc.Breaker.Healthy(addr) {
					t.Fatal("bad length not quarantined", held, err)
				}
			} else if err != nil || !held || &result[0] != &dst[0] || !pc.Breaker.Healthy(addr) {
				t.Fatal("direct body mismatch", held, err)
			}
		})
	}
}

func TestLeaseServerReleasesOnWriteError(t *testing.T) {
	mem, err := store.OpenMem(store.MemOptions{SlotSize: 16, Slots: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()
	key := store.BlockKey{}
	mem.Put(key, []byte("data"))
	srv := &Server{Borrow: func(context.Context, BlockRequest) (store.Lease, bool, error) { return mem.Acquire(key) }}
	request := httptest.NewRequest("GET", "/peer/v1/block/0/0", nil)
	srv.ServeHTTP(&failedLeaseWriter{header: make(http.Header)}, request)
	mem.Delete(key)
	r, err := mem.Reserve(context.Background(), store.BlockKey{Chunk: 1}, 16, store.Owned)
	if err != nil {
		t.Fatal("write error leaked pin", err)
	}
	r.Abort()
}

type failedLeaseWriter struct{ header http.Header }

func (w *failedLeaseWriter) Header() http.Header       { return w.header }
func (w *failedLeaseWriter) WriteHeader(int)           {}
func (w *failedLeaseWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
