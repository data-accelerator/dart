package engine

import (
	"bytes"
	"context"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/metrics"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/peer"
)

type observedPeerResponse struct {
	http.ResponseWriter
	started chan struct{}
	once    sync.Once
}

func (w *observedPeerResponse) WriteHeader(status int) {
	w.once.Do(func() { close(w.started) })
	w.ResponseWriter.WriteHeader(status)
}
func (w *observedPeerResponse) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.ResponseWriter.Write(b)
}

func TestWholeBlockRelayCommitsOnlyAfterValidation(t *testing.T) {
	for _, mode := range []string{"complete", "truncated", "wrong-length"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			content := blob(16)
			var originCalls int64
			origin := countingOrigin(t, content, &originCalls)
			partial, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode != "wrong-length" {
					w.Header().Set("Content-Length", "16")
				}
				_, _ = w.Write(content[:8])
				w.(http.Flusher).Flush()
				close(partial)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				switch mode {
				case "complete":
					_, _ = w.Write(content[8:])
				case "truncated":
					panic(http.ErrAbortHandler)
				}
			}))
			defer parent.Close()
			defer unblock()
			cli := peer.NewClient()
			cli.Breaker = peer.NewBreaker(peer.BreakerOptions{FailureThreshold: 3, Cooldown: time.Hour})
			e, _ := tailEngine(t, cli, parent, origin.URL)
			if _, err := e.Size(ctx, origin.URL); err != nil {
				t.Fatal(err)
			}
			atomic.StoreInt64(&originCalls, 0)
			rec := httptest.NewRecorder()
			downstream := &observedPeerResponse{ResponseWriter: rec, started: make(chan struct{})}
			handler := &peer.Server{Src: e.PeerSource()}
			req := httptest.NewRequest(http.MethodGet, blockPathForTest(origin.URL), nil).WithContext(ctx)
			req.Header.Set(peer.HeaderOrigin, origin.URL)
			done := make(chan struct{})
			go func() { defer close(done); handler.ServeHTTP(downstream, req) }()
			waitBlockEvent(t, ctx, partial)
			select {
			case <-downstream.started:
				t.Fatal("committed before receiving/validating the complete block")
			default:
			}
			unblock()
			waitBlockEvent(t, ctx, done)
			if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), content) || rec.Header().Get("Content-Length") != "16" {
				t.Fatalf("response=%d %x, headers %v", rec.Code, rec.Body.Bytes(), rec.Header())
			}
			wantCalls := int64(1)
			if mode == "complete" {
				wantCalls = 0
			}
			if got := atomic.LoadInt64(&originCalls); got != wantCalls {
				t.Fatalf("origin calls %d want %d", got, wantCalls)
			}
			wantState := peer.BreakerClosed
			if mode == "wrong-length" {
				wantState = peer.BreakerOpen
			}
			if got := cli.Breaker.State(strings.TrimPrefix(parent.URL, "http://")); got != wantState {
				t.Fatalf("parent circuit %v want %v", got, wantState)
			}
			cached, ok, err := e.store.Get(blockKeyFor(origin.URL))
			if err != nil || !ok || !bytes.Equal(cached, content) {
				t.Fatalf("wrong cached result %x %v %v", cached, ok, err)
			}
		})
	}
}

func blockPathForTest(url string) string {
	key := blockKeyFor(url)
	return "/peer/v2/block/" + strconv.FormatUint(key.Chunk, 16) + "/0"
}

func TestWholeBlockRelayReportsCleanFailure(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	e := prefetchEngine(t, content, 4, blockFetchFunc(func(context.Context, string, int64, int64) (fetch.Range, error) {
		return fetch.Range{}, io.ErrUnexpectedEOF
	}))
	e.sizes[fetch.OriginKey(origin.URL)] = sizeMeta{size: 16}
	srv := httptest.NewServer(&peer.Server{Src: e.PeerSource()})
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+blockPathForTest(origin.URL), nil)
	req.Header.Set(peer.HeaderOrigin, origin.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable || string(body) != "upstream unavailable\n" || resp.Header.Get(peer.HeaderSourceFailure) != "1" {
		t.Fatalf("response %d %q %v", resp.StatusCode, body, err)
	}
	if e.store.Has(blockKeyFor(origin.URL)) {
		t.Fatal("failed block cached")
	}
}

func TestWarmPeerReadRecordsCacheHit(t *testing.T) {
	content := blob(16)
	var originCalls int64
	origin := countingOrigin(t, content, &originCalls)
	e := prefetchEngine(t, content, 2, newFetcher())
	e.mx = NewMetrics(metrics.NewRegistry())
	key := blockKeyFor(origin.URL)
	if err := e.store.Put(key, content); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&peer.Server{Src: e.PeerSource()})
	defer srv.Close()
	data, held, err := peer.NewClient().Get(context.Background(), strings.TrimPrefix(srv.URL, "http://"), peer.BlockRequest{Key: key, URL: origin.URL})
	if err != nil || !held || !bytes.Equal(data, content) || e.mx.cacheHits.Value() != 1 || e.mx.peerHits.Value() != 0 || e.mx.originGet.Value() != 0 || atomic.LoadInt64(&originCalls) != 0 {
		t.Fatalf("warm read %v %v; hits=%d", held, err, e.mx.cacheHits.Value())
	}
}
