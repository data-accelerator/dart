package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
)

// Exercise the shipped assembly, not just an explicitly constructed peer.Server.
func TestNodePeerWaitsForCompleteBlock(t *testing.T) {
	content := blob(16)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "blob", time.Time{}, bytes.NewReader(content))
	}))
	defer origin.Close()
	partial, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "16")
		_, _ = w.Write(content[:8])
		w.(http.Flusher).Flush()
		close(partial)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		panic(http.ErrAbortHandler)
	}))
	defer parent.Close()
	defer unblock()
	var url string
	var key uint64
	for i := 0; ; i++ {
		url = fmt.Sprintf("%s/blob%d", origin.URL, i)
		oid, _ := chunk.ObjectID(url)
		key = chunk.ChunkKey("dart", oid, 0)
		if hashring.Rank(key, []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}})[0].ID == "A" {
			break
		}
	}
	cfg := config{cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16, prefix: "dart", selfID: "B", fanout: 1, peers: "A@" + strings.TrimPrefix(parent.URL, "http://") + ",B@127.0.0.1:1"}
	n, err := build(cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer n.closer.Close()
	srv := httptest.NewServer(n.peer)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := make(chan struct{})
	var firstOnce sync.Once
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotFirstResponseByte: func() { firstOnce.Do(func() { close(first) }) }})
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/peer/v2/block/%x/0", srv.URL, key), nil)
	req.Header.Set(peer.HeaderOrigin, url)
	done := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- err
			return
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err == nil && (resp.StatusCode != 200 || resp.ContentLength != 16 || !bytes.Equal(data, content)) {
			err = fmt.Errorf("response=%d/%d %x", resp.StatusCode, resp.ContentLength, data)
		}
		done <- err
	}()
	select {
	case <-partial:
	case <-ctx.Done():
		t.Fatal("parent request did not start")
	}
	select {
	case <-first:
		t.Fatal("node exposed unvalidated upstream bytes")
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
