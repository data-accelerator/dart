package peer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

func TestStoreStreamSourceRoundtrip(t *testing.T) {
	st := newStore(t)
	key := store.BlockKey{Chunk: 0xBEEF, Block: 3}
	want := bytes.Repeat([]byte{0x21}, 900)
	if err := st.Put(key, want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	srv := httptest.NewServer(&StreamServer{NodeID: "S", Src: StoreStreamSource(st)})
	defer srv.Close()

	var buf bytes.Buffer
	n, held, err := NewClient().Stream(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: key}, &buf)
	if err != nil || !held {
		t.Fatalf("Stream held=%v err=%v", held, err)
	}
	if n != int64(len(want)) || !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("streamed %d bytes, want %d (equal=%v)", n, len(want), bytes.Equal(buf.Bytes(), want))
	}
}

// TestStreamServerSetsContentLength: a locally-held block has a known size, so
// the peer response is Content-Length framed (not chunked).
func TestStreamServerSetsContentLength(t *testing.T) {
	st := newStore(t)
	key := store.BlockKey{Chunk: 1, Block: 1}
	_ = st.Put(key, bytes.Repeat([]byte{7}, 128))
	srv := httptest.NewServer(&StreamServer{NodeID: "S", Src: StoreStreamSource(st)})
	defer srv.Close()

	resp, err := http.Get(blockURL(addrOf(t, srv.URL), key))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.ContentLength != 128 {
		t.Errorf("ContentLength = %d, want 128", resp.ContentLength)
	}
	if len(resp.TransferEncoding) != 0 {
		t.Errorf("unexpected chunked response: %v", resp.TransferEncoding)
	}
	if resp.Header.Get(HeaderNode) != "S" {
		t.Errorf("X-DART-Node = %q", resp.Header.Get(HeaderNode))
	}
}

func TestStreamServerMissAndErrors(t *testing.T) {
	st := newStore(t)
	srv := httptest.NewServer(&StreamServer{Src: StoreStreamSource(st)})
	defer srv.Close()
	addr := addrOf(t, srv.URL)

	// Miss -> 404, and Stream reports held=false with nothing written.
	var buf bytes.Buffer
	n, held, err := NewClient().Stream(context.Background(), addr, BlockRequest{Key: store.BlockKey{Chunk: 9, Block: 9}}, &buf)
	if err != nil || held || n != 0 || buf.Len() != 0 {
		t.Errorf("miss: n=%d held=%v err=%v buflen=%d", n, held, err, buf.Len())
	}

	// Bad path -> 400.
	resp, err := http.Get(srv.URL + "/peer/v1/block/zz/1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad path status = %d, want 400", resp.StatusCode)
	}

	// Non-GET -> 405.
	resp2, err := http.Post(srv.URL+"/peer/v1/block/ab/1", "text/plain", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", resp2.StatusCode)
	}
}

// TestStreamServerEarlyErrorGives500: a source that fails before writing must
// produce a clean 500, not a truncated 200.
func TestStreamServerEarlyErrorGives500(t *testing.T) {
	srv := httptest.NewServer(&StreamServer{Src: func(context.Context, BlockRequest, io.Writer, func(int64)) (int64, bool, error) {
		return 0, false, errors.New("boom")
	}})
	defer srv.Close()
	resp, err := http.Get(blockURL(addrOf(t, srv.URL), store.BlockKey{Chunk: 1, Block: 1}))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// TestCutThroughPipelines proves the relay is genuinely cut-through: a source
// that writes the block in two halves with a pause in between must deliver the
// first half to the client *before* it produces the second, rather than the
// client seeing nothing until the whole block is ready.
func TestCutThroughPipelines(t *testing.T) {
	const half = 256
	release := make(chan struct{})
	srv := httptest.NewServer(&StreamServer{NodeID: "S",
		Src: func(_ context.Context, _ BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
			sizer(2 * half)
			n1, err := w.Write(bytes.Repeat([]byte{'a'}, half))
			if err != nil {
				return int64(n1), true, err
			}
			<-release // hold the second half until the test observes the first
			n2, err := w.Write(bytes.Repeat([]byte{'b'}, half))
			return int64(n1 + n2), true, err
		}})
	defer srv.Close()

	resp, err := http.Get(blockURL(addrOf(t, srv.URL), store.BlockKey{Chunk: 1, Block: 0}))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	// Read exactly the first half; this must succeed while the source is still
	// blocked on `release` (i.e. before the full block exists anywhere).
	first := make([]byte, half)
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp.Body, first)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first half read: %v", err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("timed out waiting for the first half: response was buffered, not cut-through")
	}
	if !bytes.Equal(first, bytes.Repeat([]byte{'a'}, half)) {
		t.Errorf("first half content mismatch")
	}

	// Now let the rest flow and verify the tail.
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("rest read: %v", err)
	}
	if !bytes.Equal(rest, bytes.Repeat([]byte{'b'}, half)) {
		t.Errorf("second half content mismatch (%d bytes)", len(rest))
	}
}

// TestClientRequestTimeout: a peer that is alive but stalled must not hold a
// request open indefinitely — the per-request bound cuts it loose. Without this,
// one stalled node would block its whole subtree in the distribution tree.
func TestClientRequestTimeout(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked // never respond until the test releases us
	}))
	defer func() { close(blocked); srv.Close() }()

	c := NewClient()
	c.Timeout = 100 * time.Millisecond
	key := store.BlockKey{Chunk: 1, Block: 0}

	start := time.Now()
	if _, _, err := c.Get(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: key}); err == nil {
		t.Error("Get against a stalled peer should fail on timeout")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Get took %v; the per-request timeout did not apply", elapsed)
	}

	start = time.Now()
	if _, _, err := c.Stream(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: key}, io.Discard); err == nil {
		t.Error("Stream against a stalled peer should fail on timeout")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Stream took %v; the per-request timeout did not apply", elapsed)
	}
}

// TestClientTimeoutDisabled: a negative Timeout defers entirely to the caller's
// context.
func TestClientTimeoutDisabled(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer func() { close(blocked); srv.Close() }()

	c := NewClient()
	c.Timeout = -1 // no client-side bound
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, _, err := c.Get(ctx, addrOf(t, srv.URL), BlockRequest{Key: store.BlockKey{Chunk: 1}}); err == nil {
		t.Error("expected the caller's context deadline to apply")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("took %v; caller context was not honored", elapsed)
	}
}

// TestStreamRelayChain wires two StreamServers so the downstream one relays from
// the upstream one, and checks the bytes arrive intact through both hops.
func TestStreamRelayChain(t *testing.T) {
	upstreamStore := newStore(t)
	key := store.BlockKey{Chunk: 42, Block: 0}
	want := bytes.Repeat([]byte{0x5E}, 700)
	_ = upstreamStore.Put(key, want)

	up := httptest.NewServer(&StreamServer{NodeID: "UP", Src: StoreStreamSource(upstreamStore)})
	defer up.Close()
	upAddr := strings.TrimPrefix(up.URL, "http://")

	var relayed atomic.Int64
	mid := httptest.NewServer(&StreamServer{NodeID: "MID",
		Src: func(ctx context.Context, req BlockRequest, w io.Writer, _ func(int64)) (int64, bool, error) {
			n, held, err := NewClient().Stream(ctx, upAddr, req, w) // cut-through relay
			relayed.Add(n)
			return n, held, err
		}})
	defer mid.Close()

	var buf bytes.Buffer
	n, held, err := NewClient().Stream(context.Background(), strings.TrimPrefix(mid.URL, "http://"),
		BlockRequest{Key: key}, &buf)
	if err != nil || !held {
		t.Fatalf("relay stream held=%v err=%v", held, err)
	}
	if n != int64(len(want)) || !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("relayed %d bytes, want %d (equal=%v)", n, len(want), bytes.Equal(buf.Bytes(), want))
	}
	if relayed.Load() != int64(len(want)) {
		t.Errorf("mid relayed %d bytes, want %d", relayed.Load(), len(want))
	}
}
