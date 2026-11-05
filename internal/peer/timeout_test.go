package peer

import (
	"context"
	"github.com/data-accelerator/dart/internal/store"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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
