package peer

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/store"
)

type unavailableSource struct{}

func (unavailableSource) Error() string  { return "origin disconnected" }
func (unavailableSource) SourceFailure() {}

func TestSourceFailureDoesNotQuarantineRelay(t *testing.T) {
	srv := httptest.NewServer(&Server{Src: func(context.Context, BlockRequest) ([]byte, bool, error) { return nil, false, unavailableSource{} }})
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 1})
	_, held, err := c.Get(context.Background(), addr, BlockRequest{Key: store.BlockKey{Chunk: 1}})
	if held || !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("unexpected result %v %v", held, err)
	}
	if c.Breaker.State(addr) != BreakerClosed {
		t.Fatal("honest relay quarantined")
	}
}
