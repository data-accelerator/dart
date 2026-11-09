package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/data-accelerator/dart/internal/store"
)

func TestBlockRoutingEpochWire(t *testing.T) {
	var got BlockRequest
	source := &Server{Src: func(_ context.Context, r BlockRequest) ([]byte, bool, error) { got = r; return []byte("ok"), true, nil }}
	srv := httptest.NewServer(source)
	defer srv.Close()
	for _, req := range []BlockRequest{{}, {HasEpoch: true}, {HasEpoch: true, Epoch: ^uint64(0)}} {
		req.Key = store.BlockKey{Chunk: 42}
		data, held, err := NewClient().Get(context.Background(), srv.Listener.Addr().String(), req)
		if err != nil || !held || string(data) != "ok" || got.Epoch != req.Epoch || got.HasEpoch != req.HasEpoch {
			t.Fatal("epoch lost", got, err)
		}
	}
	for _, raw := range []string{"-1", "not-a-number", "18446744073709551616"} {
		req := httptest.NewRequest("GET", "/peer/v2/block/2a/0", nil)
		req.Header.Set(HeaderEpoch, raw)
		rec := httptest.NewRecorder()
		source.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatal("accepted malformed epoch", raw, rec.Code)
		}
	}
}
