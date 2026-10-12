package peer

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStreamServerAbortsCommittedErrors(t *testing.T) {
	for _, sized := range []bool{false, true} {
		name := "chunked"
		if sized {
			name = "content-length"
		}
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(&StreamServer{Src: func(_ context.Context, _ BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
				if sized {
					sizer(6)
				}
				n, err := io.WriteString(w, "abc")
				if err != nil {
					return int64(n), true, err
				}
				return int64(n), true, errors.New("source interrupted")
			}})
			defer srv.Close()
			var body strings.Builder
			n, held, err := NewClient().Stream(context.Background(), strings.TrimPrefix(srv.URL, "http://"), BlockRequest{}, &body)
			if n != 3 || !held || !errors.Is(err, io.ErrUnexpectedEOF) || body.String() != "abc" {
				t.Fatalf("Stream = %d, %v, %v, body=%q; want partial bytes and unexpected EOF", n, held, err, body.String())
			}
		})
	}
}
