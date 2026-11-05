package peer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
)

type boundRoundTripFunc func(*http.Request) (*http.Response, error)

func (f boundRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type excessPeerBody struct{ read, closed int }

func (b *excessPeerBody) Read(p []byte) (int, error) {
	if b.read >= 1<<20 {
		return 0, io.EOF
	}
	n := len(p)
	if n > 1<<20-b.read {
		n = 1<<20 - b.read
	}
	b.read += n
	return n, nil
}
func (b *excessPeerBody) Close() error { b.closed++; return nil }
func TestGetValidatedBoundsPeerBody(t *testing.T) {
	for _, length := range []int64{-1, 1 << 30} {
		t.Run(map[bool]string{false: "chunked", true: "advertised"}[length > 0], func(t *testing.T) {
			body := &excessPeerBody{}
			c := NewClient()
			c.Breaker = NewBreaker(BreakerOptions{})
			c.HTTP = &http.Client{Transport: boundRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, ContentLength: length, Body: body, Header: make(http.Header)}, nil
			})}
			called := false
			data, held, err := c.GetValidated(context.Background(), "parent", BlockRequest{}, 16, func(context.Context, int64) error { called = true; return nil })
			if !errors.Is(err, ErrBlockLength) || held || data != nil || called || body.read > 17 || body.closed != 1 || c.Breaker.State("parent") != BreakerOpen {
				t.Fatalf("bound result %d %v %v; read=%d closed=%d called=%v", len(data), held, err, body.read, body.closed, called)
			}
			if length > 16 && body.read != 0 {
				t.Fatal("read oversized advertised body")
			}
		})
	}
	c := NewClient()
	if _, _, err := c.GetValidated(context.Background(), "unused", BlockRequest{}, -1, nil); err == nil {
		t.Fatal("negative expected size accepted")
	}
}
