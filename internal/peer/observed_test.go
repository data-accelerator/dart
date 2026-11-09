package peer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestGetIntoObservedDispatch(t *testing.T) {
	for _, status := range []int{200, 404, 503} {
		var notified, served atomic.Int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			served.Add(1)
			if notified.Load() != 1 {
				t.Error("dispatch notification did not precede HTTP request")
			}
			w.WriteHeader(status)
			if status == 200 {
				_, _ = w.Write([]byte("data"))
			}
		}))
		c := NewClient()
		_, held, err := c.GetIntoObserved(context.Background(), s.Listener.Addr().String(), BlockRequest{}, make([]byte, 4), nil, func() { notified.Add(1) })
		s.Close()
		if notified.Load() != 1 || served.Load() != 1 || held != (status == 200) || (err != nil) != (status == 503) {
			t.Fatal(status, notified.Load(), served.Load(), held, err)
		}
	}
	for _, mode := range []string{"breaker", "invalid", "canceled", "transport"} {
		t.Run(mode, func(t *testing.T) {
			c := NewClient()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			addr := "peer:1"
			var calls, notified int
			c.HTTP = &http.Client{Transport: boundRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if notified != 1 {
					t.Error("transport started before notification")
				}
				return nil, errors.New("transport unavailable")
			})}
			switch mode {
			case "breaker":
				c.Breaker = NewBreaker(BreakerOptions{})
				c.Breaker.RecordHardFailure(addr)
			case "invalid":
				addr = "bad\naddress"
			case "canceled":
				cancel()
			}
			_, _, err := c.GetIntoObserved(ctx, addr, BlockRequest{}, make([]byte, 4), nil, func() { notified++ })
			want := 0
			if mode == "transport" {
				want = 1
			}
			if err == nil || calls != want || notified != want {
				t.Fatal(err, calls, notified)
			}
		})
	}
}
