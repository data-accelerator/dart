package peer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
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

func TestGetIntoObservedClientTimeoutBeforeDispatch(t *testing.T) {
	var requests, notified atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer s.Close()
	now := time.Unix(1000, 0)
	b := NewBreaker(BreakerOptions{Cooldown: time.Second, Now: func() time.Time { return now }})
	addr := s.Listener.Addr().String()
	b.RecordHardFailure(addr)
	now = now.Add(time.Second)
	c := NewClient()
	c.Breaker = b
	// The client-owned deadline has elapsed before HTTP dispatch, even though
	// the original caller remains live. This must not prove peer recovery.
	c.Timeout = time.Nanosecond
	caller := context.Background()
	_, _, err := c.GetIntoObserved(caller, addr, BlockRequest{}, make([]byte, 4), nil, func() { notified.Add(1) })
	if !errors.Is(err, context.DeadlineExceeded) || caller.Err() != nil {
		t.Fatal("wrong deadline source", err, caller.Err())
	}
	if requests.Load() != 0 || notified.Load() != 0 || b.State(addr) != BreakerOpen {
		t.Fatal("undispatched client timeout claimed recovery", requests.Load(), notified.Load(), b.State(addr))
	}
}
