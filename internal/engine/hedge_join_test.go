package engine

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/peer"
)

type hedgeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f hedgeRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHedgedFetchCancelsAndJoinsContenders(t *testing.T) {
	for _, winner := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller-cancel", true: "hedge-win"}[winner], func(t *testing.T) {
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			content := blob(16)
			cli := peer.NewClient()
			cli.HTTP = &http.Client{Transport: hedgeRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host == "slow" {
					close(started)
					<-r.Context().Done()
					close(canceled)
					<-release
					return nil, r.Context().Err()
				}
				return &http.Response{StatusCode: 200, ContentLength: 16, Body: io.NopCloser(bytes.NewReader(content)), Header: make(http.Header)}, nil
			})}
			e := &Engine{peer: cli, latency: newLatencyEstimator(), hedges: newHedgeLimiter(1), hedgeEnabled: true}
			for i := 0; i < 32; i++ {
				e.latency.Observe(time.Microsecond)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan bool, 1)
			backup := ""
			if winner {
				backup = "fast"
			}
			go func() {
				data, ok := e.fetchHedged(ctx, "slow", backup, peer.BlockRequest{}, 16)
				done <- ok && bytes.Equal(data, content)
			}()
			waitBlockEvent(t, ctx, started)
			if !winner {
				cancel()
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("loser was not canceled")
			}
			select {
			case <-done:
				t.Fatal("returned before contender exited")
			default:
			}
			unblock()
			if got := <-done; got != winner {
				t.Fatalf("winner=%v got=%v", winner, got)
			}
		})
	}
}
