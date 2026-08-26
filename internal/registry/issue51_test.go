package registry

// Regression tests for issue #51: a cancelled singleflight leader used to
// poison every still-live follower with context.Canceled, because the shared
// token exchange ran on the leader's request context.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stalledTokenTransport stands in for the token endpoint: the exchange stalls
// until release is closed. Like a real transport it honors request-context
// cancellation — that fidelity is what makes the test fail on the old code,
// where the flight ran on the leader's context.
type stalledTokenTransport struct {
	entered   chan struct{} // closed when the first exchange request arrives
	release   chan struct{} // closing lets the exchange complete
	exchanges atomic.Int64
}

func (s *stalledTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.exchanges.Add(1) == 1 {
		close(s.entered)
	}
	select {
	case <-s.release:
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"token":"flight-token","expires_in":300}`)),
		}, nil
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

// TestLeaderCancellationDoesNotPoisonFollowers orchestrates the exact issue
// scenario: the elected leader is cancelled while the token endpoint is
// blocked, followers with live contexts have joined the same flight, and
// every follower must still complete with the flight's token.
//
// Looped: a follower is observably parked on the flight's done channel only
// from inside, so the test relies on a scheduler-yield rendezvous and repeats
// across scheduling windows — the same pattern as
// TestConcurrentColdRequestsShareOneExchange. On the old code the cohort
// either inherits context.Canceled or pays a second exchange, so a round
// fails as soon as any follower was parked before the cancel.
func TestLeaderCancellationDoesNotPoisonFollowers(t *testing.T) {
	for round := 0; round < 25; round++ {
		st := &stalledTokenTransport{entered: make(chan struct{}), release: make(chan struct{})}
		at := NewAuthTransport(st, nil)
		ch := challenge{scheme: "bearer", realm: "http://token.test/token", service: "svc", scope: "repository:x:pull"}
		const key = "token.test|repository:x:pull"

		// The leader leads the exchange; entered proves the in-flight call is
		// published (fetchTokenShared registers it before calling fetchToken).
		leaderCtx, cancelLeader := context.WithCancel(context.Background())
		leaderErr := make(chan error, 1)
		go func() {
			_, _, err := at.fetchTokenShared(leaderCtx, key, ch, Credential{})
			leaderErr <- err
		}()
		<-st.entered

		// Followers with live contexts join the same flight.
		const followers = 4
		var started sync.WaitGroup
		followerErrs := make([]chan error, followers)
		for i := range followerErrs {
			followerErrs[i] = make(chan error, 1)
			started.Add(1)
			go func(out chan<- error) {
				started.Done()
				tok, _, err := at.fetchTokenShared(context.Background(), key, ch, Credential{})
				if err == nil && tok != "flight-token" {
					err = fmt.Errorf("token = %q, want the flight's token", tok)
				}
				out <- err
			}(followerErrs[i])
		}
		started.Wait()
		// Rendezvous: let the followers reach the wait on the flight's done
		// channel before the leader is cancelled.
		for i := 0; i < 100; i++ {
			runtime.Gosched()
		}

		// Cancel the leader mid-exchange. The leader itself must observe its
		// own cancellation...
		cancelLeader()
		select {
		case err := <-leaderErr:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("round %d: leader err = %v, want context.Canceled", round, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: leader did not observe its own cancellation", round)
		}

		// ...but no still-live follower may inherit it: once the endpoint
		// responds, every follower completes with the shared token.
		close(st.release)
		for i, out := range followerErrs {
			select {
			case err := <-out:
				if err != nil {
					t.Fatalf("round %d: follower %d poisoned by the leader's cancellation: %v", round, i, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d: follower %d did not complete after the exchange was released", round, i)
			}
		}

		// The cohort still cost exactly one exchange, and its result is cached.
		if got := st.exchanges.Load(); got != 1 {
			t.Fatalf("round %d: %d token exchanges, want 1 (singleflight intact)", round, got)
		}
		if tok, ok := at.cachedToken(key); !ok || tok != "flight-token" {
			t.Fatalf("round %d: cached token = %q, %v; want the completed flight's token", round, tok, ok)
		}
	}
}

// TestCancelledFollowerDoesNotAbortFlight pins the symmetric direction: a
// follower leaving early must not cancel the shared exchange either — the
// leader and any remaining followers still get the token.
func TestCancelledFollowerDoesNotAbortFlight(t *testing.T) {
	st := &stalledTokenTransport{entered: make(chan struct{}), release: make(chan struct{})}
	at := NewAuthTransport(st, nil)
	ch := challenge{scheme: "bearer", realm: "http://token.test/token", service: "svc", scope: "repository:x:pull"}
	const key = "token.test|repository:x:pull"

	leaderRes := make(chan error, 1)
	go func() {
		tok, _, err := at.fetchTokenShared(context.Background(), key, ch, Credential{})
		if err == nil && tok != "flight-token" {
			err = fmt.Errorf("token = %q, want the flight's token", tok)
		}
		leaderRes <- err
	}()
	<-st.entered

	quitterCtx, cancelQuitter := context.WithCancel(context.Background())
	quitterErr := make(chan error, 1)
	go func() {
		_, _, err := at.fetchTokenShared(quitterCtx, key, ch, Credential{})
		quitterErr <- err
	}()
	cancelQuitter()
	select {
	case err := <-quitterErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("departing follower: err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("departing follower did not observe its own cancellation")
	}

	close(st.release)
	select {
	case err := <-leaderRes:
		if err != nil {
			t.Fatalf("leader failed after a follower left: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not complete after the exchange was released")
	}
	if got := st.exchanges.Load(); got != 1 {
		t.Fatalf("%d token exchanges, want 1", got)
	}
}
