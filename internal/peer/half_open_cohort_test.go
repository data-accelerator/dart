package peer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

func TestHalfOpenProbesWaitForSiblingOutcome(t *testing.T) {
	for _, invalidSibling := range []bool{false, true} {
		name := "all-success"
		if invalidSibling {
			name = "hard-failure"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/1") {
					close(started)
					<-release
					if invalidSibling {
						_, _ = io.WriteString(w, "!")
						return
					}
				}
				_, _ = io.WriteString(w, "abc")
			}))
			defer srv.Close()
			defer unblock()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			clk := newBrkClock()
			client := NewClient()
			client.Breaker = NewBreaker(BreakerOptions{HalfOpenProbes: 2, Cooldown: time.Minute, Now: clk.now})
			addr := strings.TrimPrefix(srv.URL, "http://")
			client.Breaker.RecordHardFailure(addr)
			clk.advance(2 * time.Minute)
			invalid := errors.New("wrong geometry")
			done := make(chan error, 1)
			go func() {
				_, _, err := client.StreamValidated(ctx, addr, BlockRequest{Key: store.BlockKey{Block: 1}}, io.Discard,
					func(_ context.Context, n int64) error {
						if n != 3 {
							return invalid
						}
						return nil
					})
				done <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("sibling probe never started")
			}
			if data, held, err := client.Get(ctx, addr, BlockRequest{Key: store.BlockKey{Block: 2}}); err != nil || !held || string(data) != "abc" {
				t.Fatalf("first successful probe = %q, %v, %v", data, held, err)
			}
			if state := client.Breaker.State(addr); state != BreakerHalfOpen {
				t.Fatalf("first success discarded its sibling: state=%v, want half-open", state)
			}
			// Freeze the existing probe cohort after a success; otherwise new
			// probes could keep joining and prevent the cohort from draining.
			_, _, err := client.Get(ctx, addr, BlockRequest{Key: store.BlockKey{Block: 3}})
			if !errors.Is(err, ErrCircuitOpen) {
				t.Fatalf("new request joined a recovering cohort: %v", err)
			}
			unblock()
			err = <-done
			want := BreakerClosed
			if invalidSibling {
				want = BreakerOpen
				if !errors.Is(err, invalid) {
					t.Fatalf("sibling protocol result = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if state := client.Breaker.State(addr); state != want {
				t.Fatalf("cohort completed in state=%v, want %v", state, want)
			}
		})
	}
}

func TestHalfOpenRosterSuccessCannotConsumeDataProbe(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == RosterPath {
			_, _ = io.WriteString(w, `{"epoch":"1","members":[]}`)
			return
		}
		close(started)
		<-release
		_, _ = io.WriteString(w, "!")
	}))
	defer srv.Close()
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clk := newBrkClock()
	client := NewClient()
	client.Breaker = NewBreaker(BreakerOptions{Cooldown: time.Minute, Now: clk.now})
	addr := strings.TrimPrefix(srv.URL, "http://")
	client.Breaker.RecordHardFailure(addr)
	clk.advance(2 * time.Minute)
	invalid := errors.New("wrong geometry")
	done := make(chan error, 1)
	go func() {
		_, _, err := client.StreamValidated(ctx, addr, BlockRequest{}, io.Discard, func(context.Context, int64) error { return invalid })
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("data probe never started")
	}
	if _, _, err := client.FetchRoster(ctx, addr, "", ""); err != nil {
		t.Fatal(err)
	}
	if state := client.Breaker.State(addr); state != BreakerHalfOpen {
		t.Fatalf("roster success consumed the data probe: %v", state)
	}
	unblock()
	if err := <-done; !errors.Is(err, invalid) || client.Breaker.State(addr) != BreakerOpen {
		t.Fatalf("pending protocol failure was lost: %v, state=%v", err, client.Breaker.State(addr))
	}
}
