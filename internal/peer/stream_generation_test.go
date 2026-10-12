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

func TestClientLateSuccessCannotClearProtocolQuarantine(t *testing.T) {
	for _, mode := range []string{"stream", "get", "roster"} {
		for _, afterCooldown := range []bool{false, true} {
			name := mode + "/open"
			if afterCooldown {
				name = mode + "/half-open"
			}
			t.Run(name, func(t *testing.T) {
				oldStarted, probeStarted := make(chan struct{}), make(chan struct{})
				oldRelease, probeRelease := make(chan struct{}), make(chan struct{})
				var oldOnce, probeOnce sync.Once
				releaseOld := func() { oldOnce.Do(func() { close(oldRelease) }) }
				releaseProbe := func() { probeOnce.Do(func() { close(probeRelease) }) }
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.URL.Path == RosterPath || strings.HasSuffix(r.URL.Path, "/1"):
						close(oldStarted)
						<-oldRelease
					case strings.HasSuffix(r.URL.Path, "/2"):
						close(probeStarted)
						<-probeRelease
					default:
						_, _ = io.WriteString(w, "!") // clean EOF, invalid geometry
						return
					}
					if r.URL.Path == RosterPath {
						_, _ = io.WriteString(w, `{"epoch":"1","members":[]}`)
					} else {
						_, _ = io.WriteString(w, "abc")
					}
				}))
				defer srv.Close()
				defer releaseOld()
				defer releaseProbe()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				clk := newBrkClock()
				client := NewClient()
				client.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Minute, Now: clk.now})
				addr := strings.TrimPrefix(srv.URL, "http://")
				oldDone := make(chan error, 1)
				go func() {
					req := BlockRequest{Key: store.BlockKey{Block: 1}}
					var err error
					switch mode {
					case "get":
						_, _, err = client.Get(ctx, addr, req)
					case "roster":
						_, _, err = client.FetchRoster(ctx, addr, "", "")
					default:
						_, _, err = client.Stream(ctx, addr, req, io.Discard)
					}
					oldDone <- err
				}()
				wait := func(ch <-chan struct{}) {
					t.Helper()
					select {
					case <-ch:
					case <-ctx.Done():
						t.Fatal("request did not reach its barrier")
					}
				}
				wait(oldStarted)
				invalid := errors.New("wrong geometry")
				_, _, err := client.StreamValidated(ctx, addr, BlockRequest{}, io.Discard, func(context.Context, int64) error { return invalid })
				if !errors.Is(err, invalid) || client.Breaker.State(addr) != BreakerOpen {
					t.Fatalf("invalid stream = %v, state=%v; want a quarantine", err, client.Breaker.State(addr))
				}
				wantState := BreakerOpen
				probeDone := make(chan error, 1)
				if afterCooldown {
					clk.advance(2 * time.Minute)
					go func() {
						_, _, err := client.Stream(ctx, addr, BlockRequest{Key: store.BlockKey{Block: 2}}, io.Discard)
						probeDone <- err
					}()
					wait(probeStarted)
					wantState = BreakerHalfOpen
				}
				// Complete the old successful request only after quarantine (or
				// after a new half-open probe is already outstanding).
				releaseOld()
				if err := <-oldDone; err != nil {
					t.Fatal(err)
				}
				if got := client.Breaker.State(addr); got != wantState {
					t.Fatalf("late %s success changed circuit to %v, want %v", mode, got, wantState)
				}
				_, _, err = client.Stream(ctx, addr, BlockRequest{Key: store.BlockKey{Block: 3}}, io.Discard)
				if !errors.Is(err, ErrCircuitOpen) {
					t.Fatalf("late completion freed the open/half-open gate: %v", err)
				}
				if afterCooldown {
					// A current-generation successful probe still recovers.
					releaseProbe()
					if err := <-probeDone; err != nil || client.Breaker.State(addr) != BreakerClosed {
						t.Fatalf("current probe did not recover circuit: %v", err)
					}
				}
			})
		}
	}
}

func TestClientLateFailureCannotUndoRecovery(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/1") {
			close(started)
			<-release
			http.Error(w, "old request failed", http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "abc")
	}))
	defer srv.Close()
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clk := newBrkClock()
	client := NewClient()
	client.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 1, Cooldown: time.Minute, Now: clk.now})
	addr := strings.TrimPrefix(srv.URL, "http://")
	done := make(chan error, 1)
	go func() {
		_, _, err := client.Get(ctx, addr, BlockRequest{Key: store.BlockKey{Block: 1}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("old request did not start")
	}
	client.Breaker.RecordHardFailure(addr)
	clk.advance(2 * time.Minute)
	if _, _, err := client.Stream(ctx, addr, BlockRequest{}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if client.Breaker.State(addr) != BreakerClosed {
		t.Fatal("current half-open probe did not recover")
	}
	unblock()
	if err := <-done; err == nil {
		t.Fatal("expected the old request's HTTP failure")
	}
	if state := client.Breaker.State(addr); state != BreakerClosed {
		t.Fatalf("old failure reopened recovered circuit: %v", state)
	}
}

func TestStreamValidatedUsesRequestContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "abc")
	}))
	defer srv.Close()
	client := NewClient()
	client.Timeout = time.Minute
	called := false
	n, held, err := client.StreamValidated(context.Background(), strings.TrimPrefix(srv.URL, "http://"), BlockRequest{}, io.Discard,
		func(ctx context.Context, n int64) error {
			called = true
			if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > client.Timeout {
				t.Error("validation did not receive the bounded peer request context")
			}
			if n != 3 {
				t.Errorf("validator byte count = %d, want 3", n)
			}
			return nil
		})
	if err != nil || !held || n != 3 || !called {
		t.Fatalf("validated stream = %d, %v, %v, called=%v", n, held, err, called)
	}
}
