package fetch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCoalescingLastCallerCancelsAndJoins(t *testing.T) {
	for _, nestedProbe := range []bool{false, true} {
		t.Run(map[bool]string{false: "fetch", true: "nested-probe"}[nestedProbe], func(t *testing.T) {
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			f := fetcherFunc(func(ctx context.Context, _ string, _, _ int64) (Range, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release
				return Range{}, ctx.Err()
			})
			c := &Coalescing{F: f}
			if nestedProbe {
				c = &Coalescing{F: c}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if nestedProbe {
					_, err = c.Probe(ctx, "http://origin/x")
				} else {
					_, err = c.Fetch(ctx, "http://origin/x", 0, 15)
				}
				done <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("backend did not start")
			}
			cancel()
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("last caller did not cancel backend")
			}
			select {
			case err := <-done:
				t.Fatalf("returned before backend exit: %v", err)
			default:
			}
			unblock()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestCoalescingAlreadyCanceledStartsNoFlight(t *testing.T) {
	c := &Coalescing{F: fetcherFunc(func(context.Context, string, int64, int64) (Range, error) {
		t.Error("canceled caller started work")
		return Range{}, nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Fetch(ctx, "u", 0, 15); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(c.g.m) != 0 {
		t.Fatal("canceled caller retained a flight")
	}
}
