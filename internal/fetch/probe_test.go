package fetch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOriginKey(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"HTTP://User:pass@EXAMPLE.com/a%2Fb?Signature=secret#fragment", "http://example.com/a%2Fb"},
		{"http://example.com/a/b", "http://example.com/a/b"},
		{"http://example.com/a%1Fb?Signature=renewed", "http://example.com/a%1Fb"},
		{"http://example.com/a%251Fb", "http://example.com/a%251Fb"},
		{"http://example.com/path?", "http://example.com/path"},
		{"/relative%2Fpath?secret=x#frag", "/relative%2Fpath"},
		{"bad%escape?secret=x#frag", "bad%escape"},
		{"bad%escape", "bad%escape"},
	} {
		if got := OriginKey(tc.raw); got != tc.want {
			t.Errorf("OriginKey(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

type probeGateFetcher struct {
	started chan string
	release chan struct{}
}

func (f *probeGateFetcher) Fetch(ctx context.Context, raw string, start, end int64) (Range, error) {
	f.started <- raw
	select {
	case <-f.release:
		return Range{Data: []byte("x"), Total: 4}, nil
	case <-ctx.Done():
		return Range{}, ctx.Err()
	}
}

func TestCoalescingProbeSeparatesEndpointsAndContent(t *testing.T) {
	f := &probeGateFetcher{started: make(chan string, 3), release: make(chan struct{})}
	c := &Coalescing{F: f, Key: func(string) string { return "same-digest" }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer close(f.release)
	var wg sync.WaitGroup
	for _, raw := range []string{"http://a/blob", "http://b/blob"} {
		wg.Add(1)
		go func(raw string) {
			defer wg.Done()
			_, _ = c.Probe(ctx, raw)
		}(raw)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.Fetch(ctx, "http://a/blob", 0, 0)
	}()
	// All three flights must reach the fetcher before any is released:
	// neither another endpoint nor a one-byte content read is a probe match.
	for i := 0; i < 3; i++ {
		select {
		case <-f.started:
		case <-ctx.Done():
			t.Fatal("endpoint/content flights were incorrectly coalesced")
		}
	}
	cancel()
	wg.Wait()
}

// waitContext signals when a caller reaches its context-aware flight wait.
// That lets the test release the origin only after the joiner has arrived.
type waitContext struct {
	context.Context
	waited chan struct{}
	once   sync.Once
}

func (c *waitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waited) })
	return c.Context.Done()
}

func TestCoalescingProbeDedupsSignatures(t *testing.T) {
	f := &probeGateFetcher{started: make(chan string, 2), release: make(chan struct{})}
	c := &Coalescing{F: f, Key: func(string) string { return "unused-for-probes" }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(f.release) }) }
	defer release()
	done := make(chan Range, 2)
	go func() {
		r, _ := c.Probe(ctx, "http://origin/blob?Signature=first")
		done <- r
	}()
	select {
	case raw := <-f.started:
		if !strings.Contains(raw, "Signature=") {
			t.Fatal("the actual upstream request lost its credential")
		}
	case <-ctx.Done():
		t.Fatal("probe did not start")
	}
	joiner := &waitContext{Context: ctx, waited: make(chan struct{})}
	go func() {
		r, _ := c.Probe(joiner, "http://origin/blob?Signature=renewed")
		done <- r
	}()
	select {
	case <-joiner.waited:
	case <-ctx.Done():
		t.Fatal("joiner did not start")
	}
	release()
	first := <-done
	second := <-done
	if first.Total != 4 || second.Total != 4 {
		t.Fatal("probe did not preserve metadata")
	}
	if first.Coalesced == second.Coalesced {
		t.Fatal("expected one probe leader and one joiner")
	}
	if len(f.started) != 0 {
		t.Fatal("refreshed signatures did not coalesce")
	}
}

func TestCoalescingProbeComposes(t *testing.T) {
	f := &probeGateFetcher{started: make(chan string, 2), release: make(chan struct{})}
	inner := &Coalescing{F: f, Key: func(string) string { return "same-digest" }}
	outer := &Coalescing{F: inner}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer close(f.release)
	var wg sync.WaitGroup
	for _, raw := range []string{"http://a/blob", "http://b/blob"} {
		wg.Add(1)
		go func(raw string) {
			defer wg.Done()
			_, _ = outer.Probe(ctx, raw)
		}(raw)
	}
	// Both endpoints must reach origin while neither is released. Calling the
	// inner Fetch instead of Probe joins its content-keyed flight and stalls.
	seen := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case raw := <-f.started:
			seen[raw] = true
		case <-ctx.Done():
			t.Fatal("nested content coalescing merged endpoint probes")
		}
	}
	if len(seen) != 2 {
		t.Fatalf("nested probes contacted %v, want both endpoints", seen)
	}
	cancel()
	wg.Wait()
}

type probeOnlyFetcher struct {
	probe func(context.Context, string) (Range, error)
}

func (f *probeOnlyFetcher) Fetch(context.Context, string, int64, int64) (Range, error) {
	return Range{}, errors.New("probe delegated to content fetch")
}

func (f *probeOnlyFetcher) Probe(ctx context.Context, raw string) (Range, error) {
	return f.probe(ctx, raw)
}

func TestCoalescingProbeDelegatesRefusalRetry(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	f := &probeOnlyFetcher{probe: func(ctx context.Context, raw string) (Range, error) {
		if strings.Contains(raw, "Signature=expired") {
			close(started)
			select {
			case <-release:
				return Range{}, &StatusError{Code: 403}
			case <-ctx.Done():
				return Range{}, ctx.Err()
			}
		}
		if !strings.Contains(raw, "Signature=renewed") {
			return Range{}, errors.New("retry lost the caller's credential")
		}
		return Range{Data: []byte("x"), Total: 4}, nil
	}}
	c := &Coalescing{F: f}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leader := make(chan error, 1)
	go func() {
		_, err := c.Probe(ctx, "http://origin/blob?Signature=expired")
		leader <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("leader did not delegate to the underlying Prober")
	}
	joiner := &waitContext{Context: ctx, waited: make(chan struct{})}
	done := make(chan struct{})
	var result Range
	var resultErr error
	go func() {
		result, resultErr = c.Probe(joiner, "http://origin/blob?Signature=renewed")
		close(done)
	}()
	select {
	case <-joiner.waited:
	case <-ctx.Done():
		t.Fatal("renewed credential did not join the endpoint probe")
	}
	unblock()
	if err := <-leader; !refused(err) {
		t.Fatalf("expired leader returned %v, want credential refusal", err)
	}
	<-done
	if resultErr != nil || result.Total != 4 || result.Coalesced {
		t.Fatalf("delegated retry = %+v, %v; want own successful probe", result, resultErr)
	}
}
