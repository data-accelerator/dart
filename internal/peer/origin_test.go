package peer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOriginDelegationPreservesResponseAndDoesNotFollowRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RequestURI != "/a%2Fb?sig=a%2Fb" || r.Header.Get("Accept") != "application/test" || r.Header.Get(HeaderOrigin) != "" {
			t.Errorf("origin request lost identity or retained its routing envelope: %s %v", r.RequestURI, r.Header)
		}
		w.Header().Set("Location", target.URL)
		w.Header().Set("ETag", "test-tag")
		w.WriteHeader(http.StatusFound)
		_, _ = io.WriteString(w, "redirect")
	}))
	defer origin.Close()
	root := httptest.NewServer(&OriginServer{Transport: http.DefaultTransport})
	defer root.Close()
	req, _ := http.NewRequest(http.MethodGet, origin.URL+"/a%2Fb?sig=a%2Fb", nil)
	req.Header.Set("Accept", "application/test")
	resp, err := NewClient().RoundTripOrigin(req, root.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != "redirect" || resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != target.URL || resp.Header.Get("ETag") != "test-tag" || resp.Header.Get(HeaderOriginResponse) != "" || targetCalls.Load() != 0 {
		t.Fatalf("redirect was followed or response changed: %+v %q %v hits=%d", resp, data, err, targetCalls.Load())
	}
}

func TestOriginDelegationRejectsUnsupportedOperations(t *testing.T) {
	for _, c := range []struct {
		method, origin string
		transport      http.RoundTripper
		status         int
	}{
		{http.MethodPost, "http://source/file", http.DefaultTransport, 405},
		{http.MethodGet, "file:///secret", http.DefaultTransport, 400},
		{http.MethodGet, "http://source/file", nil, 503},
	} {
		r := httptest.NewRequest(c.method, OriginPath, nil)
		r.Header.Set(HeaderOrigin, c.origin)
		w := httptest.NewRecorder()
		(&OriginServer{Transport: c.transport}).ServeHTTP(w, r)
		if w.Code != c.status {
			t.Fatalf("%s %s: %d", c.method, c.origin, w.Code)
		}
	}
}

func TestOriginDelegationTruncationDoesNotQuarantineRoot(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "prefix")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	defer origin.Close()
	root := httptest.NewServer(&OriginServer{Transport: http.DefaultTransport})
	defer root.Close()
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 1})
	addr := root.Listener.Addr().String()
	req, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	resp, err := c.RoundTripOrigin(req, addr)
	if err == nil {
		defer resp.Body.Close()
		_, err = io.ReadAll(resp.Body)
	}
	if err == nil {
		t.Fatal("truncated chunked origin response appeared complete")
	}
	if c.Breaker.State(addr) != BreakerClosed {
		t.Fatal("honest supply root quarantined for origin truncation")
	}
}

func TestOriginDelegationUsesIdleInsteadOfWholeStreamDeadline(t *testing.T) {
	release := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "b")
		case <-r.Context().Done():
		}
	}))
	defer origin.Close()
	root := httptest.NewServer(&OriginServer{Transport: http.DefaultTransport})
	defer root.Close()
	c := NewClient()
	c.Timeout = 200 * time.Millisecond
	req, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	resp, err := c.RoundTripOrigin(req, root.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b := make([]byte, 1)
	if _, err := io.ReadFull(resp.Body, b); err != nil || string(b) != "a" {
		t.Fatal(err)
	}
	// A downstream consumer may be slow. Only time blocked in an upstream
	// read is bounded; total object duration and time between reads are not.
	time.Sleep(3 * c.Timeout)
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != "b" {
		t.Fatalf("stream lifetime capped: %q %v", rest, err)
	}
}

func TestOriginDelegationCancelsStalledBody(t *testing.T) {
	done := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(done)
	}))
	defer origin.Close()
	root := httptest.NewServer(&OriginServer{Transport: http.DefaultTransport})
	defer root.Close()
	c := NewClient()
	c.Timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin.URL, nil)
	resp, err := c.RoundTripOrigin(req, root.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("idle timeout did not fire before caller deadline: %v", err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("root origin request survived canceled consumer")
	}
}

func TestOriginDelegationBoundsHeaderWait(t *testing.T) {
	done := make(chan struct{})
	root := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(done)
	}))
	defer root.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c := NewClient()
	c.Timeout = 100 * time.Millisecond
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 1})
	addr := root.Listener.Addr().String()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://origin/file", nil)
	if resp, err := c.RoundTripOrigin(req, addr); err == nil {
		resp.Body.Close()
		t.Fatal("silent root returned a response")
	} else if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("header wait was not bounded independently: %v", err)
	}
	if c.Breaker.State(addr) != BreakerOpen {
		t.Fatal("local timeout was misclassified as caller cancellation")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("header timeout did not cancel the root request")
	}
}

// Keep response refusals terminal, rather than treating an origin's 503 as a
// root transport failure and needlessly trying every supply peer.
func TestOriginDelegationPreservesOriginRefusal(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "source denied", 503) }))
	defer origin.Close()
	root := httptest.NewServer(&OriginServer{Transport: http.DefaultTransport})
	defer root.Close()
	req, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	resp, err := NewClient().RoundTripOrigin(req, root.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 503 || !strings.Contains(string(b), "source denied") {
		t.Fatalf("refusal lost: %d %q %v", resp.StatusCode, b, err)
	}
}
