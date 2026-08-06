//go:build unix

package peer

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

// closedPort returns an address that refuses connections, so a dial fails
// definitively rather than hanging.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// TestClassifyDialFailureIsHard: a dial failure is definitive evidence the peer
// is unusable, which is what lets the breaker open on one observation instead of
// spending its whole budget one dial timeout at a time.
func TestClassifyDialFailureIsHard(t *testing.T) {
	c := NewClient()
	_, _, err := c.Get(context.Background(), closedPort(t), BlockRequest{Key: store.BlockKey{Chunk: 1}})
	if err == nil {
		t.Fatal("expected a dial failure against a closed port")
	}
	if got := classify(err); got != outcomeHardFail {
		t.Errorf("classify(dial error) = %v, want outcomeHardFail", got)
	}

	// A reachable peer answering badly is only a soft failure: it may recover.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	_, _, err = c.Get(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: store.BlockKey{Chunk: 1}})
	if err == nil {
		t.Fatal("expected an error for a 500")
	}
	if got := classify(err); got != outcomeSoftFail {
		t.Errorf("classify(500) = %v, want outcomeSoftFail", got)
	}
	if got := classify(nil); got != outcomeAnswered {
		t.Errorf("classify(nil) = %v, want outcomeAnswered", got)
	}
}

// TestBreakerOpensOnFirstHardFailure is the fix for abrupt node death: a peer we
// cannot connect to is routed around after ONE attempt, not after the full
// failure threshold.
func TestBreakerOpensOnFirstHardFailure(t *testing.T) {
	dead := closedPort(t)
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 5, Cooldown: time.Hour})

	if _, _, err := c.Get(context.Background(), dead, BlockRequest{Key: store.BlockKey{Chunk: 1}}); err == nil {
		t.Fatal("expected the dial to fail")
	}
	if got := c.Breaker.State(dead); got != BreakerOpen {
		t.Fatalf("circuit = %v after one unreachable dial, want open "+
			"(threshold is 5, but a dial failure is definitive)", got)
	}
	// Subsequent attempts must not dial at all.
	_, _, err := c.Get(context.Background(), dead, BlockRequest{Key: store.BlockKey{Chunk: 2}})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("second attempt error = %v, want ErrCircuitOpen", err)
	}
}

// TestBreakerHardFailureRecovers: opening on one observation stays safe because
// it is reversible — the cooldown plus a probe brings the peer back.
func TestBreakerHardFailureRecovers(t *testing.T) {
	clk := newBrkClock()
	b := NewBreaker(BreakerOptions{FailureThreshold: 5, Cooldown: 5 * time.Second, Now: clk.now})
	const addr = "gone:1"

	b.RecordHardFailure(addr)
	if b.State(addr) != BreakerOpen {
		t.Fatal("expected open")
	}
	clk.advance(6 * time.Second)
	if !b.Allow(addr) {
		t.Fatal("expected a half-open probe after the cooldown")
	}
	b.RecordSuccess(addr)
	if got := b.State(addr); got != BreakerClosed {
		t.Errorf("state = %v after a successful probe, want closed", got)
	}
}

// TestSoftFailureStillNeedsThreshold: the fast path is reserved for definitive
// failures; a reachable-but-erroring peer must not be ejected on one hiccup.
func TestSoftFailureStillNeedsThreshold(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	addr := addrOf(t, srv.URL)

	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 3, Cooldown: time.Hour})
	for i := 0; i < 2; i++ {
		_, _, _ = c.Get(context.Background(), addr, BlockRequest{Key: store.BlockKey{Chunk: uint64(i)}})
		if got := c.Breaker.State(addr); got != BreakerClosed {
			t.Fatalf("after %d soft failures state = %v, want closed", i+1, got)
		}
	}
	_, _, _ = c.Get(context.Background(), addr, BlockRequest{Key: store.BlockKey{Chunk: 9}})
	if got := c.Breaker.State(addr); got != BreakerOpen {
		t.Errorf("after reaching the threshold state = %v, want open", got)
	}
}

// TestTransportTimeoutsConfigured: the dial bound is what turns an abruptly dead
// machine from an OS-SYN-retry stall into a fast failure, so it must actually be
// set on the transport.
func TestTransportTimeoutsConfigured(t *testing.T) {
	tr := NewTransport(DefaultDialTimeout, DefaultResponseHeaderTimeout)
	if tr.DialContext == nil {
		t.Error("no DialContext: the dial would fall back to the OS SYN retry schedule")
	}
	if tr.ResponseHeaderTimeout != DefaultResponseHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %v, want %v", tr.ResponseHeaderTimeout, DefaultResponseHeaderTimeout)
	}
	if tr.ForceAttemptHTTP2 {
		t.Error("HTTP/2 must stay off so sendfile/splice remain viable")
	}
	// Non-positive values disable the individual bounds.
	if tr2 := NewTransport(-1, -1); tr2.ResponseHeaderTimeout != 0 {
		t.Errorf("disabled ResponseHeaderTimeout = %v, want 0", tr2.ResponseHeaderTimeout)
	}
	if NewClient().HTTP.Transport == nil {
		t.Error("NewClient must install the tuned transport")
	}
}

// TestResponseHeaderTimeoutBoundsSilentPeer covers the case a dial timeout cannot:
// a peer that accepts the connection (or has one pooled) and then never answers.
// Without the bound this would wait out the whole request timeout.
func TestResponseHeaderTimeoutBoundsSilentPeer(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer func() { close(blocked); srv.Close() }()

	c := &Client{
		HTTP:    &http.Client{Transport: NewTransport(DefaultDialTimeout, 150*time.Millisecond)},
		Timeout: 30 * time.Second, // deliberately long: the header bound must fire first
	}
	start := time.Now()
	_, _, err := c.Get(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: store.BlockKey{Chunk: 1}})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout from a silent peer")
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v: ResponseHeaderTimeout did not bound a silent peer", elapsed)
	}
}

// TestDeadPeerRoutedAroundQuickly is the end-to-end property the fixes exist for:
// after one failed attempt at an unreachable peer, further attempts cost
// essentially nothing.
func TestDeadPeerRoutedAroundQuickly(t *testing.T) {
	dead := closedPort(t)
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{Cooldown: time.Hour})

	// First attempt: pays the dial failure.
	_, _, _ = c.Get(context.Background(), dead, BlockRequest{Key: store.BlockKey{Chunk: 1}})

	var attempts int32
	start := time.Now()
	for i := 0; i < 200; i++ {
		if _, _, err := c.Get(context.Background(), dead, BlockRequest{Key: store.BlockKey{Chunk: uint64(i)}}); errors.Is(err, ErrCircuitOpen) {
			atomic.AddInt32(&attempts, 1)
		}
	}
	elapsed := time.Since(start)
	if atomic.LoadInt32(&attempts) != 200 {
		t.Errorf("%d/200 attempts short-circuited", attempts)
	}
	if elapsed > time.Second {
		t.Errorf("200 short-circuited attempts took %v, want negligible", elapsed)
	}
}
