package peer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBreakerNeutralProbeCohorts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcomes []outcome
		states   []BreakerState
	}{
		{"all canceled", []outcome{outcomeNeutral, outcomeNeutral}, []BreakerState{BreakerHalfOpen, BreakerHalfOpen}},
		{"success then cancel", []outcome{outcomeAnswered, outcomeNeutral}, []BreakerState{BreakerHalfOpen, BreakerClosed}},
		{"cancel then success", []outcome{outcomeNeutral, outcomeAnswered}, []BreakerState{BreakerHalfOpen, BreakerClosed}},
		{"failure then cancel", []outcome{outcomeSoftFail, outcomeNeutral}, []BreakerState{BreakerOpen, BreakerOpen}},
		{"cancel then failure", []outcome{outcomeNeutral, outcomeSoftFail}, []BreakerState{BreakerHalfOpen, BreakerOpen}},
		{"success cancel failure", []outcome{outcomeAnswered, outcomeNeutral, outcomeHardFail}, []BreakerState{BreakerHalfOpen, BreakerHalfOpen, BreakerOpen}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := &brkClock{}
			b := NewBreaker(BreakerOptions{HalfOpenProbes: len(tc.outcomes), Now: clk.now})
			b.RecordHardFailure("peer")
			clk.advance(DefaultBreakerCooldown)
			permits := make([]breakerPermit, len(tc.outcomes))
			for i := range permits {
				var ok bool
				permits[i], ok = b.begin("peer", true)
				if !ok {
					t.Fatal("probe refused")
				}
			}
			if _, ok := b.begin("peer", true); ok {
				t.Fatal("probe limit exceeded")
			}
			for i, o := range tc.outcomes {
				b.record("peer", o, &permits[i])
				if got := b.State("peer"); got != tc.states[i] {
					t.Fatalf("completion %d: %v, want %v", i, got, tc.states[i])
				}
				if b.peers["peer"].recovering {
					if _, ok := b.begin("peer", true); ok {
						t.Fatal("recovering cohort admitted another probe")
					}
				}
			}
			if b.peers["peer"].inFlight != 0 {
				t.Fatal("probe slot leaked")
			}
			if b.State("peer") == BreakerHalfOpen {
				if b.peers["peer"].failures != 1 {
					t.Fatal("cancellation erased failure history")
				}
				p, ok := b.begin("peer", true)
				if !ok {
					t.Fatal("canceled cohort prevents real probe")
				}
				b.record("peer", outcomeAnswered, &p)
				if b.State("peer") != BreakerClosed {
					t.Fatal("real answer did not recover")
				}
			}
		})
	}
}

func TestBreakerNeutralStaleCompletion(t *testing.T) {
	clk := &brkClock{}
	b := NewBreaker(BreakerOptions{Now: clk.now})
	b.RecordHardFailure("peer")
	clk.advance(DefaultBreakerCooldown)
	old, _ := b.begin("peer", true)
	b.RecordHardFailure("peer")
	clk.advance(DefaultBreakerCooldown)
	current, _ := b.begin("peer", true)
	b.record("peer", outcomeNeutral, &old)
	if b.State("peer") != BreakerHalfOpen || b.peers["peer"].inFlight != 1 || b.peers["peer"].failures != 2 {
		t.Fatal("stale cancellation altered current quarantine")
	}
	b.record("peer", outcomeAnswered, &current)
	if b.State("peer") != BreakerClosed {
		t.Fatal("current probe could not recover")
	}
	// Entry identity must protect a replacement even with the same generation.
	b = NewBreaker(BreakerOptions{Now: clk.now})
	b.RecordHardFailure("peer")
	clk.advance(DefaultBreakerCooldown)
	old, _ = b.begin("peer", true)
	delete(b.peers, "peer")
	b.RecordHardFailure("peer")
	clk.advance(DefaultBreakerCooldown)
	current, _ = b.begin("peer", true)
	if old.generation != current.generation {
		t.Fatal("test requires matching generations")
	}
	b.record("peer", outcomeNeutral, &old)
	if b.State("peer") != BreakerHalfOpen || b.peers["peer"].inFlight != 1 {
		t.Fatal("cancellation consumed replacement entry's probe")
	}
	b.record("peer", outcomeAnswered, &current)
}

func TestRosterNeutralCannotConsumeDataProbe(t *testing.T) {
	clk := &brkClock{}
	b := NewBreaker(BreakerOptions{Now: clk.now})
	b.RecordHardFailure("peer")
	clk.advance(DefaultBreakerCooldown)
	probe, _ := b.begin("peer", true)
	roster, _ := b.begin("peer", false)
	b.record("peer", outcomeNeutral, &roster)
	if b.State("peer") != BreakerHalfOpen || b.peers["peer"].inFlight != 1 {
		t.Fatal("roster cancellation consumed data probe")
	}
	b.record("peer", outcomeNeutral, &probe)
	if b.State("peer") != BreakerHalfOpen || b.peers["peer"].inFlight != 0 {
		t.Fatal("canceled probes falsely recovered or leaked")
	}
}

func TestClientCancellationIsNeutral(t *testing.T) {
	for _, path := range []string{"block", "validated", "into", "roster", "origin"} {
		for _, phase := range []string{"before dispatch", "before headers", "body"} {
			for _, halfOpen := range []bool{false, true} {
				name := path + "/" + phase + "/closed"
				if halfOpen {
					name = path + "/" + phase + "/half-open"
				}
				t.Run(name, func(t *testing.T) {
					var requests atomic.Int32
					started := make(chan struct{})
					bodyRead := make(chan struct{})
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests.Add(1)
						if phase == "body" {
							w.Header().Set(HeaderOriginResponse, "1")
							w.Header().Set("Content-Length", "16")
							_, _ = io.WriteString(w, "x")
							w.(http.Flusher).Flush()
						}
						close(started)
						<-r.Context().Done()
					}))
					defer srv.Close()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					clk := &brkClock{}
					c := NewClient()
					c.Timeout = -1
					if phase == "body" {
						transport := c.HTTP.Transport
						c.HTTP.Transport = boundRoundTripFunc(func(r *http.Request) (*http.Response, error) {
							resp, err := transport.RoundTrip(r)
							if err == nil {
								resp.Body = &notifyBodyRead{ReadCloser: resp.Body, started: bodyRead}
							}
							return resp, err
						})
					}
					c.Breaker = NewBreaker(BreakerOptions{Now: clk.now})
					addr := srv.Listener.Addr().String()
					want := BreakerClosed
					if halfOpen {
						c.Breaker.RecordHardFailure(addr)
						clk.advance(DefaultBreakerCooldown)
						want = BreakerHalfOpen
					} else {
						c.Breaker.RecordFailure(addr)
					}
					if phase == "before dispatch" {
						cancel()
					}
					done := make(chan error, 1)
					go func() {
						var err error
						switch path {
						case "block":
							_, _, err = c.Get(ctx, addr, BlockRequest{})
						case "validated":
							_, _, err = c.GetValidated(ctx, addr, BlockRequest{}, 16, nil)
						case "into":
							_, _, err = c.GetInto(ctx, addr, BlockRequest{}, make([]byte, 16), nil)
						case "roster":
							_, _, err = c.FetchRoster(ctx, addr, "", "")
						case "origin":
							req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://origin/file", nil)
							var resp *http.Response
							resp, err = c.RoundTripOrigin(req, addr)
							if err == nil {
								_, err = io.Copy(io.Discard, resp.Body)
								resp.Body.Close()
							}
						}
						done <- err
					}()
					if phase != "before dispatch" {
						select {
						case <-started:
						case <-time.After(5 * time.Second):
							t.Fatal("request did not arrive")
						}
						if phase == "body" {
							select {
							case <-bodyRead:
							case <-time.After(5 * time.Second):
								t.Fatal("body read did not begin")
							}
						}
						cancel()
					}
					select {
					case err := <-done:
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("error = %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("canceled request did not return")
					}
					if phase == "before dispatch" && requests.Load() != 0 {
						t.Fatal("canceled request reached server")
					}
					b := c.Breaker
					if b.State(addr) != want || b.peers[addr].failures != 1 || b.peers[addr].inFlight != 0 {
						t.Fatalf("cancellation altered health or leaked admission: %+v", b.peers[addr])
					}
					p, ok := b.begin(addr, true)
					if !ok {
						t.Fatal("cancel leaked probe slot")
					}
					b.record(addr, outcomeAnswered, &p)
					if b.State(addr) != BreakerClosed {
						t.Fatal("real answer cannot recover")
					}
				})
			}
		}
	}
}

func TestOriginEarlyCloseIsNeutral(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderOriginResponse, "1")
		_, _ = io.WriteString(w, "prefix")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	clk := &brkClock{}
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{Now: clk.now})
	addr := srv.Listener.Addr().String()
	c.Breaker.RecordHardFailure(addr)
	clk.advance(DefaultBreakerCooldown)
	req, _ := http.NewRequest(http.MethodGet, "http://origin/file", nil)
	resp, err := c.RoundTripOrigin(req, addr)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if c.Breaker.State(addr) != BreakerHalfOpen || c.Breaker.peers[addr].inFlight != 0 || c.Breaker.peers[addr].failures != 1 {
		t.Fatal("early close falsely recovered or leaked")
	}
	p, ok := c.Breaker.begin(addr, true)
	if !ok {
		t.Fatal("close did not release admission")
	}
	resp.Body.Close() // must not consume the new probe's slot
	if c.Breaker.peers[addr].inFlight != 1 {
		t.Fatal("second close released another probe")
	}
	c.Breaker.record(addr, outcomeAnswered, &p)
}

// Wrap the real HTTP body to synchronize cancellation after the consumer enters
// its body path, rather than merely after the server has written headers.
type notifyBodyRead struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (b *notifyBodyRead) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	return b.ReadCloser.Read(p)
}

func TestCallerDeadlineIsNeutral(t *testing.T) {
	for _, path := range []string{"block", "roster", "origin"} {
		t.Run(path, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
			defer srv.Close()
			clk := &brkClock{}
			c := NewClient()
			c.Breaker = NewBreaker(BreakerOptions{Now: clk.now})
			addr := srv.Listener.Addr().String()
			c.Breaker.RecordHardFailure(addr)
			clk.advance(DefaultBreakerCooldown)
			ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
			defer cancel()
			var err error
			switch path {
			case "block":
				_, _, err = c.Get(ctx, addr, BlockRequest{})
			case "roster":
				_, _, err = c.FetchRoster(ctx, addr, "", "")
			case "origin":
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://origin/file", nil)
				_, err = c.RoundTripOrigin(req, addr)
			}
			if !errors.Is(err, context.DeadlineExceeded) || requests.Load() != 0 || c.Breaker.State(addr) != BreakerHalfOpen || c.Breaker.peers[addr].inFlight != 0 {
				t.Fatalf("deadline falsely changed health: %v, requests=%d, entry=%+v", err, requests.Load(), c.Breaker.peers[addr])
			}
		})
	}
}

func TestValidationCancellationIsNeutral(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer srv.Close()
	c := NewClient()
	c.Breaker = NewBreaker(BreakerOptions{FailureThreshold: 2})
	addr := srv.Listener.Addr().String()
	c.Breaker.RecordFailure(addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := c.GetValidated(ctx, addr, BlockRequest{}, 2, func(ctx context.Context, _ int64) error { cancel(); return ctx.Err() })
	if !errors.Is(err, context.Canceled) || c.Breaker.State(addr) != BreakerClosed || c.Breaker.peers[addr].failures != 1 {
		t.Fatal("validation cancellation changed failure history", err)
	}
}

func TestOriginEmptyResponseRecoversWithoutRead(t *testing.T) {
	for _, tc := range []struct {
		name, method, length string
		status               int
	}{
		{"empty GET", http.MethodGet, "0", http.StatusOK},
		{"HEAD", http.MethodHead, "100", http.StatusOK},
		{"no content", http.MethodGet, "", http.StatusNoContent},
		{"not modified", http.MethodGet, "100", http.StatusNotModified},
	} {
		for _, clientTimeout := range []time.Duration{0, 5 * time.Second} {
			t.Run(tc.name+"/"+clientTimeout.String(), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set(HeaderOriginResponse, "1")
					if tc.length != "" {
						w.Header().Set("Content-Length", tc.length)
					}
					w.WriteHeader(tc.status)
				}))
				defer srv.Close()
				clk := &brkClock{}
				c := NewClient()
				c.HTTP.Timeout = clientTimeout
				c.Breaker = NewBreaker(BreakerOptions{Now: clk.now})
				addr := srv.Listener.Addr().String()
				c.Breaker.RecordHardFailure(addr)
				clk.advance(DefaultBreakerCooldown)
				req, _ := http.NewRequest(tc.method, "http://origin/file", nil)
				resp, err := c.RoundTripOrigin(req, addr)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if c.Breaker.State(addr) != BreakerClosed {
					t.Fatal("complete empty response did not recover at headers")
				}
				if data, err := io.ReadAll(resp.Body); err != nil || len(data) != 0 {
					t.Fatalf("completed response is not readable empty body: %q %v", data, err)
				}
			})
		}
	}
}

func TestOriginHTTPClientBodyTimeoutIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderOriginResponse, "1")
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, "x")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := NewClient()
	c.Timeout = -1
	c.HTTP.Timeout = time.Second
	clk := &brkClock{}
	c.Breaker = NewBreaker(BreakerOptions{Now: clk.now})
	addr := srv.Listener.Addr().String()
	c.Breaker.RecordHardFailure(addr)
	clk.advance(DefaultBreakerCooldown)
	req, _ := http.NewRequest(http.MethodGet, "http://origin/file", nil)
	resp, err := c.RoundTripOrigin(req, addr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if !errors.Is(err, context.DeadlineExceeded) || req.Context().Err() != nil || c.Breaker.State(addr) != BreakerOpen {
		t.Fatalf("client-owned timeout falsely recovered: err=%v caller=%v breaker=%v", err, req.Context().Err(), c.Breaker.State(addr))
	}
}
