package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// OriginPath carries uncached read-only origin operations to a supply root.
const OriginPath = "/peer/v2/origin"

// HeaderOriginResponse distinguishes an origin response (including a refusal)
// from a peer that could not perform the operation. It is consumed internally.
const HeaderOriginResponse = "X-DART-Origin-Response"

// OriginServer serves terminal GET/HEAD operations through a root's local
// transport, including registry authentication. A nil Transport refuses service;
// it must never be replaced with a peer-delegating transport.
type OriginServer struct {
	Transport http.RoundTripper
}

func (s *OriginServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "origin access is read-only", http.StatusMethodNotAllowed)
		return
	}
	if s.Transport == nil {
		http.Error(w, "not a supply root", http.StatusServiceUnavailable)
		return
	}
	u, err := url.Parse(r.Header.Get(HeaderOrigin))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || r.ContentLength > 0 || len(r.TransferEncoding) != 0 || r.Header.Get("Upgrade") != "" {
		http.Error(w, "invalid origin request", http.StatusBadRequest)
		return
	}
	proxy := &httputil.ReverseProxy{
		Transport: s.Transport,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL = u
			p.Out.Host = u.Host
			p.Out.Header.Del(HeaderOrigin) // consume the proxy's routing envelope
		},
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Set(HeaderOriginResponse, "1")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.Header().Set(HeaderSourceFailure, "1")
			http.Error(w, "origin unavailable", http.StatusServiceUnavailable)
		},
	}
	proxy.ServeHTTP(w, r)
}

// RoundTripOrigin forwards one GET/HEAD operation. It never follows redirects:
// any Location is returned to the caller, whose next request must be delegated
// again. Timeout bounds header wait and each blocked body read, not the total
// duration of a large streaming object. The caller must close the response body.
func (c *Client) RoundTripOrigin(req *http.Request, addr string) (*http.Response, error) {
	hasBody := req.Body != nil && req.Body != http.NoBody
	if req.Body != nil {
		req.Body.Close()
	}
	if (req.Method != http.MethodGet && req.Method != http.MethodHead) || hasBody || req.ContentLength > 0 || len(req.TransferEncoding) != 0 {
		return nil, errors.New("peer: invalid origin operation")
	}
	permit, err := c.allow(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancelCause := context.WithCancelCause(req.Context())
	cancel := func() { cancelCause(context.Canceled) }
	bound := c.Timeout
	if bound == 0 {
		bound = DefaultRequestTimeout
	}
	var timer *time.Timer
	if bound > 0 {
		timer = time.AfterFunc(bound, func() { cancelCause(context.DeadlineExceeded) })
	}
	stop := func() {
		if timer != nil {
			timer.Stop()
		}
	}
	wire, err := http.NewRequestWithContext(ctx, req.Method, "http://"+addr+OriginPath, nil)
	if err != nil {
		stop()
		cancel()
		c.record(addr, permit, outcomeSoftFail)
		return nil, err
	}
	wire.Header = req.Header.Clone()
	wire.Header.Set(HeaderOrigin, req.URL.String())
	client := *c.http()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(wire)
	stop()
	if err == nil && ctx.Err() != nil {
		resp.Body.Close()
		err = context.Cause(ctx)
	}
	if err != nil {
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			err = context.DeadlineExceeded
		}
		cancel()
		c.record(addr, permit, requestOutcome(req.Context(), err))
		return nil, fmt.Errorf("peer origin request: %w", err)
	}
	if resp.Header.Get(HeaderOriginResponse) != "1" {
		resp.Body.Close()
		cancel()
		// A peer declining or reporting source failure is an honest answer.
		c.record(addr, permit, outcomeAnswered)
		return nil, ErrSourceUnavailable
	}
	resp.Header.Del(HeaderOriginResponse)
	resp.Request = req
	if resp.Body == http.NoBody {
		// HEAD and explicitly empty responses are already complete. Closing
		// them without a Read is not abandonment of an unfinished stream.
		cancel()
		c.record(addr, permit, outcomeAnswered)
		return resp, nil
	}
	resp.Body = &originBody{ReadCloser: resp.Body, ctx: ctx, caller: req.Context(), timer: timer, bound: bound,
		finish: func(o outcome) {
			cancel()
			c.record(addr, permit, o)
		},
	}
	return resp, nil
}

type originBody struct {
	io.ReadCloser
	ctx    context.Context
	caller context.Context
	timer  *time.Timer
	bound  time.Duration
	finish func(outcome)
	once   sync.Once
}

func (b *originBody) Read(p []byte) (int, error) {
	if b.timer != nil {
		b.timer.Reset(b.bound)
	}
	n, err := b.ReadCloser.Read(p)
	if b.timer != nil {
		b.timer.Stop()
	}
	if err != nil {
		// An origin stream error is not a malformed cached block. Caller
		// abandonment is neutral; our own idle deadline remains a failure.
		o := outcomeAnswered
		if b.ctx.Err() != nil {
			o = outcomeNeutral
			if errors.Is(context.Cause(b.ctx), context.DeadlineExceeded) {
				err = context.DeadlineExceeded
				if b.caller.Err() == nil {
					o = outcomeSoftFail
				}
			}
		}
		b.once.Do(func() { b.finish(o) })
	}
	return n, err
}

func (b *originBody) Close() error {
	if b.timer != nil {
		b.timer.Stop()
	}
	b.once.Do(func() { b.finish(outcomeNeutral) })
	return b.ReadCloser.Close()
}
