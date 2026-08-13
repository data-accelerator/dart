package engine

import (
	"context"
	"errors"
	"io"
	"net/http"
)

// ServePassthrough proxies a client request for url to origin verbatim:
// status, entity headers and body are forwarded as they arrive, and nothing
// is cached or routed through P2P. It is the fallback for an origin that does
// not honor Range (see RangeUnsupported): such an origin would answer every
// per-block fetch with the whole object, so the block layer is bypassed
// entirely.
//
// The client's Range header is forwarded, so what the client receives is
// exactly what talking to the origin directly would have produced (including
// a 200 full body answering a ranged request). HEAD is served from a GET
// upstream, since a presigned URL is signed for GET alone (see fetch.Fetcher).
//
// An error is returned only before the response is committed; once headers
// are sent, a mid-stream failure can only truncate the body (the caller
// detects it via Content-Length), so it is not reported.
func (e *Engine) ServePassthrough(ctx context.Context, w http.ResponseWriter, r *http.Request, url string) error {
	if e.opener == nil {
		return errors.New("engine: passthrough unavailable: fetcher cannot stream")
	}
	up := make(http.Header, 1)
	if rng := r.Header.Get("Range"); rng != "" {
		up.Set("Range", rng)
	}
	resp, err := e.opener.Open(ctx, url, up)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Entity headers only: hop-by-hop headers are net/http's business, and the
	// rest (Date, Server, ...) the origin's values would misrepresent us.
	for _, k := range []string{
		"Content-Type", "Content-Length", "Content-Range",
		"ETag", "Last-Modified", "Cache-Control", "Accept-Ranges",
	} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	var n int64
	if r.Method != http.MethodHead {
		n, _ = io.Copy(w, resp.Body) // a failure here can only truncate
	}
	e.mx.recordPassthrough(n)
	return nil
}
