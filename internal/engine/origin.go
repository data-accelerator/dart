package engine

import (
	"errors"
	"io"
	"net/http"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/hashring"
)

// OriginTransport returns the non-root frontend transport. Every operation is
// sent to a file-key-selected supply root; there is no direct-origin fallback.
// Root-capable callers must use their local origin transport instead.
func (e *Engine) OriginTransport() http.RoundTripper { return originTransport{e} }

// OriginSource wraps a root's local origin transport for terminal proxy service
// and accounts response bytes as origin ingress. It returns nil on a non-root
// node or for a nil transport. The supplied transport must not delegate to peers.
func (e *Engine) OriginSource(rt http.RoundTripper) http.RoundTripper {
	if !e.role.CanRoot() || rt == nil {
		return nil
	}
	return originSource{rt: rt, mx: e.mx}
}

type originSource struct {
	rt http.RoundTripper
	mx *Metrics
}

func (s originSource) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := s.rt.RoundTrip(req)
	if err == nil && s.mx != nil {
		resp.Body = &originResponseBody{ReadCloser: resp.Body, mx: s.mx}
	}
	return resp, err
}

type originResponseBody struct {
	io.ReadCloser
	mx *Metrics
}

func (b *originResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mx.originBytes.Add(int64(n))
	return n, err
}

type originTransport struct{ e *Engine }

func (t originTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	hasBody := req.Body != nil && req.Body != http.NoBody
	if req.Body != nil {
		req.Body.Close()
	}
	e := t.e
	if e.role.CanRoot() || !e.p2p {
		return nil, errors.New("engine: delegated origin transport requires a non-root peer")
	}
	if (req.Method != http.MethodGet && req.Method != http.MethodHead) || hasBody || req.ContentLength > 0 || len(req.TransferEncoding) != 0 {
		return nil, errors.New("engine: origin operation must be a bodyless GET or HEAD")
	}
	view := e.cluster.Current()
	if view == nil {
		return nil, errNoRoot
	}
	oid, _ := chunk.ObjectID(req.URL.String())
	roots := hashring.Rank(e.fileKey(oid), view.Roots())
	var last error = errNoRoot
	seen := make(map[string]bool)
	for i, n := range roots {
		if i >= maxHop {
			break
		}
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		m, ok := view.Get(n.ID)
		if !ok || m.Addr == "" || seen[m.Addr] {
			continue
		}
		seen[m.Addr] = true
		resp, err := e.peer.RoundTripOrigin(req, m.Addr)
		if err == nil {
			return resp, nil // all origin statuses are terminal, including 4xx/5xx
		}
		last = err
	}
	return nil, last
}
