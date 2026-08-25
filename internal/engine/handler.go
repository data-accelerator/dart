package engine

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Handler is an http.Handler that serves object byte ranges through an Engine.
// It resolves the origin URL for each request via Resolve, honors a single
// Range header, and always responds with an explicit Content-Length (never
// chunked) — a requirement of the client plane, not an optimization. See
// docs/engine.md.
type Handler struct {
	E *Engine
	// Resolve maps an incoming request to the upstream origin URL. Required.
	Resolve func(*http.Request) (string, error)
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	url, err := h.Resolve(r)
	if err != nil || url == "" {
		http.Error(w, "cannot resolve origin", http.StatusBadRequest)
		return
	}

	size, err := h.E.Size(r.Context(), url)
	if err != nil {
		http.Error(w, "origin error: "+err.Error(), http.StatusBadGateway)
		return
	}
	if h.E.RangeUnsupported(url) {
		// The origin ignores Range, so the block layer cannot serve it: proxy
		// the request verbatim instead (per-block fetches would each pull the
		// whole object, and no block could be safely cached for reuse).
		if err := h.E.ServePassthrough(r.Context(), w, r, url); err != nil {
			http.Error(w, "origin error: "+err.Error(), http.StatusBadGateway)
		}
		return
	}

	if size == 0 && r.Header.Get("Range") == "" {
		// A plain GET of an empty object is a valid empty 200, not a 416:
		// RFC 7233 defines 416 only for Range requests, and "no Range means the
		// whole object" — an empty whole.
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}

	start, end, isRange, ok := parseRange(r.Header.Get("Range"), size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	// Explicit Content-Length guarantees a non-chunked response even though the
	// body is streamed block by block.
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	if isRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if r.Method == http.MethodHead {
		return
	}

	// Headers (including status) are already sent; a mid-stream error can only
	// truncate the body. The caller detects truncation via Content-Length.
	_ = h.E.Serve(r.Context(), w, url, start, end)
}

// parseRange parses a single-range HTTP Range header against a known object
// size. It returns the inclusive [start, end], whether a Range was present
// (isRange), and whether the request is satisfiable (ok).
//
// Supported forms (single range only):
//
//	""            -> whole object, isRange=false
//	bytes=a-b     -> [a, min(b, size-1)]
//	bytes=a-      -> [a, size-1]
//	bytes=-n      -> last n bytes
//
// Multiple ranges, malformed input, or a start beyond the object are not
// satisfiable (ok=false).
func parseRange(header string, size int64) (start, end int64, isRange, ok bool) {
	if header == "" {
		if size == 0 {
			return 0, 0, false, false // nothing to serve
		}
		return 0, size - 1, false, true
	}
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return 0, 0, false, false
	}
	spec := strings.TrimSpace(header[len(prefix):])
	if strings.Contains(spec, ",") {
		return 0, 0, false, false // multi-range unsupported
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, false
	}
	lo, hi := strings.TrimSpace(spec[:dash]), strings.TrimSpace(spec[dash+1:])

	switch {
	case lo == "": // suffix: bytes=-n
		n, err := strconv.ParseInt(hi, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, true, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true, true
	case hi == "": // bytes=a-
		a, err := strconv.ParseInt(lo, 10, 64)
		if err != nil || a < 0 || a >= size {
			return 0, 0, true, false
		}
		return a, size - 1, true, true
	default: // bytes=a-b
		a, err1 := strconv.ParseInt(lo, 10, 64)
		b, err2 := strconv.ParseInt(hi, 10, 64)
		if err1 != nil || err2 != nil || a < 0 || a > b || a >= size {
			return 0, 0, true, false
		}
		if b >= size {
			b = size - 1
		}
		return a, b, true, true
	}
}

// staticResolver is a convenience Resolve that always returns url (useful for
// tests and single-origin deployments).
func staticResolver(url string) func(*http.Request) (string, error) {
	return func(*http.Request) (string, error) { return url, nil }
}

// NewStaticHandler returns a Handler that serves every request from a single
// fixed origin URL.
func NewStaticHandler(e *Engine, originURL string) *Handler {
	return &Handler{E: e, Resolve: staticResolver(originURL)}
}

var _ http.Handler = (*Handler)(nil)
