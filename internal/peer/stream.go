package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/data-accelerator/dart/internal/store"
)

// StreamSource serves a block by streaming it into w, returning the number of
// bytes written. held is false if this node cannot provide the block (the server
// then replies 404 and nothing has been written).
//
// A StreamSource enables cut-through: a relay can copy bytes from its upstream
// straight to the downstream socket without buffering the whole block, so a
// multi-hop chain pipelines instead of storing-and-forwarding at every hop.
//
// Implementations MUST NOT write anything when returning held=false, and MUST
// write exactly n bytes when returning n. If size is known before writing, the
// implementation reports it via the sizer callback so the server can set
// Content-Length; otherwise the response is chunked.
//
// To report that the *origin* refused the fetch-on-behalf (rather than this node
// failing), return an error satisfying UpstreamRefusedError so the server can set
// HeaderUpstreamStatus and the requester does not blame this peer.
type StreamSource func(ctx context.Context, req BlockRequest, w io.Writer, sizer func(int64)) (n int64, held bool, err error)

// UpstreamRefusedError is implemented by a Source error meaning the origin
// rejected the credential the requester supplied. See HeaderUpstreamStatus.
type UpstreamRefusedError interface {
	error
	// UpstreamStatus returns the origin's HTTP status code.
	UpstreamStatus() int
}

// upstreamStatusOf returns the origin status when err reports a credential
// refusal, else 0.
func upstreamStatusOf(err error) int {
	var ur UpstreamRefusedError
	if errors.As(err, &ur) {
		return ur.UpstreamStatus()
	}
	return 0
}

// StoreStreamSource adapts a store.Store into a StreamSource. It copies the
// block out under the store lock (Get) rather than handing back a slot-backed
// reader: on the peer plane a concurrent eviction must never be able to tear the
// bytes mid-write, and the block-sized copy is a cheap price for that.
func StoreStreamSource(s store.Store) StreamSource {
	return func(_ context.Context, req BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
		data, ok, err := s.Get(req.Key)
		if err != nil || !ok {
			return 0, false, err
		}
		sizer(int64(len(data)))
		written, err := io.Copy(w, bytes.NewReader(data))
		return written, true, err
	}
}

// StreamServer serves blocks to peers by streaming them, enabling cut-through
// relay. It is the streaming counterpart of Server.
type StreamServer struct {
	// NodeID is this node's stable identity, echoed in responses.
	NodeID string
	// Src streams block bytes. Required.
	Src StreamSource
}

var _ http.Handler = (*StreamServer)(nil)

// ServeHTTP implements http.Handler for GET /peer/v1/block/<hex>/<index>.
//
// The response uses Content-Length when the source knows the size up front
// (the common case: a locally-held block), otherwise chunked encoding — which is
// acceptable on the peer plane (unlike the client plane, which must always be
// Content-Length framed).
func (s *StreamServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key, ok := parseBlockPath(r.URL.Path)
	if !ok {
		http.Error(w, "bad block path", http.StatusBadRequest)
		return
	}
	hop, _ := strconv.Atoi(r.Header.Get(HeaderHop))
	req := BlockRequest{Key: key, URL: r.Header.Get(HeaderOrigin), Hop: hop}

	// Buffer the head of the stream so a "not held" or an immediate error can
	// still produce a clean 404/500 before any body byte is committed.
	lw := &lazyWriter{w: w, node: s.NodeID}
	n, held, err := s.Src(r.Context(), req, lw, lw.setSize)
	switch {
	case err != nil && !lw.started:
		// Distinguish "the origin refused our requester's credential" from "this
		// node failed", so the requester does not open a circuit against a healthy
		// peer (see HeaderUpstreamStatus).
		if code := upstreamStatusOf(err); code != 0 {
			w.Header().Set(HeaderUpstreamStatus, strconv.Itoa(code))
			http.Error(w, "upstream refused the supplied credential", http.StatusBadGateway)
			return
		}
		http.Error(w, "source error", http.StatusInternalServerError)
	case !held && !lw.started:
		http.Error(w, "block not held", http.StatusNotFound)
	case err != nil:
		// Headers already sent; the truncated body signals the failure. When a
		// size was reported the client detects the truncation via Content-Length
		// mismatch; on a chunked response (no sizer call) an abrupt mid-body
		// failure surfaces as unexpected EOF — but a *clean* chunked EOF is
		// indistinguishable from success, so a cut-through relay must validate
		// the streamed length against the object geometry itself (the engine's
		// relay does; see engine.relayFromParent).
		_ = n
	}
}

// lazyWriter defers the response header until the first byte is written, so the
// handler can still send a 404/500 if the source declines or fails early.
type lazyWriter struct {
	w       http.ResponseWriter
	node    string
	size    int64
	haveLen bool
	started bool
}

// setSize records a known body length, used for Content-Length if the header has
// not been written yet.
func (l *lazyWriter) setSize(n int64) {
	if !l.started {
		l.size, l.haveLen = n, true
	}
}

func (l *lazyWriter) Write(p []byte) (int, error) {
	if !l.started {
		l.started = true
		if l.node != "" {
			l.w.Header().Set(HeaderNode, l.node)
		}
		if l.haveLen {
			l.w.Header().Set("Content-Length", strconv.FormatInt(l.size, 10))
		}
		l.w.WriteHeader(http.StatusOK)
	}
	n, err := l.w.Write(p)
	if f, ok := l.w.(http.Flusher); ok {
		f.Flush() // push bytes downstream promptly (cut-through)
	}
	return n, err
}

// Stream fetches a block from the peer at addr and copies it into w as it
// arrives, without buffering the whole block. It returns the number of bytes
// written; held is false if the peer returned 404 (nothing written).
func (c *Client) Stream(ctx context.Context, addr string, req BlockRequest, w io.Writer) (n int64, held bool, err error) {
	if err := c.allow(addr); err != nil {
		return 0, false, err
	}
	caller := ctx // caller cancellation is not a peer failure (see Get)
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, blockURL(addr, req.Key), nil)
	if err != nil {
		c.record(addr, outcomeSoftFail)
		return 0, false, err
	}
	if req.URL != "" {
		hreq.Header.Set(HeaderOrigin, req.URL)
	}
	hreq.Header.Set(HeaderHop, strconv.Itoa(req.Hop))

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(hreq)
	if err != nil {
		if caller.Err() != nil {
			c.record(addr, outcomeAnswered)
		} else {
			c.record(addr, classify(err))
		}
		return 0, false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		n, err = io.Copy(w, resp.Body)
		if err != nil {
			if caller.Err() != nil {
				c.record(addr, outcomeAnswered) // we aborted; the peer is fine
			} else {
				c.record(addr, outcomeSoftFail)
			}
			return n, true, fmt.Errorf("peer %s: stream body: %w", addr, err)
		}
		c.record(addr, outcomeAnswered)
		return n, true, nil
	case http.StatusNotFound:
		c.record(addr, outcomeAnswered) // answered; simply does not hold the block
		return 0, false, nil
	default:
		if err := upstreamRefusal(addr, resp); err != nil {
			c.record(addr, outcomeAnswered) // the peer is fine; our credential is not
			return 0, false, err
		}
		c.record(addr, outcomeSoftFail)
		return 0, false, fmt.Errorf("peer %s: unexpected status %s", addr, resp.Status)
	}
}
