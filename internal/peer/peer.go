// Package peer is DART's node-to-node block transport: an HTTP server that
// serves blocks to other nodes, and a pooled client that fetches a block from a
// peer.
//
// The wire form is a plaintext HTTP/1.1 GET (peer-plane encryption is left to
// the CNI layer, per the design):
//
//	GET /peer/v1/block/<chunkKey-hex>/<blockIndex>
//	X-DART-Origin: <upstream url>   (optional; enables relay fetch-on-behalf)
//	X-DART-Hop:    <n>              (relay depth, for loop safety)
//
// A Source backed only by the local store (StoreSource) returns 404 for blocks
// it does not hold. A relay-capable Source (e.g. the engine's) may, given the
// X-DART-Origin url, fetch the block via its own parent/origin and serve it —
// turning each node into a relay in the distribution tree.
package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

// blockPath is the URL prefix for block requests.
const blockPath = "/peer/v1/block/"

// Header names on the peer plane.
const (
	HeaderNode   = "X-DART-Node"
	HeaderEpoch  = "X-DART-Epoch"
	HeaderOrigin = "X-DART-Origin"
	HeaderHop    = "X-DART-Hop"
	// HeaderUpstreamStatus lets a relay report that the *origin* refused the
	// fetch-on-behalf, as opposed to the relay itself failing.
	//
	// The distinction matters because an upstream may be a presigned URL supplied
	// by the requester: if that signature has expired, the relay cannot fetch it,
	// but nothing is wrong with the relay. Without this signal the requester would
	// charge the failure to the relay's circuit breaker and, after a few blocks,
	// eject a perfectly healthy peer.
	HeaderUpstreamStatus = "X-DART-Upstream-Status"
)

// ErrUpstreamRefused reports that a relay could not fetch on our behalf because
// the origin refused the credential we supplied (typically an expired presigned
// URL). The peer itself is healthy.
var ErrUpstreamRefused = errors.New("peer: upstream refused the supplied credential")

// BlockRequest is a decoded peer block request.
type BlockRequest struct {
	// Key identifies the block.
	Key store.BlockKey
	// URL is the upstream origin (X-DART-Origin); empty means transport-only
	// (the peer should serve only what it already holds).
	URL string
	// Hop is the relay depth (X-DART-Hop); incremented at each relay to bound
	// recursion and detect loops.
	Hop int
}

// Source returns the bytes of a block for a peer request. held is false if this
// node cannot provide it; err is for unexpected failures only.
type Source func(ctx context.Context, req BlockRequest) ([]byte, bool, error)

// StoreSource adapts a store.Store into a transport-only Source (it serves only
// locally-held blocks and ignores the relay URL).
func StoreSource(s store.Store) Source {
	return func(_ context.Context, req BlockRequest) ([]byte, bool, error) {
		return s.Get(req.Key)
	}
}

// Server serves blocks to peers.
type Server struct {
	// NodeID is this node's stable identity, echoed in responses.
	NodeID string
	// Src provides block bytes. Required.
	Src Source
}

var _ http.Handler = (*Server)(nil)

// ServeHTTP implements http.Handler for GET /peer/v1/block/<hex>/<index>.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	data, held, err := s.Src(r.Context(), req)
	if err != nil {
		// Distinguish "the origin refused our requester's credential" from "this
		// node failed", so the requester does not open a circuit against a healthy
		// peer (see HeaderUpstreamStatus).
		if code := upstreamStatusOf(err); code != 0 {
			w.Header().Set(HeaderUpstreamStatus, strconv.Itoa(code))
			http.Error(w, "upstream refused the supplied credential", http.StatusBadGateway)
			return
		}
		http.Error(w, "source error", http.StatusInternalServerError)
		return
	}
	if !held {
		http.Error(w, "block not held", http.StatusNotFound)
		return
	}
	if s.NodeID != "" {
		w.Header().Set(HeaderNode, s.NodeID)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// parseBlockPath parses "/peer/v1/block/<chunkHex>/<blockIndex>".
func parseBlockPath(p string) (store.BlockKey, bool) {
	rest, ok := strings.CutPrefix(p, blockPath)
	if !ok {
		return store.BlockKey{}, false
	}
	chunkHex, idxStr, ok := strings.Cut(rest, "/")
	if !ok || chunkHex == "" || idxStr == "" || strings.Contains(idxStr, "/") {
		return store.BlockKey{}, false
	}
	chunk, err1 := strconv.ParseUint(chunkHex, 16, 64)
	idx, err2 := strconv.ParseUint(idxStr, 10, 64)
	if err1 != nil || err2 != nil {
		return store.BlockKey{}, false
	}
	return store.BlockKey{Chunk: chunk, Block: idx}, true
}

// blockURL builds the peer request URL for a block at addr (host:port).
func blockURL(addr string, key store.BlockKey) string {
	return "http://" + addr + blockPath +
		strconv.FormatUint(key.Chunk, 16) + "/" +
		strconv.FormatUint(key.Block, 10)
}

// Timeouts bounding a peer request. They exist as three separate bounds because
// the failure modes they cover differ by orders of magnitude:
//
//   - DefaultDialTimeout catches a peer whose machine is gone. A dead host does
//     not send RST, so without an explicit bound the dial falls back to the OS
//     SYN retry schedule (minutes on Linux) and the read stalls until the overall
//     timeout. Peers are inside one cluster, so failing to connect within a
//     second means the peer is unusable.
//   - DefaultResponseHeaderTimeout catches a host that dies while one of its
//     connections sits in our idle pool: the write succeeds locally and then
//     nothing comes back. It must stay above a relay's legitimate
//     time-to-first-byte, since a relay may have to reach its own parent or the
//     origin before it can start streaming.
//   - DefaultRequestTimeout bounds the whole exchange including the body.
const (
	DefaultDialTimeout           = 1 * time.Second
	DefaultResponseHeaderTimeout = 10 * time.Second
	DefaultRequestTimeout        = 30 * time.Second
)

// Client fetches blocks from peers over pooled, keep-alive HTTP connections.
type Client struct {
	// HTTP is the underlying client; nil uses a pooled default (see NewClient).
	HTTP *http.Client
	// Timeout bounds each block request. Zero means DefaultRequestTimeout;
	// negative disables the bound (rely on the caller's context instead).
	Timeout time.Duration
	// Breaker, if non-nil, short-circuits requests to peers that are failing, so
	// a sick peer is not re-dialed on every block. Nil disables breaking.
	Breaker *Breaker
}

// NewClient returns a Client with a connection pool tuned for many peers and
// keep-alive reuse. Peer-plane traffic is plaintext HTTP.
func NewClient() *Client {
	return &Client{
		HTTP: &http.Client{
			Transport: NewTransport(DefaultDialTimeout, DefaultResponseHeaderTimeout),
		},
		Timeout: DefaultRequestTimeout,
	}
}

// NewTransport builds the peer-plane transport. A non-positive bound disables
// that particular timeout.
func NewTransport(dial, responseHeader time.Duration) *http.Transport {
	d := &net.Dialer{KeepAlive: 30 * time.Second}
	if dial > 0 {
		d.Timeout = dial
	}
	t := &http.Transport{
		DialContext: d.DialContext,
		// Idle-pool sizing: 512 global / 32 per host. The global cap holds
		// ~16 fully-idle peers' worth; past that, global pressure evicts the
		// oldest idle connections, costing a re-dial (~1 RTT) rather than a
		// failure. That is deliberate: idle counts per peer are far below 32 in
		// practice, IdleConnTimeout reaps them anyway, and a per-peer-scaled
		// global cap would need cluster size plumbed into a leaf constructor.
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   false, // HTTP/1.1 keeps sendfile/splice viable
	}
	if responseHeader > 0 {
		t.ResponseHeaderTimeout = responseHeader
	}
	return t
}

// withTimeout applies the client's per-request bound to ctx. The returned cancel
// must be called by the caller (it is a no-op when no bound applies).
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	d := c.Timeout
	if d == 0 {
		d = DefaultRequestTimeout
	}
	if d < 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// outcome is what became of a peer request, as the breaker sees it.
type outcome int

const (
	// outcomeAnswered means the peer replied, including a 404. A 404 is a
	// legitimate answer ("I do not hold that block") and blocks legitimately 404
	// all the time in a distributed cache, so counting it would trip the breaker
	// on healthy peers.
	outcomeAnswered outcome = iota
	// outcomeSoftFail is a peer that is reachable but did not deliver: a timeout,
	// an unexpected status, a truncated body. It might be a transient hiccup, so
	// it spends one unit of the failure budget.
	outcomeSoftFail
	// outcomeHardFail is a peer we could not connect to at all. That is
	// definitive rather than suspicious, so it opens the circuit at once instead
	// of spending the whole budget rediscovering the same fact one dial timeout at
	// a time.
	outcomeHardFail
)

// classify maps a request error to a breaker outcome.
func classify(err error) outcome {
	if err == nil {
		return outcomeAnswered
	}
	if errors.Is(err, ErrUpstreamRefused) {
		// The peer answered correctly; the origin rejected our credential. Charging
		// this to the peer would eject a healthy node for a client-side problem.
		return outcomeAnswered
	}
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		return outcomeHardFail
	}
	return outcomeSoftFail
}

// upstreamRefusal returns a non-nil error when resp carries a relay's report that
// the origin refused our credential.
func upstreamRefusal(addr string, resp *http.Response) error {
	code := resp.Header.Get(HeaderUpstreamStatus)
	if code == "" {
		return nil
	}
	return fmt.Errorf("peer %s: origin returned %s: %w", addr, code, ErrUpstreamRefused)
}

// http returns the HTTP client to use, defaulting to http.DefaultClient.
func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

// allow consults the breaker before dialing addr.
func (c *Client) allow(addr string) error {
	if c.Breaker != nil && !c.Breaker.Allow(addr) {
		return fmt.Errorf("peer %s: %w", addr, ErrCircuitOpen)
	}
	return nil
}

// record reports the outcome of a request to the breaker.
func (c *Client) record(addr string, o outcome) {
	if c.Breaker == nil {
		return
	}
	switch o {
	case outcomeAnswered:
		c.Breaker.RecordSuccess(addr)
	case outcomeHardFail:
		c.Breaker.RecordHardFailure(addr)
	default:
		c.Breaker.RecordFailure(addr)
	}
}

// Get fetches a block from the peer at addr (host:port) per req. held is false
// if the peer returned 404 (cannot provide the block).
func (c *Client) Get(ctx context.Context, addr string, req BlockRequest) (data []byte, held bool, err error) {
	if err := c.allow(addr); err != nil {
		return nil, false, err
	}
	caller := ctx // caller cancellation is not a peer failure (see below)
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, blockURL(addr, req.Key), nil)
	if err != nil {
		c.record(addr, outcomeSoftFail)
		return nil, false, err
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
		// A caller-aborted request (hedging loser, client disconnect) is not a
		// peer failure; charge nothing. The peer did answer if it got that far,
		// and recording answered also releases a half-open probe slot.
		if caller.Err() != nil {
			c.record(addr, outcomeAnswered)
		} else {
			c.record(addr, classify(err))
		}
		return nil, false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			if caller.Err() != nil {
				c.record(addr, outcomeAnswered) // we aborted; the peer is fine
			} else {
				c.record(addr, outcomeSoftFail)
			}
			return nil, false, fmt.Errorf("peer %s: read body: %w", addr, err)
		}
		c.record(addr, outcomeAnswered)
		return body, true, nil
	case http.StatusNotFound:
		c.record(addr, outcomeAnswered) // answered; simply does not hold the block
		return nil, false, nil
	default:
		if err := upstreamRefusal(addr, resp); err != nil {
			c.record(addr, outcomeAnswered) // the peer is fine; our credential is not
			return nil, false, err
		}
		c.record(addr, outcomeSoftFail)
		return nil, false, fmt.Errorf("peer %s: unexpected status %s", addr, resp.Status)
	}
}
