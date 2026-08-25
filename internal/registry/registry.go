// Package registry makes DART usable as a container registry pull-through
// mirror, so containerd can be pointed at it with a hosts.toml entry.
//
// This is the path for **containerd's own pulls**: manifest resolution and layer
// blobs for ordinary images. It is *not* how OverlayBD fetches layer data —
// OverlayBD uses its p2pConfig HTTP API, `GET /<APIKey>/<upstream URL>`, which
// DART serves through engine.Handler's prefix resolver (see docs/dart.md). The
// two front-ends are complementary and can share one cache: containerd resolves
// and pulls through the mirror while OverlayBD reads ranges through the prefix
// API, and a blob fetched either way is stored once because both key on the
// digest.
//
// containerd sends ordinary Registry v2 requests carrying the *upstream* paths,
// and DART decides per path whether the response is cacheable:
//
//	GET /v2/library/nginx/blobs/sha256:<hex>      -> cached, range-capable
//	GET /v2/library/nginx/manifests/latest        -> passed through, never cached
//
// # What is cacheable, and why only that
//
// Only **digest-addressed** content is cached. A blob URL names its own content
// hash, so it is immutable: caching it can never serve the wrong bytes, and the
// digest doubles as a cache key that dedups the same layer across registries
// (see chunk.ObjectID).
//
// Manifests are deliberately **not** cached. A manifest reference is usually a
// tag, and tags are mutable — `:latest` points at different content over time.
// Caching them would pin a stale image and be visible to users as "my deployment
// did not pick up the new build". Manifests are small, so passing them through
// costs little; the bytes that matter for pull time are the layers.
//
// # Pull-only
//
// A mirror advertises only pull/resolve, so write methods are refused rather
// than proxied: silently forwarding a push through a cache invites a cache that
// disagrees with its upstream.
package registry

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/data-accelerator/dart/internal/chunk"
	"net/url"
	"strings"

	"github.com/data-accelerator/dart/internal/engine"
)

// Path is the Registry v2 API prefix that a Mirror serves. A dispatcher can use
// it to route only registry traffic to the mirror and leave other front-ends
// (such as the OverlayBD prefix API) on the same listener.
const Path = "/v2/"

// Options configures a Mirror.
type Options struct {
	// Upstream is the registry being mirrored, e.g.
	// "https://registry-1.docker.io". Required.
	Upstream string
	// Engine serves cached blob bytes. Required.
	Engine *engine.Engine
	// Transport carries pass-through requests; nil uses http.DefaultTransport.
	Transport http.RoundTripper
}

// Mirror is a Registry v2 pull-through mirror: it serves blobs from DART's cache
// and passes every other API request straight to the upstream registry.
type Mirror struct {
	upstream *url.URL
	blobs    http.Handler
	proxy    *httputil.ReverseProxy
}

var _ http.Handler = (*Mirror)(nil)

// New builds a Mirror for the upstream registry.
func New(opt Options) (*Mirror, error) {
	if opt.Engine == nil {
		return nil, errors.New("registry: Engine is required")
	}
	if opt.Upstream == "" {
		return nil, errors.New("registry: Upstream is required")
	}
	up, err := url.Parse(opt.Upstream)
	if err != nil {
		return nil, fmt.Errorf("registry: parse upstream %q: %w", opt.Upstream, err)
	}
	if up.Scheme != "http" && up.Scheme != "https" {
		return nil, fmt.Errorf("registry: upstream %q must be http or https", opt.Upstream)
	}
	if up.Host == "" {
		return nil, fmt.Errorf("registry: upstream %q has no host", opt.Upstream)
	}
	up.Path = strings.TrimSuffix(up.Path, "/")

	m := &Mirror{upstream: up}
	m.blobs = &engine.Handler{E: opt.Engine, Resolve: m.resolveBlob}
	m.proxy = &httputil.ReverseProxy{
		Transport: opt.Transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = up.Scheme
			pr.Out.URL.Host = up.Host
			// Preserve the client's exact encoding: URL.Path is decoded, and a
			// decoded '?' or '#' would corrupt the outgoing request (a %3F in a
			// repository name must reach the upstream as %3F).
			pr.Out.URL.Path = up.Path + pr.In.URL.Path
			pr.Out.URL.RawPath = up.Path + pr.In.URL.EscapedPath()
			// Registries route on Host and TLS needs the right SNI, so the
			// outgoing Host must be the upstream's, not the mirror's.
			pr.Out.Host = up.Host
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, "upstream registry error: "+err.Error(), http.StatusBadGateway)
		},
	}
	return m, nil
}

// upstreamURL builds the upstream URL for a request, preserving the client's
// path encoding: r.URL.Path is decoded, so concatenating it raw would let a
// decoded '?'/'#' (from a %3F/%23 in a repository name) corrupt the URL — the
// fetch identity and the upstream request must be the same URL.
func (m *Mirror) upstreamURL(r *http.Request) string {
	u := &url.URL{
		Scheme:  m.upstream.Scheme,
		Host:    m.upstream.Host,
		Path:    m.upstream.Path + r.URL.Path,
		RawPath: m.upstream.Path + r.URL.EscapedPath(),
	}
	return u.String()
}

// resolveBlob maps a blob request to its upstream URL for the engine.
func (m *Mirror) resolveBlob(r *http.Request) (string, error) {
	if _, ok := BlobDigest(r.URL.Path); !ok {
		return "", fmt.Errorf("registry: %q is not a blob path", r.URL.Path)
	}
	return m.upstreamURL(r), nil
}

// ServeHTTP implements http.Handler.
func (m *Mirror) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Pull-only: refuse writes instead of proxying them.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "registry mirror is pull-only", http.StatusMethodNotAllowed)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/v2/") && r.URL.Path != "/v2" {
		http.Error(w, "not a registry v2 path", http.StatusNotFound)
		return
	}

	if digest, ok := BlobDigest(r.URL.Path); ok {
		// Immutable, digest-addressed: serve from cache. Echo the digest the
		// client asked for so containerd can verify without re-reading the body.
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Type", "application/octet-stream")
		m.blobs.ServeHTTP(w, r)
		return
	}
	m.proxy.ServeHTTP(w, r)
}

// BlobDigest reports the digest of a Registry v2 blob path, i.e.
// "/v2/<name>/blobs/<digest>". ok is false for any other path, including blob
// upload paths ("/v2/<name>/blobs/uploads/...") and manifest paths.
//
// The name component may itself contain slashes, so the check is anchored on the
// last "/blobs/" separator rather than on a fixed segment count.
func BlobDigest(path string) (string, bool) {
	const prefix = "/v2/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	const sep = "/blobs/"
	i := strings.LastIndex(path, sep)
	// i must leave room for a non-empty name after the prefix. Note that i can
	// legitimately be smaller than len(prefix) when the separator overlaps it
	// ("/v2/blobs/...", which has no repository name), so this bound also keeps the
	// slice below in range.
	if i < len(prefix) {
		return "", false
	}
	name := path[len(prefix):i]
	digest := path[i+len(sep):]
	if name == "" || !validDigest(digest) {
		return "", false
	}
	return digest, true
}

// validDigest reports whether s is a recognizable content digest
// "<algorithm>:<hex>". It is exactly chunk.IsDigest: the cacheability
// classification must agree with the identity derivation (chunk.ObjectID), or a
// path would be marked cacheable without being content-addressed — a mutable
// path cached forever. This rejects anything it does not fully understand: a
// further path segment (an upload URL), an empty half, an unknown shape.
func validDigest(s string) bool {
	return chunk.IsDigest(s)
}
