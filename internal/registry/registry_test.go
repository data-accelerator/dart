package registry

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/engine"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/store"
)

// fakeRegistry is a minimal Registry v2 upstream: one blob addressed by digest,
// one mutable manifest, and per-path request counters.
type fakeRegistry struct {
	blob     []byte
	digest   string
	manifest atomic.Value // string, mutable so tag staleness is observable
	blobHits atomic.Int64
	manHits  atomic.Int64
	pingHits atomic.Int64
	gotHost  atomic.Value // string
	gotAuth  atomic.Value // string
}

func newFakeRegistry(t *testing.T, blobSize int) (*fakeRegistry, *httptest.Server) {
	t.Helper()
	blob := make([]byte, blobSize)
	for i := range blob {
		blob[i] = byte(i % 251)
	}
	sum := sha256.Sum256(blob)
	fr := &fakeRegistry{blob: blob, digest: "sha256:" + hex.EncodeToString(sum[:])}
	fr.manifest.Store("manifest-v1")

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		fr.gotHost.Store(r.Host)
		fr.gotAuth.Store(r.Header.Get("Authorization"))
		switch {
		case r.URL.Path == "/v2/":
			fr.pingHits.Add(1)
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "/blobs/"):
			fr.blobHits.Add(1)
			// ServeContent gives us correct Range and Content-Length handling,
			// matching what a real registry (or its CDN) provides.
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(fr.blob))
		case strings.Contains(r.URL.Path, "/manifests/"):
			fr.manHits.Add(1)
			body := fr.manifest.Load().(string)
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			_, _ = io.WriteString(w, body)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return fr, srv
}

// newMirror wires a Mirror over a real engine with a real disk store.
func newMirror(t *testing.T, upstream string) *Mirror {
	t.Helper()
	return newMirrorWithTransport(t, upstream, nil)
}

// newMirrorWithTransport is newMirror with a transport applied to both the
// pass-through proxy and the engine's blob fetches, so an AuthTransport covers
// the whole data path.
func newMirrorWithTransport(t *testing.T, upstream string, rt http.RoundTripper) *Mirror {
	t.Helper()
	st, err := store.OpenTiered(store.TieredOptions{
		Path: filepath.Join(t.TempDir(), "blocks"), SlotSize: 64 << 10, Slots: 64,
	})
	if err != nil {
		t.Fatalf("OpenTiered: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	e, err := engine.New(engine.Options{
		Chunk:   chunk.Config{ChunkSize: 1 << 20, BlockSize: 64 << 10},
		Store:   st,
		Fetcher: &fetch.Coalescing{F: &fetch.HTTPFetcher{Client: &http.Client{Transport: rt}}},
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	m, err := New(Options{Upstream: upstream, Engine: e, Transport: rt})
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	return m
}

func get(t *testing.T, h http.Handler, path string, hdr http.Header) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body
}

// --- digest / path classification ---

func TestBlobDigestClassification(t *testing.T) {
	// validDigest is chunk.IsDigest: lowercase alnum algorithm + >= 32 hex —
	// the same recognizer chunk.ObjectID uses, so cacheable == content-addressed.
	const d = "sha256:" + "ab12cd34" + "ef56ab78" + "90cd12ef" + "34ab56cd" // 32 hex
	cacheable := []string{
		"/v2/library/nginx/blobs/" + d,
		"/v2/deep/nested/name/blobs/" + d,
	}
	rejectedShapes := []string{
		"/v2/n/blobs/sha256:ab12cd34",              // too short to be a digest
		"/v2/n/blobs/sha512:AAbb99__--==",          // not hex
		"/v2/n/blobs/multihash.sha2+256:abc",       // unrecognized algorithm shape
		"/v2/n/blobs/sha256:latest",                // a mutable tag, not a digest
		"/v2/n/blobs/SHA256:" + d[len("sha256:"):], // uppercase algorithm
	}
	for _, p := range cacheable {
		if got, ok := BlobDigest(p); !ok {
			t.Errorf("BlobDigest(%q) = not-a-blob, want cacheable", p)
		} else if !strings.Contains(p, got) {
			t.Errorf("BlobDigest(%q) = %q, not present in the path", p, got)
		}
	}

	notCacheable := append(rejectedShapes,
		"/v2/library/nginx/manifests/latest",             // mutable tag
		"/v2/library/nginx/manifests/"+d,                 // manifests are never cached
		"/v2/library/nginx/blobs/uploads/some-uuid",      // push session
		"/v2/library/nginx/blobs/uploads/uuid?digest="+d, // push completion
		"/v2/library/nginx/blobs/",                       // empty digest
		"/v2/library/nginx/blobs/notadigest",             // no algorithm separator
		"/v2/library/nginx/blobs/:abc",                   // empty algorithm
		"/v2/library/nginx/blobs/sha256:",                // empty encoding
		"/v2/library/nginx/blobs/sha256:ab/cd",           // extra path segment
		"/v2/library/nginx/blobs/SHA256:abc",             // algorithm must be lowercase
		"/v2/library/nginx/blobs/-sha256:abc",            // leading separator
		"/v2/library/nginx/blobs/sha256-:abc",            // trailing separator
		"/v2/blobs/"+d,                                   // empty name
		"/v2/",                                           // ping
		"/v1/library/nginx/blobs/"+d,                     // wrong API version
		"/blobs/"+d,                                      // not under /v2/
	)
	for _, p := range notCacheable {
		if got, ok := BlobDigest(p); ok {
			t.Errorf("BlobDigest(%q) = %q (cacheable), want not-a-blob", p, got)
		}
	}
}

// --- construction ---

func TestNewValidatesOptions(t *testing.T) {
	e, err := engine.New(engine.Options{
		Chunk:   chunk.Config{ChunkSize: 1 << 20, BlockSize: 64 << 10},
		Store:   func() store.Store { s, _ := store.OpenMem(store.MemOptions{SlotSize: 64 << 10, Slots: 4}); return s }(),
		Fetcher: &fetch.HTTPFetcher{},
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	for _, tc := range []struct {
		name string
		opt  Options
	}{
		{"no engine", Options{Upstream: "https://example.com"}},
		{"no upstream", Options{Engine: e}},
		{"bad url", Options{Upstream: "://nope", Engine: e}},
		{"bad scheme", Options{Upstream: "ftp://example.com", Engine: e}},
		{"no host", Options{Upstream: "https://", Engine: e}},
	} {
		if _, err := New(tc.opt); err == nil {
			t.Errorf("%s: expected an error", tc.name)
		}
	}
	if _, err := New(Options{Upstream: "https://example.com/", Engine: e}); err != nil {
		t.Errorf("valid options rejected: %v", err)
	}
}

// --- blob path: cached ---

// TestBlobIsCached is the point of the mirror: the second pull of the same blob
// must not touch the upstream registry.
func TestBlobIsCached(t *testing.T) {
	fr, srv := newFakeRegistry(t, 200<<10) // spans several blocks
	m := newMirror(t, srv.URL)
	path := "/v2/library/nginx/blobs/" + fr.digest

	resp, body := get(t, m, path, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !bytes.Equal(body, fr.blob) {
		t.Fatalf("blob bytes mismatch (%d vs %d)", len(body), len(fr.blob))
	}
	if resp.Header.Get("Docker-Content-Digest") != fr.digest {
		t.Errorf("Docker-Content-Digest = %q, want %q",
			resp.Header.Get("Docker-Content-Digest"), fr.digest)
	}
	first := fr.blobHits.Load()
	if first == 0 {
		t.Fatal("upstream was never contacted for the first pull")
	}

	// Second pull: served entirely from cache.
	_, body2 := get(t, m, path, nil)
	if !bytes.Equal(body2, fr.blob) {
		t.Errorf("second pull bytes mismatch")
	}
	if got := fr.blobHits.Load(); got != first {
		t.Errorf("upstream hit %d more times on a cached pull", got-first)
	}
}

// TestBlobRangeRequest: containerd and OverlayBD read partial layers, so ranges
// must work through the mirror.
func TestBlobRangeRequest(t *testing.T) {
	fr, srv := newFakeRegistry(t, 100<<10)
	m := newMirror(t, srv.URL)
	path := "/v2/library/nginx/blobs/" + fr.digest

	resp, body := get(t, m, path, http.Header{"Range": {"bytes=1000-1999"}})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, fr.blob[1000:2000]) {
		t.Errorf("range bytes mismatch")
	}
	if got := resp.Header.Get("Content-Range"); got != fmt.Sprintf("bytes 1000-1999/%d", len(fr.blob)) {
		t.Errorf("Content-Range = %q", got)
	}
	if len(resp.TransferEncoding) != 0 {
		t.Errorf("client response must not be chunked: %v", resp.TransferEncoding)
	}
}

func TestBlobHead(t *testing.T) {
	fr, srv := newFakeRegistry(t, 50<<10)
	m := newMirror(t, srv.URL)

	msrv := httptest.NewServer(m)
	defer msrv.Close()
	resp, err := http.Head(msrv.URL + "/v2/library/nginx/blobs/" + fr.digest)
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	if resp.ContentLength != int64(len(fr.blob)) {
		t.Errorf("Content-Length = %d, want %d", resp.ContentLength, len(fr.blob))
	}
	if resp.Header.Get("Docker-Content-Digest") != fr.digest {
		t.Errorf("missing Docker-Content-Digest on HEAD")
	}
}

// --- pass-through paths: never cached ---

// TestManifestNotCached is the correctness guarantee that matters most here: a
// tag is mutable, so every manifest request must reach the upstream. Caching it
// would pin a stale image.
func TestManifestNotCached(t *testing.T) {
	fr, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	path := "/v2/library/nginx/manifests/latest"

	_, body := get(t, m, path, nil)
	if string(body) != "manifest-v1" {
		t.Fatalf("first manifest = %q", body)
	}

	// The tag now points at new content, as tags do.
	fr.manifest.Store("manifest-v2")

	_, body2 := get(t, m, path, nil)
	if string(body2) != "manifest-v2" {
		t.Errorf("second manifest = %q, want the updated content (a stale cache would return v1)", body2)
	}
	if fr.manHits.Load() < 2 {
		t.Errorf("upstream saw %d manifest requests, want one per pull", fr.manHits.Load())
	}
}

// TestManifestByDigestAlsoPassesThrough: even an immutable manifest reference is
// proxied, because the value of caching manifests is small and misclassifying a
// tag would be costly.
func TestManifestByDigestAlsoPassesThrough(t *testing.T) {
	fr, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	path := "/v2/library/nginx/manifests/" + fr.digest

	get(t, m, path, nil)
	get(t, m, path, nil)
	if fr.manHits.Load() != 2 {
		t.Errorf("upstream saw %d requests, want 2 (no caching)", fr.manHits.Load())
	}
}

func TestPingPassesThrough(t *testing.T) {
	fr, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	resp, _ := get(t, m, "/v2/", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/v2/ status = %d", resp.StatusCode)
	}
	if fr.pingHits.Load() == 0 {
		t.Error("/v2/ was not forwarded upstream")
	}
}

// TestProxySetsUpstreamHost: registries route on Host, so the proxied request
// must carry the upstream's host, not the mirror's.
func TestProxySetsUpstreamHost(t *testing.T) {
	fr, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	get(t, m, "/v2/library/nginx/manifests/latest", nil)

	upstreamHost := strings.TrimPrefix(srv.URL, "http://")
	if got, _ := fr.gotHost.Load().(string); got != upstreamHost {
		t.Errorf("upstream saw Host %q, want %q", got, upstreamHost)
	}
}

// TestProxyForwardsAuthorization: the pass-through path carries the client's
// credentials, so token-authenticated manifest resolution still works.
func TestProxyForwardsAuthorization(t *testing.T) {
	fr, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	get(t, m, "/v2/library/nginx/manifests/latest",
		http.Header{"Authorization": {"Bearer test-token"}})
	if got, _ := fr.gotAuth.Load().(string); got != "Bearer test-token" {
		t.Errorf("upstream saw Authorization %q, want it forwarded", got)
	}
}

// --- rejections ---

// TestPullOnly: a mirror advertises pull/resolve only, so writes are refused
// rather than forwarded through the cache.
func TestPullOnly(t *testing.T) {
	fr, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	msrv := httptest.NewServer(m)
	defer msrv.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		req, _ := http.NewRequest(method, msrv.URL+"/v2/library/nginx/blobs/uploads/", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want 405", method, resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s Allow = %q", method, got)
		}
	}
	if fr.blobHits.Load() != 0 || fr.manHits.Load() != 0 {
		t.Error("a write reached the upstream registry")
	}
}

func TestNonV2PathRejected(t *testing.T) {
	_, srv := newFakeRegistry(t, 1<<10)
	m := newMirror(t, srv.URL)
	resp, _ := get(t, m, "/healthz", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestUpstreamPathPrefixPreserved: an upstream with a path prefix (a registry
// served under a subpath) must have it prepended.
func TestUpstreamPathPrefixPreserved(t *testing.T) {
	var gotPath atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath.Store(r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := newMirror(t, srv.URL+"/registry")
	get(t, m, "/v2/", nil)
	if got, _ := gotPath.Load().(string); got != "/registry/v2/" {
		t.Errorf("upstream path = %q, want /registry/v2/", got)
	}
}

// TestBlobDedupAcrossRegistries: blobs are keyed by digest, so the same layer
// pulled through two different upstreams is stored once. This is what makes a
// shared cache worthwhile across mirrored registries.
func TestBlobDedupAcrossRegistries(t *testing.T) {
	fr, srv := newFakeRegistry(t, 100<<10)

	// Two mirrors for different upstream hosts, sharing one engine/store.
	st, err := store.OpenTiered(store.TieredOptions{
		Path: filepath.Join(t.TempDir(), "blocks"), SlotSize: 64 << 10, Slots: 64,
	})
	if err != nil {
		t.Fatalf("OpenTiered: %v", err)
	}
	defer st.Close()
	e, err := engine.New(engine.Options{
		Chunk:   chunk.Config{ChunkSize: 1 << 20, BlockSize: 64 << 10},
		Store:   st,
		Fetcher: &fetch.Coalescing{F: &fetch.HTTPFetcher{}},
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	m1, err := New(Options{Upstream: srv.URL, Engine: e})
	if err != nil {
		t.Fatalf("New m1: %v", err)
	}
	// A second mirror pointing at the same server but addressed differently; the
	// digest is what identifies the content.
	alt := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)
	m2, err := New(Options{Upstream: alt, Engine: e})
	if err != nil {
		t.Fatalf("New m2: %v", err)
	}

	pathA := "/v2/repo-a/blobs/" + fr.digest
	pathB := "/v2/repo-b/blobs/" + fr.digest // different repo, same content

	_, b1 := get(t, m1, pathA, nil)
	afterFirst := fr.blobHits.Load()
	_, b2 := get(t, m2, pathB, nil)

	if !bytes.Equal(b1, fr.blob) || !bytes.Equal(b2, fr.blob) {
		t.Fatal("blob bytes mismatch")
	}
	if got := fr.blobHits.Load(); got != afterFirst {
		t.Errorf("upstream hit %d more times; the blob was not deduped by digest", got-afterFirst)
	}
}
