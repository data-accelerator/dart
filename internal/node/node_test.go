package node

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
)

func blob(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestResolverFixedOrigin(t *testing.T) {
	r := resolver("http://origin.example/x", "dart")
	req, _ := http.NewRequest(http.MethodGet, "http://dart/anything", nil)
	got, err := r(req)
	if err != nil || got != "http://origin.example/x" {
		t.Errorf("fixed resolver = (%q, %v)", got, err)
	}
}

func TestResolverPrefixPassthrough(t *testing.T) {
	r := resolver("", "dart")
	cases := []struct {
		uri     string
		want    string
		wantErr bool
	}{
		{"/dart/https://reg.example.com/v2/x/blobs/sha256:abc", "https://reg.example.com/v2/x/blobs/sha256:abc", false},
		{"/dart/http://reg.example.com/a?token=1", "http://reg.example.com/a?token=1", false},
		{"/wrong/https://reg.example.com/x", "", true}, // missing prefix
		{"/dart/reg.example.com/x", "", true},          // no scheme
	}
	for _, c := range cases {
		req := &http.Request{RequestURI: c.uri}
		got, err := r(req)
		if c.wantErr {
			if err == nil {
				t.Errorf("uri %q: expected error", c.uri)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("uri %q: got (%q, %v), want %q", c.uri, got, err, c.want)
		}
	}
}

// TestResolverPreservesDoubleSlash guards the "//" in an embedded https:// URL
// against path cleaning (we read RequestURI, not URL.Path).
func TestResolverPreservesDoubleSlash(t *testing.T) {
	r := resolver("", "dart")
	req := &http.Request{RequestURI: "/dart/https://host/a//b"}
	got, err := r(req)
	if err != nil || got != "https://host/a//b" {
		t.Errorf("got (%q, %v), want https://host/a//b", got, err)
	}
}

func TestNewHandlerValidation(t *testing.T) {
	base := config{
		cacheDir: t.TempDir(), cacheSize: 1 << 20,
		chunkSize: 64, blockSize: 16, prefix: "dart",
	}
	// invalid chunk config
	bad := base
	bad.chunkSize = 10 // not a multiple of 16
	if _, err := build(bad); err == nil {
		t.Error("expected invalid chunk config to fail")
	}
	// cache smaller than a block
	bad = base
	bad.cacheSize = 8
	if _, err := build(bad); err == nil {
		t.Error("expected cache-size < block-size to fail")
	}
}

// TestEndToEndPrefix wires a real origin behind a dart handler and fetches a
// range through the prefix-passthrough path.
func TestEndToEndPrefix(t *testing.T) {
	content := blob(1000)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "blob", time.Unix(0, 0), bytes.NewReader(content))
	}))
	defer origin.Close()

	cfg := config{
		cacheDir: t.TempDir(), cacheSize: 1 << 20,
		chunkSize: 64, blockSize: 16, prefix: "dart",
	}
	n, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()
	h := n.client

	dart := httptest.NewServer(h)
	defer dart.Close()

	// Client requests /dart/<origin URL> with a Range.
	req, _ := http.NewRequest(http.MethodGet, dart.URL+"/dart/"+origin.URL, nil)
	req.Header.Set("Range", "bytes=100-199")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, content[100:200]) {
		t.Errorf("body mismatch")
	}
	if len(resp.TransferEncoding) != 0 || resp.ContentLength != 100 {
		t.Errorf("expected non-chunked Content-Length=100, got TE=%v CL=%d", resp.TransferEncoding, resp.ContentLength)
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	cfg, err := parseFlags(nil, io.Discard, nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.listen != ":8080" || cfg.prefix != "dart" || cfg.blockSize == 0 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if _, err := parseFlags([]string{"-listen", "127.0.0.1:0", "-cache-size", "1048576"}, io.Discard, nil); err != nil {
		t.Errorf("parseFlags with args: %v", err)
	}
}

func TestNewHandlerBuildsForTmpDir(t *testing.T) {
	cfg := config{cacheDir: filepath.Join(t.TempDir(), "sub"), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16, prefix: "dart"}
	n, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()
	h := n.client
	if h == nil {
		t.Error("nil handler")
	}
}

func TestRunErrors(t *testing.T) {
	// Flag parse error propagates.
	if err := Run([]string{"-nope"}, io.Discard, "test"); err == nil {
		t.Error("expected flag parse error")
	}
	// newHandler error (invalid chunk config) propagates before serving.
	if err := Run([]string{"-chunk-size", "10", "-block-size", "16", "-cache-dir", t.TempDir()}, io.Discard, "test"); err == nil {
		t.Error("expected newHandler error")
	}
}

func TestParsePeers(t *testing.T) {
	ms, err := parsePeers("A@127.0.0.1:9001, B@10.0.0.2:9002")
	if err != nil || len(ms) != 2 {
		t.Fatalf("parsePeers = %v, %v", ms, err)
	}
	if ms[0].ID != "A" || ms[0].Addr != "127.0.0.1:9001" || ms[0].State != cluster.Ready {
		t.Errorf("member 0 = %+v", ms[0])
	}
	for _, bad := range []string{"", "noat", "@addr", "id@"} {
		if _, err := parsePeers(bad); err == nil {
			t.Errorf("parsePeers(%q) expected error", bad)
		}
	}
}

// TestBuildRejectsSharedCacheDir: the arena open truncates, so a second instance
// on the same -cache-dir must be refused at the lock rather than wiping the
// first one's cache.
func TestBuildRejectsSharedCacheDir(t *testing.T) {
	cfg := config{cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16, prefix: "dart"}
	n1, err := build(cfg)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}

	if _, err := build(cfg); err == nil {
		n1.closer.Close()
		t.Fatal("second build on the same cache dir succeeded")
	} else if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("error = %v, want it to report the directory is in use", err)
	}

	// After the first instance shuts down the directory is reusable.
	if err := n1.closer.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	n2, err := build(cfg)
	if err != nil {
		t.Fatalf("build after release: %v", err)
	}
	n2.closer.Close()
}

// TestBuildRegistryMirror: -registry selects the mirror data plane, and an
// invalid upstream fails the build (releasing the cache-dir lock so a retry can
// start).
func TestBuildRegistryMirror(t *testing.T) {
	var hits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := config{
		cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16,
		prefix: "dart", registry: upstream.URL,
	}
	n, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()

	srv := httptest.NewServer(n.client)
	defer srv.Close()

	// The registry ping must reach the upstream, proving the mirror is mounted.
	resp, err := http.Get(srv.URL + "/v2/")
	if err != nil {
		t.Fatalf("GET /v2/: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/v2/ status = %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&hits) == 0 {
		t.Error("the request never reached the upstream registry")
	}

	// A bad upstream is rejected, and the lock must have been released so the
	// directory is usable again.
	bad := cfg
	bad.cacheDir = t.TempDir()
	bad.registry = "ftp://not-http"
	if _, err := build(bad); err == nil {
		t.Fatal("expected an invalid -registry to fail the build")
	}
	retry := bad
	retry.registry = upstream.URL
	n2, err := build(retry)
	if err != nil {
		t.Fatalf("build after a failed attempt: %v (lock leaked?)", err)
	}
	n2.closer.Close()
}

// TestBothFrontEndsShareOneCache is the reason the mirror and the prefix API
// coexist on one listener: OverlayBD reads layer ranges through its p2pConfig
// prefix API while containerd resolves and pulls through the registry mirror. A
// blob fetched by one must satisfy the other, since both key on its digest.
func TestBothFrontEndsShareOneCache(t *testing.T) {
	content := []byte("layer-bytes-shared-by-both-front-ends")
	digest := "sha256:" + strings.Repeat("ab", 32)
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, digest) {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&upstreamHits, 1)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(content))
	}))
	defer upstream.Close()

	blobPath := "/v2/library/app/blobs/" + digest
	cfg := config{
		cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16,
		prefix: "dart", registry: upstream.URL,
	}
	n, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()

	srv := httptest.NewServer(n.client)
	defer srv.Close()

	fetch := func(path string) []byte {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		return body
	}

	// 1. containerd pulls the blob through the registry mirror.
	if got := fetch(blobPath); !bytes.Equal(got, content) {
		t.Fatalf("mirror body = %q", got)
	}
	afterMirror := atomic.LoadInt32(&upstreamHits)
	if afterMirror == 0 {
		t.Fatal("the mirror never reached the upstream")
	}

	// 2. OverlayBD reads the same blob through the p2pConfig prefix API, which
	//    embeds the full upstream URL (including "//" that must not be cleaned).
	prefixPath := "/dart/" + upstream.URL + blobPath
	if got := fetch(prefixPath); !bytes.Equal(got, content) {
		t.Errorf("prefix API body = %q, want the same bytes", got)
	}
	if got := atomic.LoadInt32(&upstreamHits); got != afterMirror {
		t.Errorf("upstream hit %d more times; the two front-ends did not share the cache", got-afterMirror)
	}
}

// TestPrefixCollidingWithRegistryPathRejected: the two front-ends share a
// listener, so a prefix that shadows /v2/ must be refused at startup.
func TestPrefixCollidingWithRegistryPathRejected(t *testing.T) {
	var out strings.Builder
	if _, err := parseFlags([]string{"-registry", "https://example.com", "-prefix", "v2"}, &out, nil); err == nil {
		t.Error("expected -prefix v2 to be rejected alongside -registry")
	}
	// Without -registry there is no dispatcher, so the prefix is unconstrained.
	if _, err := parseFlags([]string{"-prefix", "v2"}, &out, nil); err != nil {
		t.Errorf("-prefix v2 without -registry should be allowed: %v", err)
	}
}

// TestBuildRegistryAuth: a private upstream that demands a token is served
// end-to-end through the real wiring, and the credential file is validated.
func TestBuildRegistryAuth(t *testing.T) {
	content := []byte("private-layer-bytes")
	const token = "issued-token"

	var tokenCalls, blobServed int32
	var authSrvURL atomic.Value
	authSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenCalls, 1)
		user, pass, ok := r.BasicAuth()
		if !ok || user != "robot" || pass != "secret" {
			http.Error(w, "bad credential", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{"token":"`+token+`","expires_in":300}`)
	}))
	defer authSrv.Close()
	authSrvURL.Store(authSrv.URL)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.Header().Set("Www-Authenticate",
				`Bearer realm="`+authSrvURL.Load().(string)+`/token",service="private"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		atomic.AddInt32(&blobServed, 1)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(content))
	}))
	defer upstream.Close()

	credFile := filepath.Join(t.TempDir(), "creds.json")
	host := strings.TrimPrefix(upstream.URL, "http://")
	if err := os.WriteFile(credFile,
		[]byte(`{"`+host+`":{"username":"robot","password":"secret"}}`), 0o600); err != nil {
		t.Fatalf("write creds: %v", err)
	}

	cfg := config{
		cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16,
		prefix: "dart", registry: upstream.URL, registryAuth: credFile,
	}
	n, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()

	srv := httptest.NewServer(n.client)
	defer srv.Close()
	blobPath := "/v2/private/app/blobs/sha256:" + strings.Repeat("cd", 32)

	resp, err := http.Get(srv.URL + blobPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (private upstream not authenticated)", resp.StatusCode)
	}
	if !bytes.Equal(body, content) {
		t.Errorf("body = %q, want the private layer", body)
	}
	if atomic.LoadInt32(&tokenCalls) == 0 {
		t.Error("the token exchange never happened")
	}

	// A missing or malformed credential file must fail the build, releasing the
	// cache-dir lock so a corrected retry can start.
	for _, bad := range []string{
		filepath.Join(t.TempDir(), "absent.json"),
		writeTemp(t, `{"host":{"username":"","password":""}}`),
		writeTemp(t, `not json`),
	} {
		badCfg := cfg
		badCfg.cacheDir = t.TempDir()
		badCfg.registryAuth = bad
		if _, err := build(badCfg); err == nil {
			t.Errorf("credential file %q should have failed the build", bad)
			continue
		}
		fixed := badCfg
		fixed.registryAuth = credFile
		n2, err := build(fixed)
		if err != nil {
			t.Errorf("retry after a bad credential file: %v (lock leaked?)", err)
			continue
		}
		n2.closer.Close()
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "creds.json")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func TestBuildP2PWiring(t *testing.T) {
	base := config{cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16, prefix: "dart"}

	// Without -peers: single node, no peer handler.
	n1, err := build(base)
	if err != nil {
		t.Fatalf("build single-node: %v", err)
	}
	n1.closer.Close()
	if n1.peer != nil {
		t.Error("single-node build should have nil peer handler")
	}

	// With -peers but no -self-id: error.
	p2p := base
	p2p.cacheDir = t.TempDir()
	p2p.peers = "A@127.0.0.1:9001,B@127.0.0.1:9002"
	if _, err := build(p2p); err == nil {
		t.Error("expected error: -peers without -self-id")
	}

	// With -peers and -self-id: peer handler present.
	p2p.selfID = "A"
	n2, err := build(p2p)
	if err != nil {
		t.Fatalf("build p2p: %v", err)
	}
	defer n2.closer.Close()
	if n2.client == nil || n2.peer == nil {
		t.Errorf("p2p build handlers: client=%v peer=%v", n2.client != nil, n2.peer != nil)
	}
}

// TestBuildAdmin: -admin non-empty yields a working admin handler exposing the
// engine's metrics; empty disables it.
func TestBuildAdmin(t *testing.T) {
	cfg := config{cacheDir: t.TempDir(), cacheSize: 1 << 20, chunkSize: 64, blockSize: 16, prefix: "dart", adminAddr: ":0"}
	n, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()
	if n.admin == nil {
		t.Fatal("expected an admin handler")
	}
	srv := httptest.NewServer(n.admin)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/metrics status = %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "dart_block_source_total") {
		t.Errorf("engine metrics not registered:\n%s", body)
	}
	// Store occupancy is sampled at scrape time, so the tiered store's classes
	// must appear without any traffic having happened.
	for _, want := range []string{
		`dart_store_blocks{class="owned"}`,
		`dart_store_blocks{class="borrowed"}`,
		`dart_store_slots{class="owned"}`,
		"dart_store_admit_rejected_total",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing store metric %q:\n%s", want, body)
		}
	}

	cfg.adminAddr = ""
	cfg.cacheDir = t.TempDir()
	n2, err := build(cfg)
	if err != nil {
		t.Fatalf("build no-admin: %v", err)
	}
	defer n2.closer.Close()
	if n2.admin != nil {
		t.Error("empty -admin should disable the admin handler")
	}
}

// TestRedactURLUserinfo pins issue #7: the startup banner goes to stdout
// (container logs), so a credential embedded in -origin/-registry must never
// be printed.
func TestRedactURLUserinfo(t *testing.T) {
	got := redactURLUserinfo("https://ci:secret-token@reg.example.com")
	if strings.Contains(got, "secret-token") || strings.Contains(got, "ci@") {
		t.Fatalf("redactURLUserinfo leaked credential: %q", got)
	}
	if got := redactURLUserinfo("https://reg.example.com"); got != "https://reg.example.com" {
		t.Fatalf("redactURLUserinfo(no creds) = %q", got)
	}
}
