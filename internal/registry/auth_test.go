package registry

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tokenRegistry is a fake registry that speaks the Distribution token flow.
type tokenRegistry struct {
	srv       *httptest.Server
	authSrv   *httptest.Server
	token     atomic.Value // string: the token currently accepted
	expiresIn atomic.Int64 // seconds advertised by the token endpoint
	tokenIss  atomic.Int64 // token endpoint calls
	authedOK  atomic.Int64 // successfully authorized resource requests
	seenBasic atomic.Value // string: Authorization seen at the token endpoint
	scopes    struct {
		mu   sync.Mutex
		seen []string
	}
	body []byte
}

func newTokenRegistry(t *testing.T) *tokenRegistry {
	t.Helper()
	tr := &tokenRegistry{body: []byte("blob-content")}
	tr.token.Store("tok-1")
	tr.expiresIn.Store(300)

	tr.authSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tr.tokenIss.Add(1)
		tr.seenBasic.Store(r.Header.Get("Authorization"))
		tr.scopes.mu.Lock()
		tr.scopes.seen = append(tr.scopes.seen, r.URL.Query().Get("scope"))
		tr.scopes.mu.Unlock()

		// Require the operator credential before issuing a token.
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
		if r.Header.Get("Authorization") != want {
			http.Error(w, "bad credential", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{
			Token:     tr.token.Load().(string),
			ExpiresIn: int(tr.expiresIn.Load()),
		})
	}))
	t.Cleanup(tr.authSrv.Close)

	tr.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "Bearer " + tr.token.Load().(string)
		if r.Header.Get("Authorization") != want {
			// A real registry advertises the scope for the repository actually
			// requested, so derive it rather than hardcoding one.
			w.Header().Set("Www-Authenticate", fmt.Sprintf(
				`Bearer realm="%s/token",service="fake.registry",scope="%s"`,
				tr.authSrv.URL, scopeFor(r.URL.Path)))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		tr.authedOK.Add(1)
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(string(tr.body)))
	}))
	t.Cleanup(tr.srv.Close)
	return tr
}

func (tr *tokenRegistry) host() string { return strings.TrimPrefix(tr.srv.URL, "http://") }

func (tr *tokenRegistry) creds() map[string]Credential {
	return map[string]Credential{tr.host(): {Username: "user", Password: "pass"}}
}

func doGet(t *testing.T, rt http.RoundTripper, url string) (*http.Response, []byte) {
	t.Helper()
	c := &http.Client{Transport: rt}
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body
}

// TestAuthTransportTokenExchange is the core flow: a 401 with a Bearer challenge
// is turned into a token and the request is retried, transparently to the caller.
func TestAuthTransportTokenExchange(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())

	resp, body := doGet(t, at, tr.srv.URL+"/v2/library/nginx/blobs/sha256:abc")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != string(tr.body) {
		t.Errorf("body = %q", body)
	}
	if tr.tokenIss.Load() != 1 {
		t.Errorf("token endpoint called %d times, want 1", tr.tokenIss.Load())
	}
	if got, _ := tr.seenBasic.Load().(string); !strings.HasPrefix(got, "Basic ") {
		t.Errorf("token endpoint saw Authorization %q, want the operator credential", got)
	}
}

// TestAuthTransportCachesToken: the steady state must not pay a 401 plus a token
// exchange per block, or a large layer would cost hundreds of extra round trips.
func TestAuthTransportCachesToken(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())
	path := tr.srv.URL + "/v2/library/nginx/blobs/sha256:abc"

	for i := 0; i < 5; i++ {
		if resp, _ := doGet(t, at, path); resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status = %d", i, resp.StatusCode)
		}
	}
	if got := tr.tokenIss.Load(); got != 1 {
		t.Errorf("token endpoint called %d times across 5 requests, want 1", got)
	}
	if got := tr.authedOK.Load(); got != 5 {
		t.Errorf("registry authorized %d requests, want 5", got)
	}
}

// TestAuthTransportRefreshesRejectedToken: when the registry stops accepting a
// cached token (rotation, or an expiry we were not told about), the transport
// re-authenticates rather than failing the read.
func TestAuthTransportRefreshesRejectedToken(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())
	path := tr.srv.URL + "/v2/library/nginx/blobs/sha256:abc"

	if resp, _ := doGet(t, at, path); resp.StatusCode != http.StatusOK {
		t.Fatal("first request failed")
	}
	// The registry rotates the accepted token; our cached one is now stale.
	tr.token.Store("tok-2")

	resp, body := doGet(t, at, path)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status after rotation = %d, want 200 (the token was not refreshed)", resp.StatusCode)
	}
	if string(body) != string(tr.body) {
		t.Errorf("body = %q", body)
	}
	if got := tr.tokenIss.Load(); got != 2 {
		t.Errorf("token endpoint called %d times, want 2 (initial + refresh)", got)
	}
}

// TestAuthTransportShortTTLNotCached: a token whose advertised lifetime is
// already inside the safety leeway must not be reused, otherwise a request would
// go out with a token that lapses in flight.
func TestAuthTransportShortTTLNotCached(t *testing.T) {
	tr := newTokenRegistry(t)
	tr.expiresIn.Store(1) // 1s, well inside tokenLeeway
	at := NewAuthTransport(nil, tr.creds())
	path := tr.srv.URL + "/v2/library/nginx/blobs/sha256:abc"

	doGet(t, at, path)
	doGet(t, at, path)
	if got := tr.tokenIss.Load(); got != 2 {
		t.Errorf("token endpoint called %d times, want 2 (a near-expiry token must not be cached)", got)
	}
}

// TestAuthTransportNeverLeaksCredentialToOtherHosts is the security-critical
// property: registries redirect blob downloads to a CDN, the transport is invoked
// again for that host, and the registry credential must not travel with it.
func TestAuthTransportNeverLeaksCredentialToOtherHosts(t *testing.T) {
	var cdnAuth atomic.Value
	cdnAuth.Store("")
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cdnAuth.Store(r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "cdn-bytes")
	}))
	defer cdn.Close()

	tr := newTokenRegistry(t)
	// The registry redirects to the CDN once authorized.
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "Bearer " + tr.token.Load().(string)
		if r.Header.Get("Authorization") != want {
			w.Header().Set("Www-Authenticate", fmt.Sprintf(
				`Bearer realm="%s/token",service="fake.registry"`, tr.authSrv.URL))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, cdn.URL+"/signed/blob", http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()

	host := strings.TrimPrefix(redirecting.URL, "http://")
	at := NewAuthTransport(nil, map[string]Credential{host: {Username: "user", Password: "pass"}})

	resp, body := doGet(t, at, redirecting.URL+"/v2/library/nginx/blobs/sha256:abc")
	if resp.StatusCode != http.StatusOK || string(body) != "cdn-bytes" {
		t.Fatalf("status = %d body = %q", resp.StatusCode, body)
	}
	if got, _ := cdnAuth.Load().(string); got != "" {
		t.Errorf("the CDN received Authorization %q; the registry credential leaked", got)
	}
}

// TestAuthTransportUnknownHostUntouched: hosts with no configured credential are
// passed through unchanged.
func TestAuthTransportUnknownHostUntouched(t *testing.T) {
	var seen atomic.Value
	seen.Store("unset")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	at := NewAuthTransport(nil, map[string]Credential{"other.example": {Username: "u", Password: "p"}})
	if resp, _ := doGet(t, at, srv.URL+"/v2/"); resp.StatusCode != http.StatusOK {
		t.Fatal("request failed")
	}
	if got, _ := seen.Load().(string); got != "" {
		t.Errorf("unconfigured host saw Authorization %q, want none", got)
	}
}

// TestAuthTransportBasicChallenge: a registry that asks for Basic (Harbor and
// several managed registries do) is satisfied directly, with no token endpoint.
func TestAuthTransportBasicChallenge(t *testing.T) {
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != want {
			w.Header().Set("Www-Authenticate", `Basic realm="registry"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	at := NewAuthTransport(nil, map[string]Credential{host: {Username: "user", Password: "pass"}})
	resp, body := doGet(t, at, srv.URL+"/v2/library/nginx/blobs/sha256:abc")
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("status = %d body = %q", resp.StatusCode, body)
	}
	if calls.Load() != 2 {
		t.Errorf("registry saw %d requests, want 2 (challenge + retry)", calls.Load())
	}
}

// TestAuthTransportSurfacesRegistry401: if authentication cannot be completed the
// caller must see the registry's own 401, not an opaque transport error.
func TestAuthTransportSurfacesRegistry401(t *testing.T) {
	tr := newTokenRegistry(t)
	// Wrong operator credential: the token endpoint refuses to issue.
	at := NewAuthTransport(nil, map[string]Credential{
		tr.host(): {Username: "wrong", Password: "wrong"},
	})
	resp, _ := doGet(t, at, tr.srv.URL+"/v2/library/nginx/blobs/sha256:abc")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want the registry's 401", resp.StatusCode)
	}
}

// TestAuthTransportScopePerRepository: tokens are cached per repository, so one
// repository's token is not reused for another.
func TestAuthTransportScopePerRepository(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())

	doGet(t, at, tr.srv.URL+"/v2/library/nginx/blobs/sha256:abc")
	doGet(t, at, tr.srv.URL+"/v2/other/app/blobs/sha256:def")

	if got := tr.tokenIss.Load(); got != 2 {
		t.Errorf("token endpoint called %d times for two repositories, want 2", got)
	}
	tr.scopes.mu.Lock()
	seen := append([]string(nil), tr.scopes.seen...)
	tr.scopes.mu.Unlock()
	if len(seen) < 2 || seen[0] == seen[1] {
		t.Errorf("token scopes = %v, want distinct per repository", seen)
	}
}

// TestAuthTransportCachesDespiteScopeFormat guards the cache-key symmetry: a
// registry may advertise a scope in a form we would never derive (extra actions,
// different order). The token must still be cached, or every single block would
// pay a fresh 401 plus token exchange.
func TestAuthTransportCachesDespiteScopeFormat(t *testing.T) {
	tr := newTokenRegistry(t)
	// Re-point the registry at a challenge whose scope does not match scopeFor().
	tr.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+tr.token.Load().(string) {
			w.Header().Set("Www-Authenticate", fmt.Sprintf(
				`Bearer realm="%s/token",service="fake.registry",scope="repository:library/nginx:pull,push"`,
				tr.authSrv.URL))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		tr.authedOK.Add(1)
		_, _ = io.WriteString(w, string(tr.body))
	})

	at := NewAuthTransport(nil, tr.creds())
	path := tr.srv.URL + "/v2/library/nginx/blobs/sha256:abc"
	for i := 0; i < 4; i++ {
		if resp, _ := doGet(t, at, path); resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status = %d", i, resp.StatusCode)
		}
	}
	if got := tr.tokenIss.Load(); got != 1 {
		t.Errorf("token endpoint called %d times, want 1 (cache key must not depend on the advertised scope format)", got)
	}
}

func TestScopeFor(t *testing.T) {
	cases := map[string]string{
		"/v2/library/nginx/blobs/sha256:abc":    "repository:library/nginx:pull",
		"/v2/library/nginx/manifests/latest":    "repository:library/nginx:pull",
		"/v2/deep/nested/name/blobs/sha256:abc": "repository:deep/nested/name:pull",
		"/v2/library/nginx/tags/list":           "repository:library/nginx:pull",
		"/v2/":                                  "",
		"/v2/noseparator":                       "",
		"/not-registry":                         "",
	}
	for in, want := range cases {
		if got := scopeFor(in); got != want {
			t.Errorf("scopeFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseChallenge(t *testing.T) {
	ch := parseChallenge(`Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull,push"`)
	if !strings.EqualFold(ch.scheme, "bearer") {
		t.Errorf("scheme = %q", ch.scheme)
	}
	if ch.realm != "https://auth.docker.io/token" {
		t.Errorf("realm = %q", ch.realm)
	}
	if ch.service != "registry.docker.io" {
		t.Errorf("service = %q", ch.service)
	}
	// The comma inside the quoted scope must not split the parameter.
	if ch.scope != "repository:library/nginx:pull,push" {
		t.Errorf("scope = %q, want the comma preserved", ch.scope)
	}

	if got := parseChallenge(""); got.scheme != "" {
		t.Errorf("empty challenge = %+v", got)
	}
	if got := parseChallenge("Basic realm=\"r\""); !strings.EqualFold(got.scheme, "basic") || got.realm != "r" {
		t.Errorf("basic challenge = %+v", got)
	}
}

func TestAuthTransportConcurrent(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())
	path := tr.srv.URL + "/v2/library/nginx/blobs/sha256:abc"

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				c := &http.Client{Transport: at}
				resp, err := c.Get(path)
				if err != nil {
					t.Errorf("GET: %v", err)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("status = %d", resp.StatusCode)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestMirrorWithAuthTransport ties it together: a private upstream is served
// through the mirror, and the second pull is cached.
func TestMirrorWithAuthTransport(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())

	m := newMirrorWithTransport(t, tr.srv.URL, at)
	path := "/v2/library/nginx/blobs/sha256:" + strings.Repeat("ab", 32)

	resp, body := get(t, m, path, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if string(body) != string(tr.body) {
		t.Fatalf("body = %q, want the private blob", body)
	}
	authed := tr.authedOK.Load()

	_, body2 := get(t, m, path, nil)
	if string(body2) != string(tr.body) {
		t.Errorf("second pull body = %q", body2)
	}
	if got := tr.authedOK.Load(); got != authed {
		t.Errorf("upstream served %d more requests; the private blob was not cached", got-authed)
	}
}

// TestAuthTransportTokenRealmIsTrustedAsDelivered pins the documented contract
// (docs/registry.md §5): the token request goes to whatever realm the
// upstream's 401 designates — including a *different host* — carrying the
// operator credential verbatim. DART deliberately does not validate the realm
// (per-vendor token topologies differ; securing the upstream path is the
// operator's responsibility per SECURITY.md / design-assumptions A2). If that
// behavior ever changes (e.g. realm validation is added), this test must fail.
func TestAuthTransportTokenRealmIsTrustedAsDelivered(t *testing.T) {
	tr := newTokenRegistry(t)
	// The fixture's resource server and token endpoint are distinct httptest
	// servers, so this is inherently cross-host; assert that explicitly.
	if strings.TrimPrefix(tr.srv.URL, "http://") == strings.TrimPrefix(tr.authSrv.URL, "http://") {
		t.Fatal("fixture must use distinct hosts for resource and realm")
	}
	at := NewAuthTransport(nil, tr.creds())

	resp, _ := doGet(t, at, tr.srv.URL+"/v2/library/nginx/blobs/sha256:abc")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// The realm host received the operator credential verbatim (Basic user:pass).
	got, _ := tr.seenBasic.Load().(string)
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if got != want {
		t.Fatalf("realm host received %q, want the operator credential %q", got, want)
	}
}
