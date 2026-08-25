package registry

// Regression tests for issue #16 (registry bundle: R3, R6c, R6d).

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientAuthorizationTakesPrecedence pins R3: a client-supplied
// Authorization header was silently overwritten by the cached operator token
// (and substituted on the 401 retry), contradicting §8's pass-through promise.
// The client credential must reach the upstream verbatim — both cold and with
// a warm token cache — and its rejection must surface, not be masked.
func TestClientAuthorizationTakesPrecedence(t *testing.T) {
	tr := newTokenRegistry(t)
	at := NewAuthTransport(nil, tr.creds())

	// Warm the operator-token cache with a credential-less request.
	if resp, _ := doGet(t, at, tr.srv.URL+"/v2/library/nginx/blobs/sha256:abc"); resp.StatusCode != 200 {
		t.Fatal("warm-up request failed")
	}
	if tr.tokenIss.Load() != 1 {
		t.Fatalf("warm-up: %d token exchanges, want 1", tr.tokenIss.Load())
	}

	// Now a request carrying the client's own Authorization: the upstream must
	// see exactly that (not the cached operator token) on every attempt.
	var saw atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw.Store(r.Header.Get("Authorization"))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()
	at2 := NewAuthTransport(nil, tr.creds())
	at2.Creds[srv.Listener.Addr().String()] = Credential{Username: "user", Password: "pass"}

	c := &http.Client{Transport: at2}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/library/nginx/blobs/sha256:abc", nil)
	req.Header.Set("Authorization", "Bearer CLIENT-TOKEN")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want the client's 401 surfaced (no operator substitution)", resp.StatusCode)
	}
	if got, _ := saw.Load().(string); got != "Bearer CLIENT-TOKEN" {
		t.Fatalf("upstream saw %q, want the client credential verbatim", got)
	}
	if tr.tokenIss.Load() != 1 {
		t.Fatalf("a rejected client credential triggered an operator token exchange (%d total)", tr.tokenIss.Load())
	}
}

// TestConcurrentColdRequestsShareOneExchange pins R6d (singleflight): 8
// concurrent cold pulls of one repository used to fire 8 token exchanges.
// Looped: the publish ordering (cache warm before the in-flight marker is
// removed, one critical section) must hold across scheduling windows — a
// follower arriving in the store/unpublish gap used to lead a second
// exchange (found in PR review).
func TestConcurrentColdRequestsShareOneExchange(t *testing.T) {
	for round := 0; round < 25; round++ {
		tr := newTokenRegistry(t)
		at := NewAuthTransport(nil, tr.creds())
		path := tr.srv.URL + "/v2/library/nginx/blobs/sha256:abc"

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if resp, _ := doGet(t, at, path); resp.StatusCode != http.StatusOK {
					t.Errorf("status = %d", resp.StatusCode)
				}
			}()
		}
		wg.Wait()
		if got := tr.tokenIss.Load(); got != 1 {
			t.Fatalf("round %d: token endpoint called %d times for 8 concurrent cold requests, want 1", round, got)
		}
	}
}

// TestExpiredTokensPurged pins R6d (cache hygiene): the token cache must not
// grow without bound — expired entries are swept past the cap, and a cache
// full of still-valid entries resets rather than leaking forever.
func TestExpiredTokensPurged(t *testing.T) {
	at := NewAuthTransport(nil, nil)
	past := time.Now().Add(-time.Hour)
	for i := 0; i < maxCachedTokens; i++ {
		at.storeToken(fmt.Sprintf("h%d|s", i), "tok", past)
	}
	at.storeToken("new|s", "tok", time.Now().Add(time.Hour))
	if got := len(at.tokens); got > 2 {
		t.Fatalf("after sweeping expired entries the cache holds %d, want <= 2", got)
	}

	// A flood of still-valid tokens: the cache resets when full rather than
	// growing without bound — len never exceeds cap+1 no matter how many
	// distinct repositories pass through.
	future := time.Now().Add(time.Hour)
	for i := 0; i < 3*maxCachedTokens; i++ {
		at.storeToken(fmt.Sprintf("v%d|s", i), "tok", future)
	}
	if got := len(at.tokens); got > maxCachedTokens+1 {
		t.Fatalf("token cache grew to %d > cap %d; it must sweep or reset", got, maxCachedTokens+1)
	}
}

// TestPercentEncodedPathReachesUpstreamVerbatim pins R6c: the decoded URL.Path
// used to be concatenated raw into the upstream URL — a %3F in a repository
// name became a literal '?', truncating the path into a query and shifting the
// cache identity. The upstream must receive the client's exact encoding, and
// resolveBlob's identity URL must contain it.
func TestPercentEncodedPathReachesUpstreamVerbatim(t *testing.T) {
	var gotPath, gotRaw, gotQuery atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath.Store(r.URL.Path)
		gotRaw.Store(r.URL.EscapedPath())
		gotQuery.Store(r.URL.RawQuery)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	m := newMirror(t, upstream.URL)
	srv := httptest.NewServer(m)
	defer srv.Close()

	// A manifest path goes through the reverse proxy (blobs go to the engine).
	resp, err := http.Get(srv.URL + "/v2/a%3Fb/manifests/latest")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if p, _ := gotPath.Load().(string); p != "/v2/a?b/manifests/latest" {
		t.Fatalf("upstream decoded path = %q, want the full name preserved", p)
	}
	if q, _ := gotQuery.Load().(string); q != "" {
		t.Fatalf("upstream query = %q, want empty (the %%3F must stay in the path)", q)
	}
	if raw, _ := gotRaw.Load().(string); raw != "/v2/a%3Fb/manifests/latest" {
		t.Fatalf("upstream escaped path = %q, want the client's exact encoding", raw)
	}

	// And the engine-side identity URL carries the same encoding.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/a%3Fb/blobs/sha256:abc", nil)
	id := m.upstreamURL(req)
	if !strings.Contains(id, "%3F") || strings.Contains(id, "?b/") {
		t.Fatalf("identity URL %q lost or corrupted the encoded path", id)
	}
}
