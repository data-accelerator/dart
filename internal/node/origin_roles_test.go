package node

import (
	"encoding/json"
	"fmt"
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
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/registry"
)

func TestNonRootRegistryAndPassthroughUseSupplyRoot(t *testing.T) {
	content := strings.Repeat("data", 16)
	var originCalls, tokens, cdnCalls atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cdnCalls.Add(1)
		if r.URL.Path == "/manifest" {
			_, _ = io.WriteString(w, `{"schemaVersion":2}`)
			return
		}
		_, _ = io.WriteString(w, content) // deliberately ignores Range
	}))
	defer cdn.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "root-user" || p != "root-password" {
			http.Error(w, "bad credentials", 401)
			return
		}
		tokens.Add(1)
		_, _ = io.WriteString(w, `{"token":"root-token","expires_in":300}`)
	}))
	defer auth.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer root-token" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="test",scope="repository:test:pull"`, auth.URL))
			http.Error(w, "authentication required", 401)
			return
		}
		if r.Header.Get(peer.HeaderOrigin) != "" {
			t.Error("peer routing header leaked to origin")
		}
		switch {
		case r.URL.Path == "/v2/test/manifests/redirect":
			http.Redirect(w, r, cdn.URL+"/manifest", http.StatusTemporaryRedirect)
		case strings.Contains(r.URL.Path, "/manifests/"):
			if r.Header.Get("Accept") != "application/vnd.oci.image.manifest.v1+json" || r.URL.RawQuery != "key=a%2Fb" {
				t.Errorf("manifest headers/query changed: %s %v", r.RequestURI, r.Header)
			}
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			_, _ = io.WriteString(w, `{"schemaVersion":2}`)
		case r.URL.Path == "/redirect":
			http.Redirect(w, r, cdn.URL, http.StatusTemporaryRedirect)
		case r.URL.Path == "/blind":
			_, _ = io.WriteString(w, content)
		default:
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader(content))
		}
	}))
	defer origin.Close()

	rootPeer := httptest.NewUnstartedServer(nil)
	leafPeer := httptest.NewUnstartedServer(nil)
	peers := "root@" + rootPeer.Listener.Addr().String() + "#root-only,leaf@" + leafPeer.Listener.Addr().String() + "#non-root"
	rootCfg := memoryConfig(t)
	rootCfg.selfID, rootCfg.role, rootCfg.peers = "root", cluster.RootOnly, peers
	rootCfg.peerListen = rootPeer.Listener.Addr().String()
	rootCfg.registry = origin.URL
	creds, err := json.Marshal(map[string]registry.Credential{strings.TrimPrefix(origin.URL, "http://"): {Username: "root-user", Password: "root-password"}})
	if err != nil {
		t.Fatal(err)
	}
	rootCfg.registryAuth = filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(rootCfg.registryAuth, creds, 0600); err != nil {
		t.Fatal(err)
	}
	root, err := build(rootCfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer root.closer.Close()
	rootPeer.Config.Handler = root.peer
	rootPeer.Start()
	defer rootPeer.Close()
	if root.client != nil {
		t.Fatal("root-only client endpoint exists")
	}

	leafCfg := memoryConfig(t)
	leafCfg.selfID, leafCfg.role, leafCfg.peers = "leaf", cluster.NonRoot, peers
	leafCfg.peerListen = leafPeer.Listener.Addr().String()
	leafCfg.registry = origin.URL // no credentials: only the root can authenticate
	leaf, err := build(leafCfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer leaf.closer.Close()
	leafPeer.Config.Handler = leaf.peer
	leafPeer.Start()
	defer leafPeer.Close()
	client := httptest.NewServer(leaf.client)
	defer client.Close()
	request := func(method, path, want string, status int) {
		t.Helper()
		r, _ := http.NewRequest(method, client.URL+path, nil)
		r.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")
		if strings.HasPrefix(path, "/dart/") {
			r.Header.Set("Range", "bytes=1-4")
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != status || string(b) != want {
			t.Fatalf("%s %s: status=%d body=%q err=%v", method, path, resp.StatusCode, b, err)
		}
	}
	manifest := "/v2/test/manifests/latest?key=a%2Fb"
	request("GET", manifest, `{"schemaVersion":2}`, 200)
	request("HEAD", manifest, "", 200)
	// Inspect the response without client-side redirects: the mirror itself
	// must follow through roots, never hand an origin URL to an isolated client.
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	redirected, err := noFollow.Get(client.URL + "/v2/test/manifests/redirect")
	if err != nil {
		t.Fatal(err)
	}
	redirectBody, redirectErr := io.ReadAll(redirected.Body)
	redirected.Body.Close()
	if redirected.StatusCode != 200 || redirectErr != nil || string(redirectBody) != `{"schemaVersion":2}` {
		t.Fatalf("manifest redirect escaped the gateway: %d %q %v", redirected.StatusCode, redirectBody, redirectErr)
	}
	blob := "/v2/test/blobs/sha256:" + strings.Repeat("a", 64)
	request("GET", blob, content, 200)
	request("GET", "/dart/"+origin.URL+"/blind", content, 200)
	request("HEAD", "/dart/"+origin.URL+"/blind", "", 200)
	request("GET", "/dart/"+origin.URL+"/redirect", content, 200)
	if tokens.Load() == 0 || cdnCalls.Load() == 0 {
		t.Fatal("authentication/redirect paths were not exercised")
	}

	// Non-root peer endpoints cannot be used as an alternate origin proxy.
	r, _ := http.NewRequest("GET", leafPeer.URL+peer.OriginPath, nil)
	r.Header.Set(peer.HeaderOrigin, origin.URL+"/blind")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("non-root supplied an origin operation: %d", resp.StatusCode)
	}

	rootPeer.Close()
	before := originCalls.Load()
	request("GET", blob, content, 200) // fully warm metadata + blocks remain usable
	r, _ = http.NewRequest("GET", client.URL+manifest, nil)
	resp, err = http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 502 || originCalls.Load() != before {
		t.Fatalf("failed root bypassed role restriction: status=%d origin=%d->%d", resp.StatusCode, before, originCalls.Load())
	}
}
