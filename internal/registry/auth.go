package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Registry authentication.
//
// Private registries reject anonymous reads, and most of them (Docker Hub, GCR,
// GAR, Quay, Harbor in token mode) do it with the Distribution token flow rather
// than plain Basic:
//
//	1. the request is answered 401 with WWW-Authenticate: Bearer realm=...,service=...,scope=...
//	2. the client GETs that realm with its username/password to obtain a token
//	3. the request is retried with Authorization: Bearer <token>
//
// This is implemented as an http.RoundTripper rather than by threading a
// credential through the read path, for two reasons. First, no signature
// changes: the same transport serves both the cached blob fetches (via
// fetch.HTTPFetcher's client) and the mirror's pass-through proxy. Second, and
// decisively, fetch.Coalescing runs a shared fetch on a bounded background
// context, so a request-scoped credential carried in the context would be
// silently dropped for every deduplicated caller.
//
// The credential is therefore a property of the *upstream*, configured once,
// which is how a pull-through cache normally authenticates.

// LoadCredentials reads a JSON credential file mapping registry host to
// username/password:
//
//	{
//	  "registry-1.docker.io": {"username": "u", "password": "p"},
//	  "harbor.internal:5000": {"username": "robot$ci", "password": "..."}
//	}
//
// Hosts must match URL.Host exactly, including a non-default port, because that
// is the key the transport looks up. The key bounds where *resource requests*
// carry the credential; the token exchange additionally sends it to the realm
// the upstream's 401 designates (cross-host by design — see docs/registry.md
// §5 and AuthTransport's doc comment).
func LoadCredentials(path string) (map[string]Credential, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("registry: read credentials: %w", err)
	}
	var creds map[string]Credential
	if err := json.Unmarshal(raw, &creds); err != nil {
		return nil, fmt.Errorf("registry: parse credentials %q: %w", path, err)
	}
	for host, c := range creds {
		if host == "" {
			return nil, fmt.Errorf("registry: credentials %q: empty host key", path)
		}
		if c.Username == "" && c.Password == "" {
			return nil, fmt.Errorf("registry: credentials %q: host %q has no username or password", path, host)
		}
	}
	return creds, nil
}

// Credential is a username/password pair for a registry host.
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// basic returns the value for an Authorization: Basic header.
func (c Credential) basic() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(c.Username+":"+c.Password))
}

// AuthTransport adds registry credentials to outgoing requests, performing the
// Distribution token exchange when the registry asks for it.
//
// Credentials are keyed by host: resource requests carry a credential only to a
// host present in the map. That matters because registries redirect blob
// downloads to a CDN: the transport is invoked again for the redirect target,
// and sending the registry credential to an unrelated host would leak it.
//
// The token exchange is the deliberate exception: it GETs whatever realm the
// upstream's 401 WWW-Authenticate header designates — cross-host (e.g.
// registry-1.docker.io -> auth.docker.io) — carrying the credential. The realm
// is trusted as delivered; DART does not validate it (see docs/registry.md §5
// and docs/design-assumptions.md A2).
type AuthTransport struct {
	// Base carries the actual requests; nil uses http.DefaultTransport.
	Base http.RoundTripper
	// Creds maps host (as it appears in URL.Host) to a credential.
	Creds map[string]Credential

	mu       sync.Mutex
	tokens   map[string]*cachedToken // tokenKey -> token
	inflight map[string]*tokenCall   // tokenKey -> in-flight exchange
}

// tokenCall is one in-flight token exchange; followers wait on done and share
// the result (singleflight: N concurrent cold pulls cost one exchange).
type tokenCall struct {
	done      chan struct{}
	value     string
	expiresAt time.Time
	err       error
}

// maxCachedTokens bounds the token cache; past it, expired entries are swept
// before storing (a node's distinct repositories are few, so the sweep is rare).
const maxCachedTokens = 1024

// cachedToken is a bearer token with its expiry.
type cachedToken struct {
	value     string
	expiresAt time.Time
}

// tokenLeeway expires a cached token slightly early so a request in flight does
// not arrive with a token that has just lapsed.
const tokenLeeway = 30 * time.Second

func (t *cachedToken) valid(now time.Time) bool {
	return t != nil && t.value != "" && now.Add(tokenLeeway).Before(t.expiresAt)
}

var _ http.RoundTripper = (*AuthTransport)(nil)

// NewAuthTransport returns an AuthTransport for the given per-host credentials.
func NewAuthTransport(base http.RoundTripper, creds map[string]Credential) *AuthTransport {
	return &AuthTransport{Base: base, Creds: creds, tokens: make(map[string]*cachedToken)}
}

func (a *AuthTransport) base() http.RoundTripper {
	if a.Base != nil {
		return a.Base
	}
	return http.DefaultTransport
}

// RoundTrip implements http.RoundTripper.
func (a *AuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cred, haveCred := a.Creds[req.URL.Host]
	if !haveCred {
		// No credential for this host (e.g. a CDN we were redirected to): send the
		// request untouched rather than leaking someone else's credential.
		return a.base().RoundTrip(req)
	}

	// Attach a cached token if we already hold one for this repository, so the
	// steady state costs no extra round trip. A client-supplied Authorization
	// always wins: the client credential is the caller's own identity, and
	// silently replacing it with the operator's would misattribute the pull
	// (and leak the operator token where the client meant its own).
	clientAuth := req.Header.Get("Authorization")
	first := cloneRequest(req)
	scope := scopeFor(req.URL.Path)
	key := tokenKey(req.URL.Host, scope)
	usedToken := ""
	if clientAuth == "" {
		if tok, ok := a.cachedToken(key); ok {
			first.Header.Set("Authorization", "Bearer "+tok)
			usedToken = tok
		}
	}

	resp, err := a.base().RoundTrip(first)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}

	if usedToken != "" {
		// The cached token was just rejected: drop it, or the shared exchange's
		// cache double-check would hand the same dead token back. Conditional
		// on the value: a concurrent request may already have exchanged and
		// stored a fresh token under this key — deleting THAT would force a
		// pointless second exchange (PR #39 review).
		a.dropTokenIf(key, usedToken)
	}

	if clientAuth != "" {
		// The client's own credential was rejected: hand the 401 back rather
		// than substituting the operator credential (precedence above).
		return resp, nil
	}

	// The registry wants credentials. Satisfy the challenge and retry once.
	challenge := parseChallenge(resp.Header.Get("Www-Authenticate"))
	auth, err := a.authorize(req.Context(), challenge, cred, req.URL.Host, scope)
	if err != nil {
		// Return the original 401 rather than masking it: the caller sees the
		// registry's own response, which is more useful than our failure to
		// authenticate.
		return resp, nil
	}
	// Drain before closing so the connection can be reused; a 401 body is
	// small, but an undrained close tears the connection down every time.
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()

	retry := cloneRequest(req)
	retry.Header.Set("Authorization", auth)
	return a.base().RoundTrip(retry)
}

// authorize turns a challenge into an Authorization header value, exchanging the
// credential for a token when the challenge is Bearer.
//
// scope is the path-derived scope. It is what the token is *cached* under,
// deliberately: the cache is consulted before any challenge is seen, so only a
// key computable from the request keeps store and lookup symmetric. A registry
// that advertises a differently-formatted scope would otherwise make every
// request miss the cache and pay a fresh 401 plus token exchange.
func (a *AuthTransport) authorize(ctx context.Context, ch challenge, cred Credential, host, scope string) (string, error) {
	if !strings.EqualFold(ch.scheme, "bearer") {
		// Basic (or an unrecognized scheme we can still try Basic against):
		// Harbor and several managed registries accept Basic on /v2/ directly.
		return cred.basic(), nil
	}
	if ch.realm == "" {
		return "", errors.New("registry: bearer challenge without a realm")
	}
	// Ask for exactly what the registry requested; fall back to the derived scope
	// when the challenge omits it.
	if ch.scope == "" {
		ch.scope = scope
	}
	key := tokenKey(host, scope)
	// fetchTokenShared stores the token before unpublishing the in-flight
	// call, so by the time it returns the cache is already warm.
	tok, _, err := a.fetchTokenShared(ctx, key, ch, cred)
	if err != nil {
		return "", err
	}
	return "Bearer " + tok, nil
}

// tokenResponse is the token endpoint's reply.
type tokenResponse struct {
	Token       string `json:"token"`
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// fetchToken performs the token exchange against the challenge realm.
func (a *AuthTransport) fetchToken(ctx context.Context, ch challenge, cred Credential) (string, time.Time, error) {
	u, err := url.Parse(ch.realm)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("registry: bad token realm %q: %w", ch.realm, err)
	}
	q := u.Query()
	if ch.service != "" {
		q.Set("service", ch.service)
	}
	if ch.scope != "" {
		q.Set("scope", ch.scope)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", time.Time{}, err
	}
	if cred.Username != "" || cred.Password != "" {
		req.Header.Set("Authorization", cred.basic())
	}

	resp, err := a.base().RoundTrip(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("registry: token endpoint %s: %s", u.Host, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", time.Time{}, err
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", time.Time{}, fmt.Errorf("registry: decode token: %w", err)
	}
	tok := tr.Token
	if tok == "" {
		tok = tr.AccessToken // some registries use the OAuth2 field name
	}
	if tok == "" {
		return "", time.Time{}, errors.New("registry: token endpoint returned no token")
	}
	ttl := time.Duration(tr.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 60 * time.Second // the spec's default when expires_in is absent
	}
	return tok, time.Now().Add(ttl), nil
}

func (a *AuthTransport) cachedToken(key string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if t := a.tokens[key]; t.valid(time.Now()) {
		return t.value, true
	}
	return "", false
}

func (a *AuthTransport) storeToken(key, value string, expiresAt time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.storeTokenLocked(key, value, expiresAt)
}

// storeTokenLocked stores; caller must hold a.mu.
func (a *AuthTransport) storeTokenLocked(key, value string, expiresAt time.Time) {
	if a.tokens == nil {
		a.tokens = make(map[string]*cachedToken)
	}
	if len(a.tokens) >= maxCachedTokens {
		// Sweep expired entries first; if everything is somehow still valid,
		// drop the whole map — tokens are re-fetched on demand, and an
		// unbounded map is worse than a rare extra exchange.
		now := time.Now()
		for k, tok := range a.tokens {
			if !tok.valid(now) {
				delete(a.tokens, k)
			}
		}
		if len(a.tokens) >= maxCachedTokens {
			a.tokens = make(map[string]*cachedToken)
		}
	}
	a.tokens[key] = &cachedToken{value: value, expiresAt: expiresAt}
}

// dropTokenIf removes a cached token only if the entry still holds the
// rejected value — a concurrent re-exchange may already have replaced it.
func (a *AuthTransport) dropTokenIf(key, rejected string) {
	a.mu.Lock()
	if t := a.tokens[key]; t != nil && t.value == rejected {
		delete(a.tokens, key)
	}
	a.mu.Unlock()
}

// fetchTokenShared singleflights the exchange per cache key: concurrent cold
// pulls of one repository share one token endpoint round trip. The first
// caller starts the flight; every caller — the leader included — then waits
// for its result, and each caller's own ctx only bounds how long THAT caller
// waits. The flight itself runs on a bounded background context
// (runTokenExchange): one caller's cancellation must not poison the other
// waiters (issue #51) — the same flight semantics as fetch.Coalescing.
func (a *AuthTransport) fetchTokenShared(ctx context.Context, key string, ch challenge, cred Credential) (string, time.Time, error) {
	a.mu.Lock()
	// Double-check the cache: a sibling request may have completed the exchange
	// (and left the cache warm) between our first attempt and now.
	if tok := a.tokens[key]; tok.valid(time.Now()) {
		a.mu.Unlock()
		return tok.value, tok.expiresAt, nil
	}
	c, joined := a.inflight[key]
	if !joined {
		c = &tokenCall{done: make(chan struct{})}
		if a.inflight == nil {
			a.inflight = make(map[string]*tokenCall)
		}
		a.inflight[key] = c
	}
	a.mu.Unlock()
	if !joined {
		go a.runTokenExchange(key, c, ch, cred)
	}
	select {
	case <-c.done:
		return c.value, c.expiresAt, c.err
	case <-ctx.Done():
		return "", time.Time{}, ctx.Err()
	}
}

// maxTokenExchange bounds one shared token-exchange flight: a stalled token
// endpoint (accepted, then silent) must not pin the cache key forever. The
// bound is generous — the exchange is one small JSON GET — and expiring it
// fails the flight, unpublishes the key, and lets the next caller lead a
// fresh exchange.
const maxTokenExchange = 1 * time.Minute

// runTokenExchange completes one in-flight exchange and publishes the result.
// It runs detached from every caller's context (bounded by maxTokenExchange)
// so a cancelled caller cannot abort work its peers still wait on; a
// completed flight still warms the cache even if no waiter remains.
//
// The result goes to the cache BEFORE the in-flight marker is removed: a
// follower arriving after the delete but before a later store would see
// neither and lead a second exchange. Under this one critical section a new
// entrant always sees either the in-flight call or the warm cache. done is
// closed only after the result fields are written and the lock is released,
// so waiters observe a happens-before edge on the result and nothing runs
// under a.mu.
func (a *AuthTransport) runTokenExchange(key string, c *tokenCall, ch challenge, cred Credential) {
	flightCtx, cancel := context.WithTimeout(context.Background(), maxTokenExchange)
	c.value, c.expiresAt, c.err = a.fetchToken(flightCtx, ch, cred)
	cancel()

	a.mu.Lock()
	if c.err == nil {
		a.storeTokenLocked(key, c.value, c.expiresAt)
	}
	delete(a.inflight, key)
	a.mu.Unlock()
	close(c.done)
}

func tokenKey(host, scope string) string { return host + "|" + scope }

// challenge is a parsed WWW-Authenticate header.
type challenge struct {
	scheme  string
	realm   string
	service string
	scope   string
}

// parseChallenge parses a WWW-Authenticate value such as
//
//	Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"
func parseChallenge(v string) challenge {
	var ch challenge
	v = strings.TrimSpace(v)
	if v == "" {
		return ch
	}
	scheme, rest, _ := strings.Cut(v, " ")
	ch.scheme = scheme
	for _, part := range splitParams(rest) {
		k, val, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		val = strings.Trim(strings.TrimSpace(val), `"`)
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "realm":
			ch.realm = val
		case "service":
			ch.service = val
		case "scope":
			ch.scope = val
		}
	}
	return ch
}

// splitParams splits comma-separated parameters, ignoring commas inside quotes
// (a scope value may contain them).
func splitParams(s string) []string {
	var out []string
	var cur strings.Builder
	inQuotes := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuotes = !inQuotes
			cur.WriteByte(c)
		case c == ',' && !inQuotes:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// scopeFor derives the pull scope for a Registry v2 path, so a token can be
// cached and reused instead of triggering a 401 per request.
//
// It returns "" for paths with no repository (e.g. "/v2/"), which is the correct
// scope for the version check.
func scopeFor(path string) string {
	rest, ok := strings.CutPrefix(path, "/v2/")
	if !ok {
		return ""
	}
	// The repository name precedes the last /blobs/ or /manifests/ separator.
	for _, sep := range []string{"/blobs/", "/manifests/", "/tags/", "/referrers/"} {
		if i := strings.LastIndex(rest, sep); i > 0 {
			return "repository:" + rest[:i] + ":pull"
		}
	}
	return ""
}

// cloneRequest copies a request so RoundTrip never mutates its argument, as the
// http.RoundTripper contract requires.
func cloneRequest(r *http.Request) *http.Request {
	c := r.Clone(r.Context())
	c.Header = r.Header.Clone()
	if c.Header == nil {
		c.Header = make(http.Header)
	}
	return c
}
