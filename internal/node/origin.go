package node

import (
	"errors"
	"net/http"
)

// Non-root nodes have no local origin backend. Their frontend operations use
// the delegated transport, and block/metadata operations use the root path.
type deniedOriginTransport struct{}

func (deniedOriginTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		r.Body.Close()
	}
	return nil, errors.New("non-root node cannot contact origin directly")
}

// A registry ReverseProxy normally returns redirects to the local client. On
// an origin-isolated node, resolve them through the delegated transport instead
// so the client never needs a direct connection to the origin or its CDN.
type delegatedFrontendTransport struct{ base http.RoundTripper }

func (t delegatedFrontendTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.RequestURI = "" // ReverseProxy's transport input may retain server form.
	c := http.Client{Transport: t.base}
	return c.Do(r)
}
