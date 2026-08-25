package node

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
)

// RosterRoute is the exact path the roster server is mounted on. peer.RosterPath
// has no trailing slash, so registering it on a ServeMux matches only that path
// and does not shadow the "/peer/" block routes.
const RosterRoute = peer.RosterPath

// rosterFetcher adapts peer.Client to cluster.RosterFetcher.
//
// The adapter exists so that internal/cluster does not depend on a transport:
// membership is about identity and convergence, not about how bytes move, and the
// separation is what lets convergence be tested without sockets.
type rosterFetcher struct {
	c        *peer.Client
	selfID   string
	selfAddr string
}

func (f *rosterFetcher) FetchRoster(ctx context.Context, addr string) ([]cluster.Member, string, error) {
	r, responder, err := f.c.FetchRoster(ctx, addr, f.selfID, f.selfAddr)
	if err != nil {
		return nil, "", err
	}
	out := make([]cluster.Member, 0, len(r.Members))
	for _, m := range r.Members {
		if m.ID == "" || m.Addr == "" {
			continue // unusable: an identity with no address cannot be dialed
		}
		w := m.Weight
		// NaN cannot arrive here (JSON cannot represent it; A4).
		if w <= 0 {
			w = 1
		}
		// State is deliberately not set from the wire; the provider treats a learned
		// member as Ready and derives liveness locally.
		out = append(out, cluster.Member{ID: m.ID, Addr: m.Addr, Weight: w})
	}
	return out, responder, nil
}

// rosterOf renders a provider's current membership for the wire.
func rosterOf(d *cluster.DynamicProvider) peer.Roster {
	v := d.Current()
	out := peer.Roster{
		Epoch:   strconv.FormatUint(v.Epoch(), 10),
		Members: make([]peer.RosterMember, 0, v.Len()),
	}
	for _, m := range v.Members() {
		if m.Addr == "" {
			continue // nothing a caller could do with it
		}
		out.Members = append(out.Members, peer.RosterMember{
			ID: m.ID, Addr: m.Addr, Weight: m.Weight,
		})
	}
	return out
}

// advertisedAddr determines the address peers should use to reach this node.
//
// A listen address is frequently not a reachable address: ":9000" and
// "0.0.0.0:9000" name every interface, and telling a peer to connect to 0.0.0.0
// would have it dial itself. When the host part is missing or wildcard, it is taken
// from POD_IP / DART_ADVERTISE_HOST or, failing that, the hostname — which is what
// resolves inside a StatefulSet.
func advertisedAddr(explicit, listen string) (string, error) {
	if explicit != "" {
		h, _, err := net.SplitHostPort(explicit)
		if err != nil {
			return "", fmt.Errorf("-peer-advertise %q is not host:port: %w", explicit, err)
		}
		// An explicit wildcard advertisement is the exact harm this function
		// exists to prevent: every peer that learns it dials itself.
		if isWildcardHost(h) {
			return "", fmt.Errorf("-peer-advertise %q is a wildcard address: every peer that learned it would dial itself", explicit)
		}
		return explicit, nil
	}

	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("-peer-listen %q is not host:port: %w", listen, err)
	}
	if isWildcardHost(host) {
		h, err := advertiseHost()
		if err != nil {
			return "", fmt.Errorf("cannot determine an address to advertise for -peer-listen %q "+
				"(set -peer-advertise or POD_IP): %w", listen, err)
		}
		host = h
	}
	return net.JoinHostPort(host, port), nil
}

func isWildcardHost(h string) bool {
	if h == "" {
		return true
	}
	// Parse rather than string-match: the IPv6 unspecified address has many
	// spellings ("::", "[::]", "0:0:0:0:0:0:0:0", "[0::0]", ...) and every one
	// of them advertised to a peer means "dial yourself".
	h = strings.TrimPrefix(strings.TrimSuffix(h, "]"), "[")
	// A zone suffix ("::%eth0") is not parseable by net.ParseIP but the address
	// is still the unspecified one.
	if i := strings.IndexByte(h, '%'); i >= 0 {
		h = h[:i]
	}
	if ip := net.ParseIP(h); ip != nil {
		return ip.IsUnspecified()
	}
	return false
}

// advertiseHost picks a host others can reach us on. POD_IP is the Kubernetes
// convention (injected via the downward API) and is preferred because a hostname
// only resolves where a headless Service publishes it.
func advertiseHost() (string, error) {
	for _, env := range []string{"DART_ADVERTISE_HOST", "POD_IP"} {
		if v := os.Getenv(env); v != "" {
			return v, nil
		}
	}
	return os.Hostname()
}
