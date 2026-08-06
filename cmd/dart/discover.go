package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

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

func (f *rosterFetcher) FetchRoster(ctx context.Context, addr string) ([]cluster.Member, error) {
	r, err := f.c.FetchRoster(ctx, addr, f.selfID, f.selfAddr)
	if err != nil {
		return nil, err
	}
	out := make([]cluster.Member, 0, len(r.Members))
	for _, m := range r.Members {
		if m.ID == "" || m.Addr == "" {
			continue // unusable: an identity with no address cannot be dialed
		}
		w := m.Weight
		if w <= 0 {
			w = 1
		}
		// State is deliberately not set from the wire; the provider treats a learned
		// member as Ready and derives liveness locally.
		out = append(out, cluster.Member{ID: m.ID, Addr: m.Addr, Weight: w})
	}
	return out, nil
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
		if _, _, err := net.SplitHostPort(explicit); err != nil {
			return "", fmt.Errorf("-peer-advertise %q is not host:port: %w", explicit, err)
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
	switch h {
	case "", "0.0.0.0", "::", "[::]":
		return true
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
