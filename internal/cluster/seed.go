package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Seeder produces candidate peer addresses ("host:port"). It answers only "who
// might be out there", never "who is alive" — liveness is decided locally from
// reachability (see Member.State).
//
// A Seeder is the environment-specific half of discovery, and the only half. It
// is deliberately small and pluggable: Kubernetes, DNS, a static list and a flat
// file all reduce to this one method, whereas converging on a full membership set
// is done over DART's own peer connections and is the same everywhere.
type Seeder interface {
	// Seeds returns candidate peer addresses. Returning an empty slice with a nil
	// error is valid and means "nothing found right now".
	Seeds(ctx context.Context) ([]string, error)
}

// StaticSeeder returns a fixed address list.
type StaticSeeder []string

func (s StaticSeeder) Seeds(context.Context) ([]string, error) { return []string(s), nil }

// DNSSeeder resolves a name to peer addresses, which is how a headless Service is
// consumed: it resolves to the addresses of its ready endpoints.
//
// This works inside Kubernetes and outside it, needs no API credentials, no RBAC
// and no client library, and — unlike a gossip mesh — gives every node the *same*
// rendezvous name, so nodes converge on one membership set regardless of start
// order rather than risking a permanent split.
//
// Its limitations are real and are why DNS is only the seed: a DNS answer carries
// addresses but no identities (see Member.ID, which must be stable and is not an
// address), per-resolver TTL caching means nodes see slightly different answers
// for a while, and a single response can be truncated once a cluster is large.
// Membership is completed by exchanging rosters over the peer connections.
type DNSSeeder struct {
	// Name is the DNS name to resolve, e.g. "dart.dart-system.svc.cluster.local".
	Name string
	// Port is the peer port to attach to each resolved address.
	Port int
	// Lookup overrides address resolution; nil uses net.DefaultResolver.
	// Injected in tests.
	Lookup func(ctx context.Context, host string) ([]string, error)
}

// ParseSeeder builds a Seeder from a spec string:
//
//	dns:<name>:<port>     resolve a (headless) Service name
//	static:<a>,<b>,...    a fixed address list
//
// A bare value with no recognized scheme is treated as a static list, so
// "10.0.0.1:9000,10.0.0.2:9000" works as written.
func ParseSeeder(spec string) (Seeder, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("empty seed spec")
	}
	switch {
	case strings.HasPrefix(spec, "dns:"):
		rest := strings.TrimPrefix(spec, "dns:")
		host, portStr, err := net.SplitHostPort(rest)
		if err != nil {
			return nil, fmt.Errorf("seed spec %q: want dns:<name>:<port>: %w", spec, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("seed spec %q: bad port %q", spec, portStr)
		}
		if host == "" {
			return nil, fmt.Errorf("seed spec %q: empty DNS name", spec)
		}
		return &DNSSeeder{Name: host, Port: port}, nil

	case strings.HasPrefix(spec, "static:"):
		return parseStaticSeeder(strings.TrimPrefix(spec, "static:"), spec)

	default:
		return parseStaticSeeder(spec, spec)
	}
}

func parseStaticSeeder(list, spec string) (Seeder, error) {
	var out StaticSeeder
	for _, tok := range strings.Split(list, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(tok); err != nil {
			return nil, fmt.Errorf("seed spec %q: %q is not host:port: %w", spec, tok, err)
		}
		out = append(out, tok)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("seed spec %q: no addresses", spec)
	}
	return out, nil
}

// Seeds resolves the name and returns one address per result.
func (d *DNSSeeder) Seeds(ctx context.Context) ([]string, error) {
	lookup := d.Lookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupHost
	}
	hosts, err := lookup(ctx, d.Name)
	if err != nil {
		// A resolution failure is not fatal and must not clear membership: we keep
		// serving from the peers we already know. It is reported so a caller can
		// surface it.
		return nil, fmt.Errorf("resolve %s: %w", d.Name, err)
	}
	addrs := make([]string, 0, len(hosts))
	for _, h := range hosts {
		addrs = append(addrs, net.JoinHostPort(h, strconv.Itoa(d.Port)))
	}
	return addrs, nil
}
