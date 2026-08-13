package node

import (
	"fmt"
	"strings"

	"github.com/data-accelerator/dart/internal/cluster"
)

// DiscoveryScheme binds a -discover scheme name to a Seeder constructor, which
// is how discovery is extended without this package growing a dependency: a
// command registers the schemes it ships, and Run dispatches
// -discover=<scheme>:<spec> to the matching New. Registration is explicit (an
// argument to Run), never an init() side effect, so the main dart binary stays
// dependency-free while a variant such as dart-k8s wires in a scheme backed by
// client-go.
//
// A constructed Seeder that needs a background lifecycle — an informer, a
// watch — should expose a Run(context.Context) method; Run starts it alongside
// the discovery loop and expects it to return when the context is cancelled.
// Seeders without one (DNS, static) need nothing beyond cluster.Seeder.
type DiscoveryScheme struct {
	// Name is the scheme as written in -discover=<name>:<spec> ("dns", "k8s").
	Name string
	// Usage renders the spec syntax in -discover's help text; empty defaults to
	// "<name>:<spec>".
	Usage string
	// New builds a Seeder from the spec part of the -discover value.
	New func(spec string) (cluster.Seeder, error)
}

// DNSScheme resolves a headless-Service name: -discover=dns:<name>:<port>.
var DNSScheme = DiscoveryScheme{
	Name:  "dns",
	Usage: "dns:<name>:<port> (a headless Service)",
	New:   func(spec string) (cluster.Seeder, error) { return cluster.ParseSeeder("dns:" + spec) },
}

// StaticScheme is a fixed address list: -discover=static:<a>,<b>,...
var StaticScheme = DiscoveryScheme{
	Name:  "static",
	Usage: "static:<a>,<b>,...",
	New:   func(spec string) (cluster.Seeder, error) { return cluster.ParseSeeder("static:" + spec) },
}

// resolveSeeder parses a -discover value of the form <scheme>:<spec> against
// the registered schemes. A value whose scheme is not registered falls back to
// cluster.ParseSeeder, which keeps the historical behaviors working: dns: and
// static: resolve even unregistered, and a bare "a:port,b:port" value is a
// static list. What the fallback deliberately cannot provide is a scheme this
// module has no code for — k8s: only works when its scheme is registered.
func resolveSeeder(discover string, schemes []DiscoveryScheme) (cluster.Seeder, error) {
	if name, spec, ok := strings.Cut(discover, ":"); ok {
		for _, s := range schemes {
			if s.Name == name {
				if s.New == nil {
					return nil, fmt.Errorf("discovery scheme %q has no constructor", name)
				}
				return s.New(spec)
			}
		}
	}
	return cluster.ParseSeeder(discover)
}

// schemeUsage renders the registered schemes for the -discover help text, so a
// binary's --help reflects exactly what it was linked with.
func schemeUsage(schemes []DiscoveryScheme) string {
	names := make([]string, 0, len(schemes))
	for _, s := range schemes {
		if s.Usage != "" {
			names = append(names, s.Usage)
			continue
		}
		names = append(names, s.Name+":<spec>")
	}
	if len(names) == 0 {
		return "dns:<name>:<port> (a headless Service) or static:<a>,<b>,..."
	}
	return strings.Join(names, ", ")
}
