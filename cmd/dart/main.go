// Command dart runs a DART caching-proxy node: an HTTP server that serves
// object byte ranges through the read-through engine (cache, peers, origin).
//
// This binary deliberately wires in only the dependency-free discovery schemes
// (DNS and static lists) so the main module keeps an empty go.sum. The dart-k8s
// variant (providers/k8s/cmd/dart-k8s) additionally registers the EndpointSlice
// scheme.
package main

import (
	"fmt"
	"os"

	"github.com/data-accelerator/dart/internal/node"
)

// version is stamped at build time via -ldflags "-X main.version=...". It is
// reported by -version and on startup so a running container can be identified.
var version = "dev"

// schemes is what this binary accepts in -discover=<scheme>:<spec>.
var schemes = []node.DiscoveryScheme{node.DNSScheme, node.StaticScheme}

func main() {
	if err := node.Run(os.Args[1:], os.Stdout, version, schemes...); err != nil {
		fmt.Fprintln(os.Stderr, "dart:", err)
		os.Exit(1)
	}
}
