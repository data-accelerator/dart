// Command dart-k8s is the DART node binary with Kubernetes-native discovery:
// everything cmd/dart accepts, plus -discover=k8s:<namespace>/<service>[/<port>]
// backed by an EndpointSlice watch (see the providers/k8s module).
//
// It exists as a separate binary so that the client-go dependency tree stays
// out of the main DART module; deploy it with the RBAC in deploy/k8s/rbac.yaml.
package main

import (
	"fmt"
	"os"

	"github.com/data-accelerator/dart/internal/node"
	"github.com/data-accelerator/dart/providers/k8s"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

// schemes is what this binary accepts in -discover=<scheme>:<spec>: the
// dependency-free schemes plus the EndpointSlice watch.
var schemes = []node.DiscoveryScheme{node.DNSScheme, node.StaticScheme, k8s.Scheme}

func main() {
	if err := node.Run(os.Args[1:], os.Stdout, version, schemes...); err != nil {
		fmt.Fprintln(os.Stderr, "dart-k8s:", err)
		os.Exit(1)
	}
}
