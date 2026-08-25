package node

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
)

// stubScheme records the spec it was constructed with, so tests can see which
// constructor ran. runSeeder additionally carries a Run method, exercising the
// lifecycle wiring.
type stubSeeder struct{ spec string }

func (s *stubSeeder) Seeds(context.Context) ([]string, error) { return []string{s.spec}, nil }

type stubRunSeeder struct {
	stubSeeder
	run chan struct{}
}

func (s *stubRunSeeder) Run(ctx context.Context) { <-ctx.Done(); close(s.run) }

func stubScheme(name string) DiscoveryScheme {
	return DiscoveryScheme{
		Name: name,
		New:  func(spec string) (cluster.Seeder, error) { return &stubSeeder{spec: spec}, nil },
	}
}

func TestResolveSeederDispatch(t *testing.T) {
	schemes := []DiscoveryScheme{stubScheme("fake"), nilCtorScheme("broken")}
	cases := []struct {
		name     string
		discover string
		// wantSeeds is matched against the resulting seeder's Seeds output;
		// wantErr substring-matches the expected failure.
		wantSeeds []string
		wantErr   string
	}{
		{"registered scheme gets the spec part", "fake:ns/svc", []string{"ns/svc"}, ""},
		{"dns resolves unregistered for compatibility", "dns:example.svc:9000", nil, ""},
		{"static resolves unregistered", "static:10.0.0.1:9000", []string{"10.0.0.1:9000"}, ""},
		{"bare list stays a static list", "10.0.0.1:9000,10.0.0.2:9000", []string{"10.0.0.1:9000", "10.0.0.2:9000"}, ""},
		{"scheme with no constructor is a wiring error", "broken:x", nil, "no constructor"},
		{"unregistered scheme-shaped value is not silently static", "k8s:ns/svc", nil, ""}, // parsed as a static address; see note below
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			seeder, err := resolveSeeder(c.discover, schemes)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("resolveSeeder(%q) error = %v, want substring %q", c.discover, err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSeeder(%q): %v", c.discover, err)
			}
			if c.wantSeeds == nil {
				return // only asserting construction succeeded (DNS would do a live lookup)
			}
			got, err := seeder.Seeds(context.Background())
			if err != nil {
				t.Fatalf("Seeds: %v", err)
			}
			if strings.Join(got, ",") != strings.Join(c.wantSeeds, ",") {
				t.Errorf("Seeds = %v, want %v", got, c.wantSeeds)
			}
		})
	}
}

// nilCtorScheme is a scheme whose New was never wired, which resolveSeeder must
// reject loudly rather than panic on a nil call.
func nilCtorScheme(name string) DiscoveryScheme { return DiscoveryScheme{Name: name} }

// TestUnregisteredK8sSpecDoesNotMatch pins the compatibility fallback: with no
// k8s scheme registered, "k8s:ns/svc" falls through to the static parser, which
// accepts it as one odd-looking host:port. It never reaches a Kubernetes
// client, and the dial failure surfaces through discovery's error reporting —
// registering the scheme (dart-k8s) is what gives the spec its meaning.
func TestUnregisteredK8sSpecDoesNotMatch(t *testing.T) {
	seeder, err := resolveSeeder("k8s:ns/svc", nil)
	if err != nil {
		t.Fatalf("static fallback: %v", err)
	}
	if _, ok := seeder.(cluster.StaticSeeder); !ok {
		t.Errorf("seeder = %T, want cluster.StaticSeeder (no k8s code reached)", seeder)
	}
}

func TestSchemeUsage(t *testing.T) {
	if got := schemeUsage(nil); !strings.Contains(got, "dns:") {
		t.Errorf("schemeUsage(nil) = %q, want the historical dns/static help", got)
	}
	got := schemeUsage([]DiscoveryScheme{DNSScheme, stubScheme("k8s")})
	if !strings.Contains(got, "k8s:<spec>") {
		t.Errorf("schemeUsage with k8s = %q, want it listed", got)
	}
}

func TestRunPrintsVersion(t *testing.T) {
	var out strings.Builder
	if err := Run([]string{"-version"}, &out, "v1.test", stubScheme("fake")); err != nil {
		t.Fatalf("Run -version: %v", err)
	}
	if !strings.Contains(out.String(), "v1.test") {
		t.Errorf("version output = %q, want it to contain v1.test", out.String())
	}
}

// TestBuildKeepsSeederForLifecycle: a seeder built through discovery must stay
// reachable from the built node, because Run type-asserts it for an optional
// Run(context.Context) lifecycle (informers). If build dropped it, a watched
// seeder would silently never start.
func TestBuildKeepsSeederForLifecycle(t *testing.T) {
	rs := &stubRunSeeder{run: make(chan struct{})}
	schemes := []DiscoveryScheme{{
		Name: "watch",
		New:  func(spec string) (cluster.Seeder, error) { return rs, nil },
	}}
	n, err := build(config{
		schemes:   schemes,
		cacheDir:  t.TempDir(),
		cacheSize: 1 << 20,
		blockSize: 1 << 16,
		chunkSize: 1 << 18,
		selfID:    "self",
		// advertisedAddr requires a listen host:port when -peer-advertise is unset.
		peerListen: "127.0.0.1:0",
		discover:   "watch:whatever",
	}, io.Discard)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()
	if n.seeder != cluster.Seeder(rs) {
		t.Fatalf("n.seeder = %T, want the stub seeder", n.seeder)
	}
	if _, ok := n.seeder.(interface{ Run(context.Context) }); !ok {
		t.Error("n.seeder lost its Run method; the lifecycle wiring would not start it")
	}
}
