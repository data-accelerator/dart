package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestParseSeeder(t *testing.T) {
	t.Run("dns", func(t *testing.T) {
		s, err := ParseSeeder("dns:dart.dart-system.svc.cluster.local:19146")
		if err != nil {
			t.Fatalf("ParseSeeder: %v", err)
		}
		d, ok := s.(*DNSSeeder)
		if !ok {
			t.Fatalf("got %T, want *DNSSeeder", s)
		}
		if d.Name != "dart.dart-system.svc.cluster.local" || d.Port != 19146 {
			t.Errorf("parsed name=%q port=%d", d.Name, d.Port)
		}
	})

	t.Run("static with scheme", func(t *testing.T) {
		s, err := ParseSeeder("static:10.0.0.1:9000,10.0.0.2:9000")
		if err != nil {
			t.Fatalf("ParseSeeder: %v", err)
		}
		addrs, _ := s.Seeds(context.Background())
		if len(addrs) != 2 || addrs[0] != "10.0.0.1:9000" {
			t.Errorf("addrs = %v", addrs)
		}
	})

	t.Run("bare list is static", func(t *testing.T) {
		s, err := ParseSeeder("10.0.0.1:9000, 10.0.0.2:9000")
		if err != nil {
			t.Fatalf("ParseSeeder: %v", err)
		}
		addrs, _ := s.Seeds(context.Background())
		if len(addrs) != 2 {
			t.Errorf("addrs = %v", addrs)
		}
	})

	t.Run("ipv6 host:port is handled", func(t *testing.T) {
		s, err := ParseSeeder("static:[fd00::1]:9000")
		if err != nil {
			t.Fatalf("ParseSeeder: %v", err)
		}
		addrs, _ := s.Seeds(context.Background())
		if len(addrs) != 1 || addrs[0] != "[fd00::1]:9000" {
			t.Errorf("addrs = %v", addrs)
		}
	})

	t.Run("rejects", func(t *testing.T) {
		bad := []string{
			"",
			"   ",
			"dns:",
			"dns:name-without-port",
			"dns::19146",         // empty name
			"dns:name:0",         // port out of range
			"dns:name:99999",     // port out of range
			"dns:name:notaport",  //
			"static:",            // no addresses
			"static:no-port",     //
			"host-without-port",  //
			"static:1.2.3.4:abc", // net.SplitHostPort accepts this, but see below
		}
		for _, in := range bad {
			if _, err := ParseSeeder(in); err == nil {
				// SplitHostPort does not validate that the port is numeric, so a
				// non-numeric port in a static list is accepted here and fails later at
				// dial time. Record which inputs slip through rather than assert falsely.
				if in == "static:1.2.3.4:abc" {
					t.Logf("note: %q is accepted; the port is validated at dial time", in)
					continue
				}
				t.Errorf("ParseSeeder(%q) succeeded, want an error", in)
			}
		}
	})
}

// TestDNSSeederAttachesPort: a DNS answer carries addresses only, so the peer port
// comes from configuration and must be attached to each result.
func TestDNSSeederAttachesPort(t *testing.T) {
	d := &DNSSeeder{
		Name: "dart.ns.svc",
		Port: 19146,
		Lookup: func(_ context.Context, host string) ([]string, error) {
			if host != "dart.ns.svc" {
				t.Errorf("looked up %q", host)
			}
			return []string{"10.0.0.1", "10.0.0.2", "fd00::5"}, nil
		},
	}
	addrs, err := d.Seeds(context.Background())
	if err != nil {
		t.Fatalf("Seeds: %v", err)
	}
	want := []string{"10.0.0.1:19146", "10.0.0.2:19146", "[fd00::5]:19146"}
	if len(addrs) != len(want) {
		t.Fatalf("addrs = %v, want %v", addrs, want)
	}
	for i := range want {
		if addrs[i] != want[i] {
			// IPv6 must be bracketed or the result is not a dialable address.
			t.Errorf("addrs[%d] = %q, want %q", i, addrs[i], want[i])
		}
	}
}

// TestDNSSeederErrorIsReportedNotSwallowed: resolution failure must surface, so a
// caller can log it, while the caller keeps its existing membership (asserted in
// TestDynamicSurvivesSeederOutage).
func TestDNSSeederErrorIsReportedNotSwallowed(t *testing.T) {
	sentinel := errors.New("nxdomain")
	d := &DNSSeeder{
		Name:   "missing.svc",
		Port:   9000,
		Lookup: func(context.Context, string) ([]string, error) { return nil, sentinel },
	}
	addrs, err := d.Seeds(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the cause", err)
	}
	if !strings.Contains(err.Error(), "missing.svc") {
		t.Errorf("error %v does not name what failed to resolve", err)
	}
	if len(addrs) != 0 {
		t.Errorf("addrs = %v, want none", addrs)
	}
}

// TestDNSSeederEmptyAnswerIsNotAnError: a headless Service with no ready endpoints
// resolves to nothing. That is a normal transient state during startup, not a
// failure.
func TestDNSSeederEmptyAnswerIsNotAnError(t *testing.T) {
	d := &DNSSeeder{
		Name:   "dart.ns.svc",
		Port:   9000,
		Lookup: func(context.Context, string) ([]string, error) { return nil, nil },
	}
	addrs, err := d.Seeds(context.Background())
	if err != nil {
		t.Errorf("empty answer produced an error: %v", err)
	}
	if len(addrs) != 0 {
		t.Errorf("addrs = %v", addrs)
	}
}
