package node

// Regression tests for issue #17 (node bundle: N1, N3, N4, N5, N7).

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestWildcardAdvertiseRejected pins N3: an explicit -peer-advertise wildcard
// used to pass straight through to the roster — every peer that learned it
// would dial itself. All wildcard forms must be rejected; a loopback address
// stays valid (the local multi-node rig runs several nodes on one host).
func TestWildcardAdvertiseRejected(t *testing.T) {
	for _, bad := range []string{"0.0.0.0:9000", ":9000", "[::]:9000", "::1:9000",
		"0:0:0:0:0:0:0:0:9000", "[0::0]:9000", // IPv6 unspecified, long forms
		"[::%eth0]:9000"} { // zoned unspecified
		if _, err := advertisedAddr(bad, "127.0.0.1:9001"); err == nil {
			t.Errorf("advertisedAddr(%q) accepted, want a wildcard rejection", bad)
		}
	}
	// Loopback is a real address: single-host rigs advertise it legitimately.
	if got, err := advertisedAddr("127.0.0.1:9000", "127.0.0.1:9001"); err != nil || got != "127.0.0.1:9000" {
		t.Errorf("advertisedAddr(loopback) = %q, %v; want accepted verbatim", got, err)
	}
}

// TestOwnedFractionFlagValidation pins N4: an explicit out-of-range
// -owned-fraction used to be silently replaced with 0.8 by the store —
// "disable the owned budget" (0) silently became "reserve 80% for it".
// Flag parsing must fail startup; the store rejects < 0 / >= 1 too.
func TestOwnedFractionFlagValidation(t *testing.T) {
	for _, v := range []string{"0", "1", "1.5", "-0.5", "NaN", "+Inf"} {
		if _, err := parseFlags([]string{"-owned-fraction", v}, io.Discard, nil); err == nil {
			t.Errorf("-owned-fraction %s accepted, want a startup error", v)
		}
	}
	cfg, err := parseFlags([]string{"-owned-fraction", "0.08"}, io.Discard, nil)
	if err != nil {
		t.Fatalf("-owned-fraction 0.08: %v", err)
	}
	if cfg.ownedFraction != 0.08 {
		t.Errorf("ownedFraction = %v, want 0.08", cfg.ownedFraction)
	}
	// Unset → the default stays.
	cfg, err = parseFlags(nil, io.Discard, nil)
	if err != nil || cfg.ownedFraction != 0.8 {
		t.Errorf("default ownedFraction = %v, %v; want 0.8", cfg.ownedFraction, err)
	}
}

// TestParseByteSizeExact pins N5: plain byte counts above 2^53 used to round
// silently through float64 (9007199254740993 → 9007199254740992).
func TestParseByteSizeExact(t *testing.T) {
	for _, s := range []string{"9007199254740992", "9007199254740993", "9007199254740995"} {
		got, err := parseByteSize(s)
		if err != nil {
			t.Fatalf("parseByteSize(%q): %v", s, err)
		}
		if fmt.Sprint(got) != s {
			t.Errorf("parseByteSize(%q) = %d — silent float64 rounding", s, got)
		}
	}
	if _, err := parseByteSize("9223372036854775808"); err == nil {
		t.Error("int64 overflow must error, not wrap")
	}
	// The unit path stays fractional and (beyond 2^53) approximately right.
	if got, err := parseByteSize("1.5GiB"); err != nil || got != 1610612736 {
		t.Errorf("1.5GiB = %d, %v; want 1610612736", got, err)
	}
}

// TestDiscoveryErrorsRoutedAndThrottled pins N7: discovery errors went to
// os.Stderr unthrottled — unreachable for library callers and one line per
// refresh tick under a persistent outage. They now go to `out`, at most one
// line per minute with a suppression count.
func TestDiscoveryErrorsRoutedAndThrottled(t *testing.T) {
	var out bytes.Buffer
	log := newThrottledLogger(&out)

	log(fmt.Errorf("first failure"))
	if !strings.Contains(out.String(), "first failure") {
		t.Fatalf("first error not routed to out: %q", out.String())
	}
	for i := 0; i < 50; i++ {
		log(fmt.Errorf("flood %d", i))
	}
	if n := strings.Count(out.String(), "\n"); n != 1 {
		t.Fatalf("51 errors produced %d lines, want 1 (throttled)", n)
	}
}

// TestOutWriterIsSerialized pins the lockedWriter wrap: the banner, shutdown
// lines, and throttled discovery diagnostics all share the caller's out,
// which may not be concurrency-safe. Concurrent writers must not interleave
// bytes within a single Write call.
func TestOutWriterIsSerialized(t *testing.T) {
	w := &lockedWriter{w: io.Discard}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := w.Write([]byte("dart: discover: x\n")); err != nil {
					t.Errorf("Write: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}

// TestEarlyListenerFailureShutsSiblings pins N1: when one server fails at
// startup (here the client listener, pre-occupied), Run used to return the
// bind error while the peer and admin servers kept serving — over a store the
// deferred closer had already closed. All siblings must be down when Run
// returns.
func TestEarlyListenerFailureShutsSiblings(t *testing.T) {
	// Occupy the client port.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	// Reserve free ports for peer/admin, then release them for dart to bind.
	freePort := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		return addr
	}
	peerAddr, adminAddr := freePort(), freePort()

	runErr := make(chan error, 1)
	go func() {
		runErr <- Run([]string{
			"-listen", occupied.Addr().String(),
			"-peer-listen", peerAddr, "-peer-advertise", "127.0.0.1:1",
			"-self-id", "n1", "-peers", "n1@127.0.0.1:1",
			"-admin", adminAddr,
			"-cache-dir", t.TempDir(), "-cache-size", "1MiB",
			"-chunk-size", "64KiB", "-block-size", "16KiB", "-mem-size", "16KiB",
		}, io.Discard, "test")
	}()

	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("Run with an occupied client port returned nil error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s on a bind failure")
	}

	// By the time Run returns, the siblings must refuse connections.
	for name, addr := range map[string]string{"peer": peerAddr, "admin": adminAddr} {
		deadline := time.Now().Add(2 * time.Second)
		for {
			c, dialErr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if dialErr != nil {
				break // refused: shut down ✓
			}
			c.Close()
			if time.Now().After(deadline) {
				t.Fatalf("%s server at %s still accepting after Run returned", name, addr)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
