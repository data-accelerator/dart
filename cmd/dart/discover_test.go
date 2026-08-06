package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
)

// TestAdvertisedAddrWildcard is the point of -peer-advertise: a listen address is
// often not a reachable one. Telling a peer to dial 0.0.0.0 would have it dial
// itself, so a wildcard host must be replaced with something routable.
func TestAdvertisedAddrWildcard(t *testing.T) {
	t.Setenv("DART_ADVERTISE_HOST", "10.1.2.3")

	for _, listen := range []string{":9000", "0.0.0.0:9000", "[::]:9000"} {
		got, err := advertisedAddr("", listen)
		if err != nil {
			t.Fatalf("advertisedAddr(%q): %v", listen, err)
		}
		if got != "10.1.2.3:9000" {
			t.Errorf("advertisedAddr(%q) = %q, want 10.1.2.3:9000", listen, got)
		}
		if strings.Contains(got, "0.0.0.0") || strings.HasPrefix(got, ":") {
			t.Errorf("advertisedAddr(%q) = %q is not dialable by a peer", listen, got)
		}
	}
}

// TestAdvertisedAddrPrefersPodIP: POD_IP is the Kubernetes convention, and is
// preferred over the hostname because a hostname only resolves where a headless
// Service publishes it.
func TestAdvertisedAddrPrefersPodIP(t *testing.T) {
	os.Unsetenv("DART_ADVERTISE_HOST")
	t.Setenv("POD_IP", "10.9.9.9")
	got, err := advertisedAddr("", ":19146")
	if err != nil {
		t.Fatalf("advertisedAddr: %v", err)
	}
	if got != "10.9.9.9:19146" {
		t.Errorf("advertisedAddr = %q, want 10.9.9.9:19146", got)
	}

	// An explicit override wins over both.
	t.Setenv("DART_ADVERTISE_HOST", "10.0.0.1")
	got, err = advertisedAddr("1.2.3.4:5555", ":19146")
	if err != nil {
		t.Fatalf("advertisedAddr: %v", err)
	}
	if got != "1.2.3.4:5555" {
		t.Errorf("explicit -peer-advertise ignored: got %q", got)
	}
}

// TestAdvertisedAddrKeepsConcreteHost: when the listen address already names a real
// interface, it is usable as-is and must not be rewritten.
func TestAdvertisedAddrKeepsConcreteHost(t *testing.T) {
	t.Setenv("DART_ADVERTISE_HOST", "should-not-be-used")
	got, err := advertisedAddr("", "10.5.5.5:9000")
	if err != nil {
		t.Fatalf("advertisedAddr: %v", err)
	}
	if got != "10.5.5.5:9000" {
		t.Errorf("advertisedAddr = %q, want the listen address unchanged", got)
	}
}

func TestAdvertisedAddrRejectsGarbage(t *testing.T) {
	for _, c := range []struct{ explicit, listen string }{
		{"not-host-port", ":9000"},
		{"", "no-port-here"},
	} {
		if got, err := advertisedAddr(c.explicit, c.listen); err == nil {
			t.Errorf("advertisedAddr(%q, %q) = %q, want an error", c.explicit, c.listen, got)
		}
	}
}

// TestRosterFetcherAdapter checks the translation between the wire form and
// membership, including that unusable entries are dropped rather than becoming
// members nothing can dial.
func TestRosterFetcherAdapter(t *testing.T) {
	srv := httptest.NewServer(&peer.RosterServer{
		NodeID: "node-b",
		Src: func() peer.Roster {
			return peer.Roster{
				Epoch: "42",
				Members: []peer.RosterMember{
					{ID: "node-b", Addr: "10.0.0.2:9000", Weight: 2},
					{ID: "node-c", Addr: "10.0.0.3:9000"}, // weight omitted -> 1
					{ID: "", Addr: "10.0.0.4:9000"},       // no identity: unusable
					{ID: "node-e", Addr: ""},              // no address: unusable
				},
			}
		},
	})
	defer srv.Close()

	f := &rosterFetcher{c: peer.NewClient(), selfID: "node-a", selfAddr: "10.0.0.1:9000"}
	ms, err := f.FetchRoster(context.Background(), strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}

	byID := map[string]cluster.Member{}
	for _, m := range ms {
		byID[m.ID] = m
	}
	if len(byID) != 2 {
		t.Fatalf("got %d usable members (%v), want 2", len(byID), byID)
	}
	if byID["node-b"].Weight != 2 {
		t.Errorf("weight lost: %+v", byID["node-b"])
	}
	if byID["node-c"].Weight != 1 {
		t.Errorf("omitted weight should default to 1: %+v", byID["node-c"])
	}
	for _, m := range ms {
		// State is a local judgement and must never arrive from the wire.
		if m.State != 0 {
			t.Errorf("member %s arrived with State=%v set from the wire", m.ID, m.State)
		}
	}
}

// TestRosterOfRendersCurrentView: what we serve must be what we believe, and must
// omit members a caller could not use.
func TestRosterOfRendersCurrentView(t *testing.T) {
	d := cluster.NewDynamicProvider(cluster.DynamicConfig{
		Self:   cluster.Member{ID: "self", Addr: "10.0.0.1:9000"},
		Seeder: cluster.StaticSeeder{},
	})
	d.Learn(
		cluster.Member{ID: "peer1", Addr: "10.0.0.2:9000", Weight: 3},
		cluster.Member{ID: "addrless", Addr: ""},
	)
	d.Refresh(context.Background())

	r := rosterOf(d)
	if r.Epoch != strings.TrimSpace(r.Epoch) || r.Epoch == "" {
		t.Errorf("epoch = %q, want a non-empty decimal string", r.Epoch)
	}
	// It must be parseable back as a uint64 by a peer.
	var probe struct{ Epoch string }
	b, _ := json.Marshal(r)
	if err := json.Unmarshal(b, &probe); err != nil {
		t.Fatalf("roster does not round-trip: %v", err)
	}

	ids := map[string]float64{}
	for _, m := range r.Members {
		ids[m.ID] = m.Weight
	}
	if _, ok := ids["self"]; !ok {
		t.Error("roster omits self; a caller could not learn our identity")
	}
	if w := ids["peer1"]; w != 3 {
		t.Errorf("peer1 weight = %v, want 3", w)
	}
	if _, ok := ids["addrless"]; ok {
		t.Error("a member with no address was advertised")
	}
}

// TestDiscoverAndPeersAreExclusive: one states membership, the other maintains it.
// Silently preferring one would hide a misconfiguration.
func TestDiscoverAndPeersAreExclusive(t *testing.T) {
	dir := t.TempDir()
	_, err := build(config{
		listen: "127.0.0.1:0", prefix: "dart", cacheDir: dir,
		cacheSize: 64 << 20, blockSize: 1 << 20, chunkSize: 8 << 20,
		selfID: "a", peerListen: "127.0.0.1:0",
		peers:    "a@127.0.0.1:1",
		discover: "static:127.0.0.1:2",
	})
	if err == nil {
		t.Fatal("expected an error when both -peers and -discover are set")
	}
	if !strings.Contains(err.Error(), "alternatives") {
		t.Errorf("error %v does not explain the conflict", err)
	}
}

// TestDiscoverRequiresSelfID: placement keys are derived from the identity, so
// starting without one would be meaningless rather than merely degraded.
func TestDiscoverRequiresSelfID(t *testing.T) {
	dir := t.TempDir()
	_, err := build(config{
		listen: "127.0.0.1:0", prefix: "dart", cacheDir: dir,
		cacheSize: 64 << 20, blockSize: 1 << 20, chunkSize: 8 << 20,
		peerListen: "127.0.0.1:0", discover: "static:127.0.0.1:2",
	})
	if err == nil || !strings.Contains(err.Error(), "self-id") {
		t.Errorf("error = %v, want a complaint about -self-id", err)
	}
}

// TestRosterServedOnPeerListener: the roster endpoint must be reachable on the peer
// listener without shadowing the block routes that share the /peer/ prefix.
func TestRosterServedOnPeerListener(t *testing.T) {
	dir := t.TempDir()
	n, err := build(config{
		listen: "127.0.0.1:0", prefix: "dart", cacheDir: dir,
		cacheSize: 64 << 20, blockSize: 1 << 20, chunkSize: 8 << 20,
		selfID: "node-a", peerListen: "127.0.0.1:0",
		peerAdvertise: "10.0.0.1:9000",
		discover:      "static:127.0.0.1:65535",
		adminAddr:     "",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer n.closer.Close()
	if n.peer == nil {
		t.Fatal("peer handler is nil with -discover set")
	}

	srv := httptest.NewServer(n.peer)
	defer srv.Close()

	resp, err := http.Get(srv.URL + peer.RosterPath)
	if err != nil {
		t.Fatalf("GET roster: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("roster status = %d, want 200", resp.StatusCode)
	}
	var r peer.Roster
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, m := range r.Members {
		if m.ID == "node-a" && m.Addr == "10.0.0.1:9000" {
			found = true
		}
	}
	if !found {
		t.Errorf("roster %+v does not advertise this node at its advertised address", r.Members)
	}

	// A block request must still reach the block server. RosterPath has no trailing
	// slash, so it matches exactly and must not shadow the "/peer/" subtree.
	resp2, err := http.Get(srv.URL + "/peer/v1/block/ff/0")
	if err != nil {
		t.Fatalf("GET block: %v", err)
	}
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	// We hold nothing, so the block server answers 404. Reaching *that* answer is
	// the assertion: a 405 or a JSON roster here would mean the routes collided.
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("block request -> %d %q, want 404 from the block server",
			resp2.StatusCode, strings.TrimSpace(string(body)))
	}
	if strings.Contains(string(body), "epoch") {
		t.Error("a block request was answered by the roster handler")
	}
}
