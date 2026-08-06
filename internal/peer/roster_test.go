package peer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func rosterServer(t *testing.T, s *RosterServer) (addr string, close func()) {
	t.Helper()
	srv := httptest.NewServer(s)
	return strings.TrimPrefix(srv.URL, "http://"), srv.Close
}

// TestRosterRoundTrip is the basic contract: a caller with only an address learns
// the responder's stable identity plus everyone the responder knows.
func TestRosterRoundTrip(t *testing.T) {
	addr, done := rosterServer(t, &RosterServer{
		NodeID: "node-b",
		Src: func() Roster {
			return Roster{
				Epoch: "12345",
				Members: []RosterMember{
					{ID: "node-b", Addr: "10.0.0.2:9000", Weight: 1},
					{ID: "node-c", Addr: "10.0.0.3:9000", Weight: 2},
				},
			}
		},
	})
	defer done()

	c := NewClient()
	got, err := c.FetchRoster(context.Background(), addr, "node-a", "10.0.0.1:9000")
	if err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}
	if got.Epoch != "12345" {
		t.Errorf("epoch = %q", got.Epoch)
	}
	if len(got.Members) != 2 {
		t.Fatalf("members = %+v", got.Members)
	}
	if got.Members[1].Weight != 2 {
		t.Errorf("weight not carried: %+v", got.Members[1])
	}
}

// TestRosterCarriesNoState pins that liveness never travels. A peer's opinion about
// who is up must not be importable, or one node's transient failure would become
// cluster-wide membership churn.
func TestRosterCarriesNoState(t *testing.T) {
	b, err := json.Marshal(Roster{
		Epoch:   "1",
		Members: []RosterMember{{ID: "x", Addr: "10.0.0.1:9000", Weight: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, forbidden := range []string{"state", "State", "suspect", "Suspect", "ready", "Ready"} {
		if strings.Contains(s, forbidden) {
			t.Errorf("roster JSON %s contains %q; state must not be on the wire", s, forbidden)
		}
	}
	// And an unknown field on the wire is ignored rather than breaking decoding.
	var r Roster
	if err := json.Unmarshal([]byte(`{"epoch":"7","members":[{"id":"y","addr":"h:1","state":3}]}`), &r); err != nil {
		t.Fatalf("decoding a roster with an extra field failed: %v", err)
	}
	if len(r.Members) != 1 || r.Members[0].ID != "y" {
		t.Errorf("decoded %+v", r.Members)
	}
}

// TestRosterEpochIsStringForPrecision: an epoch is a full uint64 and JSON numbers
// are doubles, so a numeric epoch above 2^53 would be silently corrupted.
func TestRosterEpochIsStringForPrecision(t *testing.T) {
	const big = uint64(11942282109320200150) // > 2^53
	want := strconv.FormatUint(big, 10)

	addr, done := rosterServer(t, &RosterServer{
		NodeID: "n",
		Src:    func() Roster { return Roster{Epoch: want, Members: []RosterMember{{ID: "n", Addr: "h:1"}}} },
	})
	defer done()

	c := NewClient()
	got, err := c.FetchRoster(context.Background(), addr, "", "")
	if err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}
	if got.Epoch != want {
		t.Fatalf("epoch = %q, want %q", got.Epoch, want)
	}
	back, err := strconv.ParseUint(got.Epoch, 10, 64)
	if err != nil || back != big {
		t.Errorf("epoch did not survive the round trip: %v (%v)", back, err)
	}

	// Show what the alternative would have cost: as a JSON number it is corrupted.
	var asNum struct{ E float64 }
	_ = json.Unmarshal([]byte(`{"E":11942282109320200150}`), &asNum)
	if uint64(asNum.E) == big {
		t.Log("note: this platform happened to preserve the value; the string form is still required")
	}
}

// TestRosterLearnsCaller is the inbound half of the exchange. Without it, a node
// that started first — and so had nothing to seed from — could never be told about
// anyone, and nobody would ever be told about it.
func TestRosterLearnsCaller(t *testing.T) {
	type learned struct{ id, addr string }
	var got []learned

	addr, done := rosterServer(t, &RosterServer{
		NodeID: "server",
		Src:    func() Roster { return Roster{Epoch: "1", Members: []RosterMember{{ID: "server", Addr: "s:1"}}} },
		Learn:  func(id, a string) { got = append(got, learned{id, a}) },
	})
	defer done()

	c := NewClient()
	if _, err := c.FetchRoster(context.Background(), addr, "caller", "10.9.9.9:9000"); err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}
	if len(got) != 1 || got[0].id != "caller" || got[0].addr != "10.9.9.9:9000" {
		t.Fatalf("server learned %+v, want caller/10.9.9.9:9000", got)
	}

	// An anonymous caller teaches nothing rather than producing a bogus member.
	got = nil
	if _, err := c.FetchRoster(context.Background(), addr, "", ""); err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("server learned %+v from an anonymous caller", got)
	}
}

// TestRosterAdvertisedAddrNotRemoteAddr explains why the address is a header: the
// connection's remote port is the caller's ephemeral source port, not the port it
// serves blocks on.
func TestRosterAdvertisedAddrNotRemoteAddr(t *testing.T) {
	var learnedAddr, remoteAddr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteAddr = r.RemoteAddr
		(&RosterServer{
			NodeID: "s",
			Src:    func() Roster { return Roster{Epoch: "1", Members: []RosterMember{{ID: "s", Addr: "s:1"}}} },
			Learn:  func(_, a string) { learnedAddr = a },
		}).ServeHTTP(w, r)
	}))
	defer srv.Close()

	c := NewClient()
	advertised := "10.0.0.7:19146"
	if _, err := c.FetchRoster(context.Background(), strings.TrimPrefix(srv.URL, "http://"), "caller", advertised); err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}
	if learnedAddr != advertised {
		t.Errorf("learned %q, want the advertised %q", learnedAddr, advertised)
	}
	if learnedAddr == remoteAddr {
		t.Errorf("learned address equals RemoteAddr (%q); the ephemeral port would be unusable", remoteAddr)
	}
}

// TestRosterServerNamesItself: a responder that omits itself from the body is
// repaired from the header, because that entry is the only reason a caller holding
// just an address makes the request at all.
func TestRosterServerNamesItself(t *testing.T) {
	addr, done := rosterServer(t, &RosterServer{
		NodeID: "node-b",
		// Deliberately omits itself.
		Src: func() Roster { return Roster{Epoch: "3", Members: []RosterMember{{ID: "node-c", Addr: "c:1"}}} },
	})
	defer done()

	c := NewClient()
	got, err := c.FetchRoster(context.Background(), addr, "", "")
	if err != nil {
		t.Fatalf("FetchRoster: %v", err)
	}
	var found bool
	for _, m := range got.Members {
		if m.ID == "node-b" {
			found = true
			if m.Addr != addr {
				t.Errorf("reconstructed addr = %q, want %q", m.Addr, addr)
			}
		}
	}
	if !found {
		t.Errorf("responder identity missing from %+v", got.Members)
	}
}

func TestRosterServerRejectsWrites(t *testing.T) {
	addr, done := rosterServer(t, &RosterServer{
		NodeID: "n",
		Src:    func() Roster { return Roster{Epoch: "1"} },
	})
	defer done()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, _ := http.NewRequest(method, "http://"+addr+RosterPath, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s -> %d, want 405", method, resp.StatusCode)
		}
	}
}

// TestRosterFetchIgnoresOpenCircuit encodes a distinction that a first attempt at
// this got wrong.
//
// Gating a roster fetch on the circuit breaker looks consistent with block
// fetching, but it breaks discovery. Every node in a cluster that boots at once
// will dial some peer that is not listening yet; a dial failure is a hard failure
// and opens the circuit on the first attempt; and with discovery gated on the
// circuit, the node then refuses to retry for the whole cooldown. With a cyclic
// seed list every node stalls simultaneously. For a block there is always another
// source, so skipping is free; for a roster, refusing to ask is refusing to ever
// learn about that peer.
func TestRosterFetchIgnoresOpenCircuit(t *testing.T) {
	var hits int
	addr, done := rosterServer(t, &RosterServer{
		NodeID: "up",
		Src: func() Roster {
			hits++
			return Roster{Epoch: "1", Members: []RosterMember{{ID: "up", Addr: "u:1"}}}
		},
	})
	defer done()

	brk := NewBreaker(BreakerOptions{FailureThreshold: 1})
	c := NewClient()
	c.Breaker = brk

	// Force the circuit open, as a startup dial failure would.
	brk.RecordHardFailure(addr)
	if got := brk.State(addr); got != BreakerOpen {
		t.Fatalf("setup: circuit = %v, want open", got)
	}

	got, err := c.FetchRoster(context.Background(), addr, "me", "me:1")
	if err != nil {
		t.Fatalf("FetchRoster refused while the circuit was open: %v", err)
	}
	if hits != 1 {
		t.Errorf("server saw %d requests, want 1", hits)
	}
	if len(got.Members) == 0 {
		t.Error("no members returned")
	}

	// The success must still be recorded, so discovery doubles as the probe that
	// brings the data path back.
	if st := brk.State(addr); st != BreakerClosed {
		t.Errorf("circuit = %v after a successful roster fetch, want closed: "+
			"discovery should be what recovers a peer", st)
	}
}

// TestRosterFetchRecordsFailure: not gating on the breaker must not mean ignoring
// it. A roster fetch that cannot connect is still evidence the peer is down, and
// the data path should act on it.
func TestRosterFetchRecordsFailure(t *testing.T) {
	brk := NewBreaker(BreakerOptions{FailureThreshold: 5})
	c := NewClient()
	c.Breaker = brk

	const dead = "127.0.0.1:1" // nothing listens here
	if _, err := c.FetchRoster(context.Background(), dead, "a", "a:1"); err == nil {
		t.Fatal("expected a dial failure")
	}
	// A dial failure is a conclusion, not a suspicion: one is enough.
	if got := brk.State(dead); got != BreakerOpen {
		t.Errorf("circuit = %v after a failed roster dial, want open", got)
	}
}

// TestRosterFetchBadBody: a malformed response is an error, not a silently empty
// roster, or membership could be quietly emptied by a broken peer.
func TestRosterFetchBadBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()

	c := NewClient()
	if _, err := c.FetchRoster(context.Background(), strings.TrimPrefix(srv.URL, "http://"), "", ""); err == nil {
		t.Error("expected a decode error")
	}
}

func TestRosterServerNoSource(t *testing.T) {
	addr, done := rosterServer(t, &RosterServer{NodeID: "n"}) // Src is nil
	defer done()
	resp, err := http.Get("http://" + addr + RosterPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}
