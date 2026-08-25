package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// RosterPath is the URL path of the membership-exchange endpoint.
const RosterPath = "/peer/v1/roster"

// HeaderPeerAddr carries the caller's *advertised* peer address on a roster
// request.
//
// It cannot be derived from the connection: RemoteAddr's port is the caller's
// ephemeral source port, not the address it accepts block requests on.
const HeaderPeerAddr = "X-DART-Peer-Addr"

// maxRosterBody bounds a roster response. A roster is small — a few tens of bytes
// per member — and this exists so a misbehaving or hostile peer cannot make us
// allocate without limit. 4 MiB is room for well over 10k members.
const maxRosterBody = 4 << 20

// RosterMember is one entry of a roster on the wire.
//
// It carries only the *authoritative* fields. State is deliberately absent:
// liveness is derived locally by each node from its own reachability
// observations, and propagating it would both couple nodes to each other's
// transient failures and cause membership to flap cluster-wide. See
// cluster.Member.State.
type RosterMember struct {
	ID     string  `json:"id"`
	Addr   string  `json:"addr"`
	Weight float64 `json:"weight,omitempty"`
}

// Roster is the membership snapshot exchanged between peers.
type Roster struct {
	// Epoch is the sender's membership epoch, as a decimal string. It is a string
	// rather than a JSON number because an epoch is a full uint64 and JSON numbers
	// are doubles: values above 2^53 would silently lose precision in any
	// standards-conforming parser.
	Epoch string `json:"epoch"`
	// Members is the sender's view. It always includes the sender itself, which is
	// what lets a caller learn the sender's stable ID — DNS hands out addresses,
	// never identities.
	Members []RosterMember `json:"members"`
}

// RosterServer answers membership-exchange requests.
type RosterServer struct {
	// NodeID is this node's stable identity, echoed in HeaderNode.
	NodeID string
	// Src returns the roster to serve. Required.
	Src func() Roster
	// Learn, if set, is called with a caller's identity and advertised address.
	//
	// This makes the exchange bidirectional, which is not optional. A node whose
	// seed set is empty — the first one to start, or one configured only with
	// static seeds that do not yet resolve — is never anybody's seed either, so if
	// rosters only ever flowed towards the caller it could stay permanently
	// isolated. Learning from inbound requests closes that.
	Learn func(id, addr string)
}

func (s *RosterServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "roster is read-only", http.StatusMethodNotAllowed)
		return
	}
	if s.Src == nil {
		http.Error(w, "no roster source", http.StatusInternalServerError)
		return
	}
	if s.Learn != nil {
		if id, addr := r.Header.Get(HeaderNode), r.Header.Get(HeaderPeerAddr); id != "" && addr != "" {
			s.Learn(id, addr)
		}
	}

	roster := s.Src()
	body, err := json.Marshal(roster)
	if err != nil {
		http.Error(w, "encode roster", http.StatusInternalServerError)
		return
	}
	if s.NodeID != "" {
		w.Header().Set(HeaderNode, s.NodeID)
	}
	w.Header().Set(HeaderEpoch, roster.Epoch)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

// FetchRoster asks the node at addr for its membership view.
//
// selfID and selfAddr identify this node to the callee so the exchange is
// bidirectional; either may be empty, in which case the callee learns nothing
// from us.
//
// The second return value is the responder's own stable ID (from its
// self-identification header), which is how a caller credits liveness to the
// member that actually answered rather than to whichever member happens to
// advertise the dialed address. It is empty when the responder does not
// identify itself.
//
// Unlike a block fetch, this deliberately does **not** consult the circuit
// breaker for admission, and the difference matters. Skipping a peer whose circuit
// is open is right for a block: there is a grandparent and there is the origin. For
// a roster there is no alternative source — refusing to ask is refusing to ever
// learn about that node again. Worse, a whole cluster starting at once will always
// have some nodes dialing peers that are not listening yet, and a dial failure
// opens a circuit on the first attempt; gating discovery on that stalls convergence
// for the cooldown, and with a cyclic seed list it stalls every node at once.
//
// Outcomes are still *recorded*, which makes this doubly useful: because discovery
// keeps probing at a bounded interval, a peer that comes back has its circuit
// closed by the next successful roster fetch, so the data path recovers without
// waiting for a request to be spent on discovering it.
func (c *Client) FetchRoster(ctx context.Context, addr, selfID, selfAddr string) (Roster, string, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+RosterPath, nil)
	if err != nil {
		c.record(addr, outcomeSoftFail)
		return Roster{}, "", err
	}
	if selfID != "" {
		req.Header.Set(HeaderNode, selfID)
	}
	if selfAddr != "" {
		req.Header.Set(HeaderPeerAddr, selfAddr)
	}

	resp, err := c.http().Do(req)
	if err != nil {
		c.record(addr, classify(err))
		return Roster{}, "", fmt.Errorf("peer %s: roster: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.record(addr, outcomeSoftFail)
		return Roster{}, "", fmt.Errorf("peer %s: roster: unexpected status %s", addr, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRosterBody))
	if err != nil {
		c.record(addr, outcomeSoftFail)
		return Roster{}, "", fmt.Errorf("peer %s: roster: read: %w", addr, err)
	}
	var roster Roster
	if err := json.Unmarshal(body, &roster); err != nil {
		c.record(addr, outcomeSoftFail)
		return Roster{}, "", fmt.Errorf("peer %s: roster: decode: %w", addr, err)
	}

	// The responder's identity is the second return value, taken from its
	// self-identification header — an address is never an identity (a recycled
	// pod IP answers for a member that is gone), so callers must credit
	// liveness by this ID, not by the dialed address. It is empty when the
	// responder does not identify itself.
	//
	// If the responder named itself in the header but omitted itself from the body,
	// add it. That entry is the whole point of the exchange for a caller that only
	// has an address, so it is worth reconstructing rather than discarding the
	// response.
	responder := resp.Header.Get(HeaderNode)
	if responder != "" && !rosterHas(roster, responder) {
		roster.Members = append(roster.Members, RosterMember{ID: responder, Addr: addr, Weight: 1})
	}
	c.record(addr, outcomeAnswered)
	return roster, responder, nil
}

func rosterHas(r Roster, id string) bool {
	for _, m := range r.Members {
		if m.ID == id {
			return true
		}
	}
	return false
}
