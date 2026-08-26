package cluster

// Regression tests for issue #21 item 2: the epoch serialization frames member
// fields with 0x1F/0x1E, so member IDs must exclude the framing control bytes.
// ValidMemberID defines the alphabet; roster ingest enforces it.

import (
	"strings"
	"testing"
	"time"
)

func TestValidMemberID(t *testing.T) {
	good := []string{"node-a", "worker-0.default.svc", "10.0.0.1", "a", "550e8400-e29b-41d4-a716-446655440000"}
	for _, s := range good {
		if !ValidMemberID(s) {
			t.Errorf("ValidMemberID(%q) = false, want true", s)
		}
	}
	bad := []string{"", "a\x1fb", "a\x1eb", "a\nb", "a b", "\t", "node\x00"}
	for _, s := range bad {
		if ValidMemberID(s) {
			t.Errorf("ValidMemberID(%q) = true, want false", s)
		}
	}
}

// TestRosterIngestRejectsControlByteIDs: a crafted ID containing the framing
// bytes used to be learnable — and could collide the epoch of a genuinely
// different membership (two nodes believing they agree). Learn must drop it.
func TestRosterIngestRejectsControlByteIDs(t *testing.T) {
	now := time.Now()
	d := NewDynamicProvider(DynamicConfig{
		Self:            Member{ID: "self", Addr: "127.0.0.1:9000", Weight: 1},
		Seeder:          StaticSeeder{},
		RefreshInterval: time.Hour,
		ForgetAfter:     time.Hour,
		Now:             func() time.Time { return now },
	})

	// The crafted ID from the issue: ID + 0x1F + weight bytes + 0x1E + "b".
	crafted := "a\x1f\x3f\xf0\x00\x00\x00\x00\x00\x00\x1eb"
	d.Learn(
		Member{ID: crafted, Addr: "127.0.0.1:9001", Weight: 1},
		Member{ID: "honest", Addr: "127.0.0.1:9002", Weight: 1},
	)
	d.mu.Lock()
	_, craftedKnown := d.known[crafted]
	_, honestKnown := d.known["honest"]
	d.mu.Unlock()
	if craftedKnown {
		t.Fatal("a control-byte member ID was ingested — it could collide the epoch framing")
	}
	if !honestKnown {
		t.Fatal("a legitimate member was dropped alongside the crafted one")
	}
}

// TestEpochCollisionInputsRejected pins the issue's crafted example end to
// end: the 1-member view whose ID embeds the framing of the 2-member view can
// no longer be constructed through any ingest path, so the epoch collision
// (verified at audit time as 8e3bc4f021964c93) is unreachable. NewView itself
// stays trusting — the enforcement is at the ingress, per the issue's
// non-breaking fix.
func TestEpochCollisionInputsRejected(t *testing.T) {
	// Direct NewView still computes whatever it is given (documented; the
	// framing bytes stay valid FNV input) — the guard is that no node can
	// *introduce* such an ID. Assert both sides of that boundary.
	w1 := [8]byte{0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // 1.0 big-endian
	crafted := "a" + string([]byte{0x1f}) + string(w1[:]) + string([]byte{0x1e}) + "b"
	if ValidMemberID(crafted) {
		t.Fatal("the crafted collision ID must fail alphabet validation")
	}
	// The two pre-guard views MUST still collide when constructed directly: that
	// is the defect the ingress guard exists for. An unconditional assertion —
	// if a future protocol change intentionally reframes the epoch, it must
	// update this assertion deliberately, not pass silently (issue #46).
	two := NewView([]Member{
		{ID: "a", Weight: 1},
		{ID: "b", Weight: 1},
	})
	one := NewView([]Member{{ID: crafted, Weight: 1}})
	if two.Epoch() != one.Epoch() {
		t.Fatal("crafted pre-guard views no longer demonstrate the framing collision")
	}
}

// TestInvalidRosterIDReportedViaOnError pins #46 item 1: a mixed roster feed
// must learn the valid member AND emit exactly one diagnostic naming the
// invalid one (safely quoted) — a silently dropped ID hides a mixed-version
// cluster behind a mysteriously missing member.
func TestInvalidRosterIDReportedViaOnError(t *testing.T) {
	now := time.Now()
	var diags []error
	d := NewDynamicProvider(DynamicConfig{
		Self:            Member{ID: "self", Addr: "127.0.0.1:9000", Weight: 1},
		Seeder:          StaticSeeder{},
		RefreshInterval: time.Hour,
		ForgetAfter:     time.Hour,
		Now:             func() time.Time { return now },
		OnError:         func(err error) { diags = append(diags, err) },
	})

	d.Learn(
		Member{ID: "bad\x1fid", Addr: "127.0.0.1:9001", Weight: 1},
		Member{ID: "honest", Addr: "127.0.0.1:9002", Weight: 1},
		Member{ID: "self", Addr: "127.0.0.1:9000", Weight: 1}, // self: silent
	)
	if len(diags) != 1 {
		t.Fatalf("got %d diagnostics, want exactly 1 (self must stay silent)", len(diags))
	}
	if !strings.Contains(diags[0].Error(), "bad") || !strings.Contains(diags[0].Error(), "invalid ID") {
		t.Fatalf("diagnostic does not describe the rejected member: %v", diags[0])
	}
	d.mu.Lock()
	_, honestKnown := d.known["honest"]
	d.mu.Unlock()
	if !honestKnown {
		t.Fatal("the valid sibling must still be learned")
	}
}
