package cluster

import "testing"

func TestRosterDigest(t *testing.T) {
	a := Member{ID: "a", Addr: "10.0.0.1:9000", Weight: 1.5, Role: RootOnly}
	b := Member{ID: "b", Addr: "10.0.0.2:9000", Weight: 2, Role: NonRoot}
	v := NewView([]Member{b, a})
	// Computed independently with Python hashlib and struct.pack:
	// for each addressable member in ID order, pack each text field's byte
	// length as >Q, append its bytes, then pack weight and role as >dB.
	const want = "v1-debea35308d92049cef8e1e4ee74afafa6b5bab8606c799a978a7fdfc10faee8"
	if got := v.RosterDigest(); got != want {
		t.Fatalf("digest %s, want %s", got, want)
	}
	a.State = Suspect
	if got := NewView([]Member{a, {ID: "addrless"}, b}).RosterDigest(); got != want {
		t.Fatalf("non-wire changes altered digest: %s", got)
	}
	a.Addr = "10.0.0.5:9000"
	moved := NewView([]Member{a, b})
	if moved.Epoch() != v.Epoch() || moved.RosterDigest() == want {
		t.Fatal("address-only change must affect the roster digest alone")
	}
}
