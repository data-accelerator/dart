package cluster

import (
	"reflect"
	"testing"
)

func TestRolePlacementAndEpoch(t *testing.T) {
	members := []Member{
		{ID: "node-a", Weight: 1, State: Ready, Role: Ordinary},
		{ID: "node-b", Weight: 2, State: Ready, Role: RootOnly},
		{ID: "node-c", Weight: 1, State: Ready, Role: NonRoot},
	}
	v := NewView(members)
	// Independently computed by contrib/golden/golden_reference.py: epoch with
	// roles {node-b:1,node-c:2}, including the dart-roles-v1 NUL prefix.
	const want uint64 = 14506959425017771709
	if v.Epoch() != want {
		t.Fatalf("epoch = %d, want %d", v.Epoch(), want)
	}
	if len(v.Roots()) != 2 || v.Roots()[0].ID != "node-a" || v.Roots()[1].ID != "node-b" {
		t.Fatalf("roots: %v", v.Roots())
	}
	if len(v.Readers()) != 2 || v.Readers()[0].ID != "node-a" || v.Readers()[1].ID != "node-c" {
		t.Fatalf("readers: %v", v.Readers())
	}
	members[0], members[2] = members[2], members[0]
	shuffled := NewView(members)
	if shuffled.Epoch() != v.Epoch() || !reflect.DeepEqual(shuffled.Members(), v.Members()) {
		t.Fatal("input order affects roles")
	}
	members[0].Role = Ordinary
	if NewView(members).Epoch() == v.Epoch() {
		t.Fatal("role change did not change epoch")
	}
	members[0].State = Suspect
	if got := NewView(members); len(got.Roots()) != 2 || len(got.Readers()) != 1 {
		t.Fatal("non-ready candidate included")
	}
}

func TestRoleConflictCanonicalization(t *testing.T) {
	a := Member{ID: "same", Weight: 1, State: Ready, Role: RootOnly}
	b := a
	b.Role = NonRoot
	x, y := NewView([]Member{a, b}), NewView([]Member{b, a})
	if !reflect.DeepEqual(x.Members(), y.Members()) || x.Epoch() != y.Epoch() {
		t.Fatal("conflicting roles depend on input order")
	}
}

func TestRoleCapabilities(t *testing.T) {
	for _, c := range []struct {
		role                Role
		root, reader, valid bool
		name                string
	}{
		{Ordinary, true, true, true, "ordinary"}, {RootOnly, true, false, true, "root-only"},
		{NonRoot, false, true, true, "non-root"}, {Role(255), false, false, false, "unknown"},
	} {
		if c.role.CanRoot() != c.root || c.role.CanRead() != c.reader || c.role.Valid() != c.valid || c.role.String() != c.name {
			t.Errorf("role %d has unexpected capabilities", c.role)
		}
	}
}

// A direct report controls roles even when a stale neighbor repeatedly gossips
// the old value; address gossip must still not refresh the contact clock.
func TestDirectRoleSurvivesStaleHearsay(t *testing.T) {
	d := NewDynamicProvider(DynamicConfig{Self: Member{ID: "self"}, Seeder: StaticSeeder{}})
	d.Learn(Member{ID: "peer", Role: Ordinary})
	d.LearnMember(Member{ID: "peer", Addr: "p:1", Role: NonRoot})
	d.Learn(Member{ID: "peer", Addr: "p:2", Role: Ordinary})
	d.LearnPeer("peer", "p:3")
	m, _ := d.publish().Get("peer")
	if m.Role != NonRoot || m.Addr != "p:3" {
		t.Fatalf("stale role won: %+v", m)
	}
	d.LearnMember(Member{ID: "peer", Addr: "p:4", Role: RootOnly})
	d.Learn(Member{ID: "peer", Role: NonRoot})
	m, _ = d.publish().Get("peer")
	if m.Role != RootOnly {
		t.Fatalf("direct role change lost: %+v", m)
	}
}

func TestUnknownRoleRejectedAtDiscovery(t *testing.T) {
	var errors int
	d := NewDynamicProvider(DynamicConfig{Self: Member{ID: "self"}, Seeder: StaticSeeder{}, OnError: func(error) { errors++ }})
	d.Learn(Member{ID: "bad", Role: Role(3)})
	d.LearnMember(Member{ID: "bad-direct", Role: Role(255)})
	if v := d.publish(); v.Len() != 1 || errors != 2 {
		t.Fatalf("invalid members accepted: %v, diagnostics %d", v.Members(), errors)
	}
}
