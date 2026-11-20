package cluster

import (
	"context"
	"testing"
	"time"
)

// A dead identity must stay absent, including when surviving peers learned it at
// different times and an old-version neighbor continues advertising it forever.
func TestDynamicExpiredIdentityDoesNotResurrect(t *testing.T) {
	for _, reused := range []bool{false, true} {
		name := "departure"
		addr := "gone"
		if reused {
			name, addr = "replacement", "b"
		}
		t.Run(name, func(t *testing.T) {
			c := newClock()
			f := &fakeFetcher{responders: map[string]string{"a": "A", "b": "B"}}
			a := NewDynamicProvider(DynamicConfig{Self: Member{ID: "A", Addr: "a"}, Seeder: StaticSeeder{"b"}, Fetcher: f, Now: c.now})
			b := NewDynamicProvider(DynamicConfig{Self: Member{ID: "B", Addr: "b"}, Seeder: StaticSeeder{"a"}, Fetcher: f, Now: c.now})
			dead := Member{ID: "AZ", Addr: addr}
			a.LearnMember(dead)
			a.publish()
			c.advance(30 * time.Second)
			b.LearnMember(dead)
			b.publish()
			for elapsed := 35; elapsed <= 600; elapsed += 5 {
				c.advance(5 * time.Second)
				f.set("a", append(a.Roster(), dead)...)
				f.set("b", append(b.Roster(), dead)...)
				a.Refresh(context.Background())
				b.Refresh(context.Background())
				if elapsed > 95 {
					for _, p := range []*DynamicProvider{a, b} {
						if hasID(p.Current(), "AZ") {
							t.Fatalf("t=%ds %s resurrected AZ: %v", elapsed, p.Self().ID, ids(p.Current()))
						}
						if p.Current().Len() != 2 {
							t.Fatalf("live peers lost: %v", ids(p.Current()))
						}
					}
				}
			}
			// A restarted observer must not admit AZ from the same stale roster either.
			restarted := NewDynamicProvider(DynamicConfig{Self: a.Self(), Seeder: StaticSeeder{"b"}, Fetcher: f, Now: c.now})
			for i := 0; i < 30; i++ {
				c.advance(5 * time.Second)
				if v := restarted.Refresh(context.Background()); hasID(v, "AZ") {
					t.Fatalf("restarted observer admitted stale AZ: %v", ids(v))
				}
			}
			// A real rejoin is accepted even after repeated stale reports. Inbound
			// confirmation must work even when this observer cannot dial the returning node.
			restarted.LearnMember(dead)
			if v := restarted.Refresh(context.Background()); !hasID(v, "AZ") {
				t.Fatal("direct inbound rejoin was rejected")
			}
		})
	}
}

func TestDynamicHearsayCandidateRequiresDirectContact(t *testing.T) {
	c := newClock()
	f := &fakeFetcher{}
	b := Member{ID: "B", Addr: "b"}
	x := Member{ID: "X", Addr: "x"}
	f.set("b", b, x)
	d := NewDynamicProvider(DynamicConfig{Self: Member{ID: "A", Addr: "a"}, Seeder: StaticSeeder{"b"}, Fetcher: f, Now: c.now})
	if v := d.Refresh(context.Background()); hasID(v, "X") || !hasID(v, "B") {
		t.Fatalf("hearsay admitted as member: %v", ids(v))
	}
	f.set("x", x)
	if v := d.Refresh(context.Background()); !hasID(v, "X") {
		t.Fatalf("candidate was not probed and admitted: %v", ids(v))
	}
}
