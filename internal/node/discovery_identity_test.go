package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
)

// Exercise responder headers, roster decoding and provider admission together.
// The endpoint changes identity but keeps advertising its historical AZ entry,
// modeling a stale neighbor or an old-version observer during an upgrade.
func TestDiscoveryIdentityReplacementOverHTTP(t *testing.T) {
	var identity atomic.Value
	identity.Store("AZ")
	var endpoint string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := identity.Load().(string)
		roster := peer.Roster{Members: []peer.RosterMember{{ID: id, Addr: endpoint, Weight: 1}}}
		if id != "AZ" {
			roster.Members = append(roster.Members, peer.RosterMember{ID: "AZ", Addr: endpoint, Weight: 1})
		}
		handler := peer.RosterServer{NodeID: id, Src: func() peer.Roster { return roster }}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	endpoint = strings.TrimPrefix(server.URL, "http://")
	now := time.Unix(1700000000, 0)
	newObserver := func() *cluster.DynamicProvider {
		return cluster.NewDynamicProvider(cluster.DynamicConfig{
			Self: cluster.Member{ID: "observer", Addr: "observer:9000"}, Seeder: cluster.StaticSeeder{endpoint},
			Fetcher: &rosterFetcher{c: peer.NewClient(), selfID: "observer", selfAddr: "observer:9000"},
			Now:     func() time.Time { return now },
		})
	}
	observer := newObserver()
	if _, ok := observer.Refresh(context.Background()).Get("AZ"); !ok {
		t.Fatal("initial AZ not discovered")
	}
	identity.Store("B")
	for step := 1; step <= 120; step++ {
		now = now.Add(5 * time.Second)
		view := observer.Refresh(context.Background())
		if _, ok := view.Get("B"); !ok {
			t.Fatal("replacement B not discovered")
		}
		if _, ok := view.Get("AZ"); ok && step > 12 {
			t.Fatalf("expired AZ visible at step %d", step)
		}
		if step == 60 {
			observer = newObserver()
		}
	}
	identity.Store("AZ")
	if _, ok := observer.Refresh(context.Background()).Get("AZ"); !ok {
		t.Fatal("real AZ rejoin rejected")
	}
}
