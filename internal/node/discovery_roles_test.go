package node

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
)

// Exercise the real wire adapters, including a root with no outbound roster
// fetcher. Inbound self-reports must suffice to join that node to the cluster.
func TestDiscoveryMixedRolesConvergeAndChange(t *testing.T) {
	ctx := context.Background()
	type instance struct {
		provider atomic.Pointer[cluster.DynamicProvider]
		self     cluster.Member
	}
	newPeer := func(id string, role cluster.Role, seeds cluster.Seeder, outbound bool) *instance {
		n := &instance{}
		srv := httptest.NewUnstartedServer(nil)
		n.self = cluster.Member{ID: id, Addr: srv.Listener.Addr().String(), Weight: 1, Role: role}
		cfg := cluster.DynamicConfig{Self: n.self, Seeder: seeds, OnError: func(err error) { t.Error(err) }}
		if outbound {
			cfg.Fetcher = &rosterFetcher{c: peer.NewClient(), selfID: id, selfAddr: n.self.Addr, selfRole: role}
		}
		n.provider.Store(cluster.NewDynamicProvider(cfg))
		mux := http.NewServeMux()
		mux.Handle(peer.RosterPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d := n.provider.Load()
			(&peer.RosterServer{NodeID: id, Role: d.Self().Role,
				Src: func() peer.Roster { return rosterOf(d) },
				LearnMember: func(m peer.RosterMember) {
					d.LearnMember(cluster.Member{ID: m.ID, Addr: m.Addr, Weight: m.Weight, Role: m.Role})
				},
			}).ServeHTTP(w, r)
		}))
		srv.Config.Handler = mux
		srv.Start()
		t.Cleanup(srv.Close)
		return n
	}
	root := newPeer("supply", cluster.RootOnly, cluster.StaticSeeder{}, false)
	host, port, err := net.SplitHostPort(root.self.Addr)
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	dns := &cluster.DNSSeeder{Name: "shared-service", Port: portNumber,
		Lookup: func(context.Context, string) ([]string, error) { return []string{host}, nil },
	}
	consumer := newPeer("compute", cluster.NonRoot, dns, true)
	ordinary := newPeer("ordinary", cluster.Ordinary, cluster.StaticSeeder{root.self.Addr}, true)
	nodes := []*instance{consumer, ordinary, root}
	converge := func() {
		for pass := 0; pass < 3; pass++ {
			for _, n := range nodes {
				n.provider.Load().Refresh(ctx)
			}
		}
	}
	assertViews := func(wantRoots int) uint64 {
		t.Helper()
		want := root.provider.Load().Current()
		for _, n := range nodes {
			v := n.provider.Load().Current()
			if v.Len() != 3 || v.Epoch() != want.Epoch() || !reflect.DeepEqual(v.Members(), want.Members()) {
				t.Fatalf("%s failed convergence: %v, want %v", n.self.ID, v.Members(), want.Members())
			}
			if len(v.Roots()) != wantRoots {
				t.Fatalf("%s root candidates: %v", n.self.ID, v.Roots())
			}
			if m, _ := v.Get("compute"); m.Role != cluster.NonRoot {
				t.Fatal("discovery promoted a compute node")
			}
		}
		return want.Epoch()
	}
	// Before finding a supply peer the compute node has no root candidates.
	if len(consumer.provider.Load().Current().Roots()) != 0 {
		t.Fatal("startup inferred origin capability from an empty seed result")
	}
	converge()
	previous := assertViews(2)
	if len(root.provider.Load().Current().Readers()) != 2 {
		t.Fatal("root-only member entered the reader candidate set")
	}
	// A restart may change a role at the same stable identity/address. Neighbors
	// still gossip its old root-only role during the next concurrent exchange.
	replacement := root.self
	replacement.Role = cluster.NonRoot
	root.provider.Store(cluster.NewDynamicProvider(cluster.DynamicConfig{Self: replacement, Seeder: cluster.StaticSeeder{}}))
	converge()
	if assertViews(1) == previous {
		t.Fatal("role change did not change the converged epoch")
	}
}

func TestDiscoveryRejectsLegacyRoster(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/peer/v1/roster", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"epoch":"1","members":[{"id":"old","addr":"old:1"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	var failures atomic.Int32
	d := cluster.NewDynamicProvider(cluster.DynamicConfig{
		Self:    cluster.Member{ID: "compute", Role: cluster.NonRoot},
		Seeder:  cluster.StaticSeeder{srv.Listener.Addr().String()},
		Fetcher: &rosterFetcher{c: peer.NewClient(), selfID: "compute", selfRole: cluster.NonRoot},
		OnError: func(error) { failures.Add(1) },
	})
	if v := d.Refresh(context.Background()); v.Len() != 1 || len(v.Roots()) != 0 || failures.Load() != 1 {
		t.Fatalf("legacy peer entered role-aware membership: %v; failures=%d", v.Members(), failures.Load())
	}
}
