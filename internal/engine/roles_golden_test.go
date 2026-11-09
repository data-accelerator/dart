package engine

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
	"github.com/data-accelerator/dart/internal/tracker"
)

type roleGolden struct {
	Members []cluster.Member
	Cases   []struct {
		Key                  uint64
		Readers              []string
		Owners, Roots, Order []string
		Parents              map[string][]string
	}
}

func readRoleGolden(t *testing.T) roleGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "roles.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture roleGolden
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Members) != 9 || len(fixture.Cases) != 25 {
		t.Fatal("role fixture unexpectedly shrank")
	}
	return fixture
}

func roleIDs(nodes []hashring.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID
	}
	return out
}

// Values come only from contrib/golden/role_reference.py. Exercise the actual
// engine tree/classification, including tracker-empty and duplicate reader sets.
func TestRolePlacementAndTreeGolden(t *testing.T) {
	fixture := readRoleGolden(t)
	rng := rand.New(rand.NewSource(95))
	st := openStoreAt(t)
	for iteration := 0; iteration < 16; iteration++ {
		members := append([]cluster.Member(nil), fixture.Members...)
		rng.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
		provider := cluster.NewStaticProvider(members...)
		view := provider.Current()
		for _, tc := range fixture.Cases {
			opt := Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher(), Cluster: provider,
				Peer: peer.NewClient(), SelfID: "C", Role: cluster.NonRoot, Replicas: 2}
			if tc.Readers != nil {
				opt.TrackerRegistry = &tracker.Registry{}
			}
			e, err := New(opt)
			if err != nil {
				t.Fatal(err)
			}
			readers := append([]string(nil), tc.Readers...)
			rng.Shuffle(len(readers), func(i, j int) { readers[i], readers[j] = readers[j], readers[i] })
			addr, _, _ := e.trackerAddr(view, 123)
			e.rs["object"] = &readerSet{nodes: readers, expiresAt: time.Now().Add(time.Hour), basis: readerBasis{addr: addr, epoch: view.Epoch()}}
			if roots := roleIDs(hashring.Rank(tc.Key, view.Roots())); !reflect.DeepEqual(roots, tc.Roots) {
				t.Fatalf("key=%x roots=%v want=%v", tc.Key, roots, tc.Roots)
			}
			order, tracked := e.treeNodes(context.Background(), view, "object", 123, tc.Key)
			if tracked != (tc.Readers != nil) || !reflect.DeepEqual(roleIDs(order), tc.Order) {
				t.Fatalf("key=%x readers=%v order=%v want=%v", tc.Key, tc.Readers, roleIDs(order), tc.Order)
			}
			for fanout, expected := range tc.Parents {
				k, err := strconv.Atoi(fanout)
				if err != nil {
					t.Fatal(err)
				}
				got := make([]string, len(order))
				for i := range order {
					if p := hashring.Parent(i, len(order), k); p >= 0 {
						got[i] = order[p].ID
					}
				}
				if !reflect.DeepEqual(got, expected) {
					t.Fatalf("key=%x fanout=%d parents=%v want=%v", tc.Key, k, got, expected)
				}
			}
			for _, m := range members {
				e.selfID = m.ID
				want := store.Borrowed
				for _, owner := range tc.Owners {
					if m.ID == owner {
						want = store.Owned
					}
				}
				if got := e.classOf(tc.Key); got != want {
					t.Fatalf("key=%x member=%s class=%v want=%v", tc.Key, m.ID, got, want)
				}
			}
		}
	}
}

func TestRoleGoldenAgainstPythonReference(t *testing.T) {
	if os.Getenv("DART_GOLDEN_REF") != "1" {
		t.Skip("set DART_GOLDEN_REF=1 for the independent Python cross-check")
	}
	script := filepath.Join("..", "..", "contrib", "golden", "role_reference.py")
	out, err := exec.Command("python3", script).Output()
	if err != nil {
		t.Fatal(err)
	}
	var regenerated roleGolden
	if err := json.Unmarshal(out, &regenerated); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(regenerated, readRoleGolden(t)) {
		t.Fatal("role/tree fixture differs from independent Python reference")
	}
}
