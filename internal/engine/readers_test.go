package engine

import (
	"bytes"
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/tracker"
)

// nodeIDsOf extracts the IDs of ranked tree candidates.
func nodeIDsOf(ns []hashring.Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}

// TestTreeNodesPrefersReaderSet: with a tracker configured and several readers
// registered, the tree candidates are exactly the readers that have addresses —
// not every Ready member.
func TestTreeNodesPrefersReaderSet(t *testing.T) {
	reg := tracker.NewRegistry(tracker.Options{Tick: time.Millisecond, LeaseTTL: time.Minute})
	members := []cluster.Member{
		{ID: "A", Addr: "a:1", Weight: 1, State: cluster.Ready},
		{ID: "B", Addr: "b:1", Weight: 1, State: cluster.Ready},
		{ID: "C", Addr: "c:1", Weight: 1, State: cluster.Ready},
		{ID: "D", Addr: "d:1", Weight: 1, State: cluster.Ready},
	}
	prov := cluster.NewStaticProvider(members...)
	view := prov.Current()

	// Make the test deterministic: run as whichever member is the HRW tracker for
	// this file, so the local registry path is exercised without a remote tracker.
	const oid = "obj-1"
	fileKey := chunk.ChunkKey("dart", oid, -1)
	self := hashring.Rank(fileKey, view.Ready())[0].ID

	// The two readers are the tracker node plus one other member.
	other := "A"
	if other == self {
		other = "B"
	}

	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: self,
		TrackerRegistry: reg, TrackerClient: tracker.NewClient(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := e.fileKey(oid); got != fileKey {
		t.Fatalf("fileKey mismatch: %d vs %d", got, fileKey)
	}

	// Register the two readers and let the tick publish them.
	reg.Join(oid, self, 0)
	reg.Join(oid, other, 0)
	time.Sleep(5 * time.Millisecond)
	reg.Join(oid, other, 0)

	nodes, fromReaders := e.treeNodes(context.Background(), view, oid, fileKey, 12345)
	if !fromReaders {
		t.Fatalf("tree was not built from the reader set: %v", nodeIDsOf(nodes))
	}
	got := nodeIDsOf(nodes)
	root := hashring.Rank(12345, view.Roots())[0].ID
	expected := map[string]bool{root: true, self: true, other: true}
	if len(got) != len(expected) || got[0] != root {
		t.Fatalf("tree %v, want owner %s plus readers", got, root)
	}
	for _, id := range got {
		if !expected[id] {
			t.Fatalf("unrelated member %s included", id)
		}
	}
}

// Tracking disabled uses all Ready readers; missing snapshots use owner+self.
func TestTreeNodesWithoutReaderSnapshot(t *testing.T) {
	members := []cluster.Member{
		{ID: "A", Addr: "a:1", Weight: 1, State: cluster.Ready},
		{ID: "B", Addr: "b:1", Weight: 1, State: cluster.Ready},
		{ID: "C", Addr: "c:1", Weight: 1, State: cluster.Ready},
	}
	prov := cluster.NewStaticProvider(members...)

	// No tracker at all.
	e, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "A",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nodes, fromReaders := e.treeNodes(context.Background(), prov.Current(), "obj", 1, 2)
	if fromReaders {
		t.Error("expected all-member fallback without a tracker")
	}
	if len(nodes) != 3 {
		t.Errorf("candidates = %v, want all 3 Ready members", nodeIDsOf(nodes))
	}

	// Tracker present but unavailable: retain the owner and the local reader.
	reg := tracker.NewRegistry(tracker.Options{Tick: time.Millisecond, LeaseTTL: time.Minute})
	e2, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: "A",
		TrackerRegistry: reg, TrackerClient: tracker.NewClient(),
	})
	if err != nil {
		t.Fatalf("New2: %v", err)
	}
	reg.Join("solo", "A", 0)
	nodes2, fromReaders2 := e2.treeNodes(context.Background(), prov.Current(), "solo", e2.fileKey("solo"), 3)
	if !fromReaders2 {
		t.Error("tracker mode must retain the owner even with a single reader")
	}
	want := []string{hashring.Rank(3, prov.Current().Roots())[0].ID}
	if want[0] != "A" {
		want = append(want, "A")
	}
	if !reflect.DeepEqual(nodeIDsOf(nodes2), want) {
		t.Errorf("candidates = %v, want direct owner when reader snapshot is unavailable", nodeIDsOf(nodes2))
	}
}

// TestReaderSetTreeEndToEnd runs three P2P nodes that all share one tracker and
// verifies a read still succeeds and populates caches when the tree is derived
// from the reader set.
func TestReaderSetTreeEndToEnd(t *testing.T) {
	content := blob(10)
	origin := countingOrigin(t, content, nil)

	// One shared tracker, hosted on its own HTTP server so every engine reaches
	// it through the client path.
	reg := tracker.NewRegistry(tracker.Options{Tick: time.Millisecond, LeaseTTL: time.Minute})
	tsrv := httptest.NewServer((&tracker.Server{R: reg}).Handler())
	defer tsrv.Close()
	trackerAddr := strings.TrimPrefix(tsrv.URL, "http://")

	ids := []string{"A", "B", "C"}
	engines := map[string]*Engine{}
	provs := map[string]*cluster.StaticProvider{}
	for _, id := range ids {
		prov := cluster.NewStaticProvider()
		e, err := New(Options{
			Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
			Cluster: prov, Peer: peer.NewClient(), SelfID: id, Fanout: 1,
			// Point every node at the shared tracker: since no member's address
			// equals the tracker's, they use the client path against it via the
			// member list below.
			TrackerClient: tracker.NewClient(),
		})
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}
		engines[id], provs[id] = e, prov
	}

	// Membership: the tracker address is attached to a synthetic member "T" so
	// trackerAddr() can resolve it; A/B/C carry their peer addresses.
	members := []cluster.Member{{ID: "T", Addr: trackerAddr, Weight: 1, State: cluster.Ready}}
	for _, id := range ids {
		srv := httptest.NewServer(&peer.Server{NodeID: id, Src: engines[id].PeerSource()})
		t.Cleanup(srv.Close)
		members = append(members, cluster.Member{
			ID: id, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, State: cluster.Ready,
		})
	}
	for _, id := range ids {
		provs[id].Set(members)
	}

	// Every node reads the object; whichever routing the reader set produces,
	// the bytes must be correct.
	for _, id := range ids {
		var buf bytes.Buffer
		if err := engines[id].Serve(context.Background(), &buf, origin.URL, 0, int64(len(content)-1)); err != nil {
			t.Fatalf("%s Serve: %v", id, err)
		}
		if !bytes.Equal(buf.Bytes(), content) {
			t.Errorf("%s bytes mismatch", id)
		}
	}
}

// Every edge from every snapshot is descending in one common strict order.
// Their union is therefore acyclic even when no two nodes share a snapshot.
func TestReaderSnapshotChurnPreservesStrictOrder(t *testing.T) {
	members := []cluster.Member{{ID: "R", Addr: "root:1", Weight: 1, State: cluster.Ready, Role: cluster.RootOnly}}
	for i, id := range []string{"A", "B", "C", "D", "E", "F"} {
		members = append(members, cluster.Member{ID: id, Addr: id + ":1", Weight: float64(i + 1), State: cluster.Ready, Role: cluster.NonRoot})
	}
	for _, key := range []uint64{0, 42, 0xdeadbeef} {
		positions := map[string]int{"R": -1}
		full := cluster.NewStaticProvider(members...).Current()
		for i, n := range hashring.Rank(key, full.Readers()) {
			positions[n.ID] = i
		}
		// Also vary membership eligibility while preserving common weights.
		for _, absent := range []string{"", "B", "E"} {
			viewMembers := append([]cluster.Member(nil), members...)
			for i := range viewMembers {
				if viewMembers[i].ID == absent {
					viewMembers[i].State = cluster.Suspect
				}
			}
			view := cluster.NewStaticProvider(viewMembers...).Current()
			for _, self := range members[1:] {
				if self.ID == absent {
					continue
				}
				e := &Engine{selfID: self.ID, role: cluster.NonRoot, trackerReg: &tracker.Registry{}, rs: make(map[string]*readerSet)}
				for mask := 0; mask < 64; mask++ {
					ids := []string{"R", "unknown"}
					for i, m := range members[1:] {
						if mask&(1<<i) != 0 {
							ids = append(ids, m.ID, m.ID)
						}
					}
					before := append([]string(nil), ids...)
					e.rs["object"] = &readerSet{nodes: ids, expiresAt: time.Now().Add(time.Hour)}
					order, _ := e.treeNodes(context.Background(), view, "object", 1, key)
					seen, selfIndex := make(map[string]bool), -1
					for i, n := range order {
						if seen[n.ID] || n.ID == absent {
							t.Fatal("duplicate or ineligible reader", order)
						}
						seen[n.ID] = true
						if n.ID == self.ID {
							selfIndex = i
						}
						if i > 0 && positions[order[i-1].ID] >= positions[n.ID] {
							t.Fatal("relative order changed", order)
						}
					}
					if selfIndex < 1 || order[0].ID != "R" || !reflect.DeepEqual(ids, before) {
						t.Fatal("self missing or tracker snapshot mutated")
					}
					for _, fanout := range []int{1, 2, 4, 8} {
						e.fanout = fanout
						for _, rootFanout := range []int{1, 2, 4, 8} {
							e.rootFanout = rootFanout
							for i := 1; i < len(order); i++ {
								parent := hashring.ParentWithRootFanout(i, len(order), fanout, rootFanout)
								if parent < 0 || positions[order[parent].ID] >= positions[order[i].ID] {
									t.Fatal("non-descending parent edge")
								}
							}
							primary, backup := e.hedgeTargets(order, view)
							if primary == "" {
								t.Fatal("eligible reader has no parent")
							}
							for _, addr := range []string{primary, backup} {
								if addr == "" {
									continue
								}
								for _, m := range members {
									if m.Addr == addr && positions[m.ID] >= positions[self.ID] {
										t.Fatal("fallback points to later reader")
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
