package engine

import (
	"bytes"
	"context"
	"net/http/httptest"
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
	if len(got) != 2 {
		t.Fatalf("tree candidates = %v, want exactly the 2 readers (%s, %s)", got, self, other)
	}
	for _, id := range got {
		if id != self && id != other {
			t.Errorf("non-reader %q in tree candidates %v", id, got)
		}
	}
}

// TestTreeNodesFallsBackToAllMembers: without a tracker, or with too few
// readers to form a tree, routing falls back to all Ready members.
func TestTreeNodesFallsBackToAllMembers(t *testing.T) {
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

	// Tracker present but only one reader: too small for a tree, so fall back.
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
	if fromReaders2 {
		t.Error("a single reader should not form a reader-set tree")
	}
	if len(nodes2) != 3 {
		t.Errorf("candidates = %v, want all 3 Ready members", nodeIDsOf(nodes2))
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
		srv := httptest.NewServer(&peer.StreamServer{NodeID: id, Src: engines[id].PeerStreamSource()})
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
