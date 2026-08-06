package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

func openTieredAt(t *testing.T, slots int) *store.Tiered {
	t.Helper()
	ts, err := store.OpenTiered(store.TieredOptions{
		Path: filepath.Join(t.TempDir(), "blocks"), SlotSize: testCfg().BlockSize, Slots: slots,
	})
	if err != nil {
		t.Fatalf("OpenTiered: %v", err)
	}
	t.Cleanup(func() { ts.Close() })
	return ts
}

// TestClassOfMatchesPlacement: a node classifies a chunk as owned exactly when
// it is in the HRW top-Replicas over Ready members, else borrowed.
func TestClassOfMatchesPlacement(t *testing.T) {
	members := []cluster.Member{
		{ID: "A", Addr: "a:1", Weight: 1, State: cluster.Ready},
		{ID: "B", Addr: "b:1", Weight: 1, State: cluster.Ready},
		{ID: "C", Addr: "c:1", Weight: 1, State: cluster.Ready},
	}
	prov := cluster.NewStaticProvider(members...)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}

	const chunkKey = uint64(0xFEEDFACE)
	ranked := hashring.Rank(chunkKey, nodes)
	owner, notOwner := ranked[0].ID, ranked[2].ID

	// The owner sees it as owned.
	eOwner, err := New(Options{
		Chunk: testCfg(), Store: openTieredAt(t, 10), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: owner,
	})
	if err != nil {
		t.Fatalf("New owner: %v", err)
	}
	if got := eOwner.classOf(chunkKey); got != store.Owned {
		t.Errorf("owner %s classOf = %v, want owned", owner, got)
	}

	// A non-top-R node sees it as borrowed.
	eOther, err := New(Options{
		Chunk: testCfg(), Store: openTieredAt(t, 10), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: notOwner,
	})
	if err != nil {
		t.Fatalf("New other: %v", err)
	}
	if got := eOther.classOf(chunkKey); got != store.Borrowed {
		t.Errorf("non-owner %s classOf = %v, want borrowed", notOwner, got)
	}

	// With Replicas=3 every member is an authoritative holder.
	eRep, err := New(Options{
		Chunk: testCfg(), Store: openTieredAt(t, 10), Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: notOwner, Replicas: 3,
	})
	if err != nil {
		t.Fatalf("New replicas: %v", err)
	}
	if got := eRep.classOf(chunkKey); got != store.Owned {
		t.Errorf("with Replicas=3, %s classOf = %v, want owned", notOwner, got)
	}
}

// TestClassOfSingleNodeIsOwned: without P2P there is no placement, so everything
// cached belongs to this node.
func TestClassOfSingleNodeIsOwned(t *testing.T) {
	e, err := New(Options{Chunk: testCfg(), Store: openTieredAt(t, 10), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := e.classOf(1234); got != store.Owned {
		t.Errorf("single-node classOf = %v, want owned", got)
	}
}

// TestServeUsesOwnedBudget: a single-node engine on a tiered store writes into
// the owned budget, leaving borrowed empty.
func TestServeUsesOwnedBudget(t *testing.T) {
	content := blob(100)
	origin := countingOrigin(t, content, nil)
	ts := openTieredAt(t, 20)
	e, err := New(Options{Chunk: testCfg(), Store: ts, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := e.Serve(context.Background(), &buf, origin.URL, 0, 99); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("bytes mismatch")
	}
	st := ts.Stats()
	if st.OwnedBlocks == 0 {
		t.Errorf("expected blocks in the owned budget: %+v", st)
	}
	if st.BorrowedBlocks != 0 {
		t.Errorf("single-node serve should not use borrowed: %+v", st)
	}
}

// TestServeBorrowedWhenNotOwner: a P2P node that is not the placement owner
// caches into the borrowed budget, so it cannot displace owned shards.
func TestServeBorrowedWhenNotOwner(t *testing.T) {
	content := blob(10) // single block
	origin := countingOrigin(t, content, nil)

	oid, _ := chunk.ObjectID(origin.URL)
	chunkKey := chunk.ChunkKey("dart", oid, 0)
	nodes := []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}, {ID: "C", Weight: 1}}
	notOwner := hashring.Rank(chunkKey, nodes)[2].ID

	// Only this node has an address, so peer pulls fail and it falls back to
	// origin — but the classification must still say borrowed.
	members := []cluster.Member{
		{ID: "A", Weight: 1, State: cluster.Ready},
		{ID: "B", Weight: 1, State: cluster.Ready},
		{ID: "C", Weight: 1, State: cluster.Ready},
	}
	prov := cluster.NewStaticProvider(members...)
	ts := openTieredAt(t, 20)
	e, err := New(Options{
		Chunk: testCfg(), Store: ts, Fetcher: newFetcher(),
		Cluster: prov, Peer: peer.NewClient(), SelfID: notOwner,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var buf bytes.Buffer
	if err := e.Serve(context.Background(), &buf, origin.URL, 0, int64(len(content)-1)); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("bytes mismatch")
	}
	st := ts.Stats()
	if st.BorrowedBlocks == 0 {
		t.Errorf("non-owner should cache into borrowed: %+v", st)
	}
	if st.OwnedBlocks != 0 {
		t.Errorf("non-owner must not use the owned budget: %+v", st)
	}
}
