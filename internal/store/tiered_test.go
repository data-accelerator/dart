package store

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
)

// --- TinyLFU sketch ---

func TestSketchDoorkeeperAbsorbsFirstSighting(t *testing.T) {
	s := newSketch(64)
	const h = 0xABCDEF
	// A first sighting only primes the doorkeeper, so the estimate stays low.
	s.Increment(h)
	if got := s.Estimate(h); got != 1 {
		t.Errorf("estimate after 1 access = %d, want 1 (doorkeeper only)", got)
	}
	// Repeats build real frequency.
	for i := 0; i < 5; i++ {
		s.Increment(h)
	}
	if got := s.Estimate(h); got <= 1 {
		t.Errorf("estimate after 6 accesses = %d, want > 1", got)
	}
}

func TestSketchUnseenKeyIsZero(t *testing.T) {
	s := newSketch(64)
	if got := s.Estimate(0xDEADBEEF); got != 0 {
		t.Errorf("estimate of unseen key = %d, want 0", got)
	}
}

// TestSketchAdmitFavorsPopular is the property the borrowed budget depends on: a
// frequently-seen candidate beats a one-shot victim, and a one-shot candidate
// does not displace a popular victim.
func TestSketchAdmitFavorsPopular(t *testing.T) {
	s := newSketch(128)
	const hot, cold, fresh = uint64(1), uint64(2), uint64(3)
	for i := 0; i < 20; i++ {
		s.Increment(hot)
	}
	s.Increment(cold) // seen once

	if !s.Admit(hot, cold) {
		t.Error("a hot candidate should be admitted over a cold victim")
	}
	if s.Admit(fresh, hot) {
		t.Error("an unseen candidate should not displace a hot victim")
	}
}

// TestSketchDecay: after enough activity the sketch halves counters, so stale
// popularity fades instead of pinning a block forever.
func TestSketchDecay(t *testing.T) {
	s := newSketch(8) // floored to minSketchKeys; resetAt = minSketchKeys*10
	const old = uint64(42)
	for i := 0; i < 12; i++ {
		s.Increment(old)
	}
	before := s.Estimate(old)

	// Drive unrelated traffic past the reset threshold. Each key needs two
	// increments to clear its doorkeeper and reach the counters.
	for k := uint64(100000); k < 100000+2*minSketchKeys*10; k++ {
		s.Increment(k)
		s.Increment(k)
	}
	if after := s.Estimate(old); after >= before {
		t.Errorf("estimate did not decay: before=%d after=%d", before, after)
	}
}

func TestSketchConcurrent(t *testing.T) {
	s := newSketch(256)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				h := uint64(i % 500)
				s.Increment(h)
				s.Estimate(h)
				s.Admit(h, h+1)
			}
		}(g)
	}
	wg.Wait()
}

func TestNextPow2(t *testing.T) {
	cases := map[uint64]uint64{0: 2, 1: 2, 2: 2, 3: 4, 5: 8, 16: 16, 17: 32, 1000: 1024}
	for in, want := range cases {
		if got := nextPow2(in); got != want {
			t.Errorf("nextPow2(%d) = %d, want %d", in, got, want)
		}
	}
}

// --- Tiered store ---

func openTiered(t *testing.T, slots int, frac float64) *Tiered {
	t.Helper()
	ts, err := OpenTiered(TieredOptions{
		Path: filepath.Join(t.TempDir(), "blocks"), SlotSize: 64, Slots: slots, OwnedFraction: frac,
	})
	if err != nil {
		t.Fatalf("OpenTiered: %v", err)
	}
	t.Cleanup(func() { ts.Close() })
	return ts
}

func TestTieredBadOptions(t *testing.T) {
	base := TieredOptions{Path: filepath.Join(t.TempDir(), "b"), SlotSize: 64, Slots: 4}
	bad := base
	bad.SlotSize = 0
	if _, err := OpenTiered(bad); err != ErrBadTieredOptions {
		t.Errorf("SlotSize 0 = %v, want ErrBadTieredOptions", err)
	}
	bad = base
	bad.Slots = 1
	if _, err := OpenTiered(bad); err != ErrBadTieredOptions {
		t.Errorf("Slots 1 = %v, want ErrBadTieredOptions", err)
	}
}

func TestTieredSplitsCapacity(t *testing.T) {
	ts := openTiered(t, 10, 0.8)
	st := ts.Stats()
	if st.OwnedSlots != 8 || st.BorrowedSlots != 2 {
		t.Errorf("split = owned %d / borrowed %d, want 8/2", st.OwnedSlots, st.BorrowedSlots)
	}
	// An out-of-range fraction is rejected, never silently substituted (issue
	// #17 N4): 1.5/-0.5 must error; 0 is the zero value (unset) → 0.8 default;
	// node flag parsing rejects an explicitly-set 0 (see issue17_test.go).
	for _, f := range []float64{1, 1.5, -0.5} {
		if _, err := OpenTiered(TieredOptions{
			Path: filepath.Join(t.TempDir(), "b"), SlotSize: 64 << 10, Slots: 10,
			OwnedFraction: f,
		}); err != ErrBadTieredOptions {
			t.Errorf("OwnedFraction %v = %v, want ErrBadTieredOptions", f, err)
		}
	}
	ts2 := openTiered(t, 10, 0) // zero value = unset
	if st2 := ts2.Stats(); st2.OwnedSlots != 8 {
		t.Errorf("unset fraction: owned slots = %d, want 8 (default 0.8)", st2.OwnedSlots)
	}
	// Both budgets always get at least one slot.
	ts3 := openTiered(t, 2, 0.99)
	st3 := ts3.Stats()
	if st3.OwnedSlots < 1 || st3.BorrowedSlots < 1 {
		t.Errorf("degenerate split = owned %d / borrowed %d", st3.OwnedSlots, st3.BorrowedSlots)
	}
}

func TestTieredRoundtripBothClasses(t *testing.T) {
	ts := openTiered(t, 10, 0.8)
	ownedKey := BlockKey{Chunk: 1, Block: 0}
	borrowedKey := BlockKey{Chunk: 2, Block: 0}
	ownedData := bytes.Repeat([]byte{1}, 16)
	borrowedData := bytes.Repeat([]byte{2}, 16)

	if ok, err := ts.PutClass(ownedKey, ownedData, Owned); !ok || err != nil {
		t.Fatalf("PutClass owned = %v, %v", ok, err)
	}
	if ok, err := ts.PutClass(borrowedKey, borrowedData, Borrowed); !ok || err != nil {
		t.Fatalf("PutClass borrowed = %v, %v", ok, err)
	}
	got, ok, err := ts.Get(ownedKey)
	if err != nil || !ok || !bytes.Equal(got, ownedData) {
		t.Errorf("Get owned = ok=%v err=%v", ok, err)
	}
	got2, ok2, _ := ts.Get(borrowedKey)
	if !ok2 || !bytes.Equal(got2, borrowedData) {
		t.Errorf("Get borrowed mismatch")
	}
	if st := ts.Stats(); st.OwnedBlocks != 1 || st.BorrowedBlocks != 1 {
		t.Errorf("stats = %+v", st)
	}
	if ts.Len() != 2 {
		t.Errorf("Len = %d, want 2", ts.Len())
	}
	if !ts.Has(ownedKey) || !ts.Has(borrowedKey) {
		t.Error("Has should see both classes")
	}
}

// TestTieredBudgetIsolation is the core guarantee: hammering the borrowed budget
// must never evict owned blocks.
func TestTieredBudgetIsolation(t *testing.T) {
	ts := openTiered(t, 10, 0.8) // owned 8, borrowed 2
	data := bytes.Repeat([]byte{7}, 16)

	// Fill the owned budget.
	var owned []BlockKey
	for i := 0; i < 8; i++ {
		k := BlockKey{Chunk: uint64(100 + i), Block: 0}
		owned = append(owned, k)
		if ok, err := ts.PutClass(k, data, Owned); !ok || err != nil {
			t.Fatalf("owned put %d: %v %v", i, ok, err)
		}
	}
	// Churn far more borrowed blocks than the borrowed budget can hold. Touch
	// each twice so admission cannot reject everything.
	for i := 0; i < 200; i++ {
		k := BlockKey{Chunk: uint64(9000 + i), Block: 0}
		ts.Get(k) // prime the sketch
		_, _ = ts.PutClass(k, data, Borrowed)
	}

	// Every owned block must survive.
	for i, k := range owned {
		if !ts.Has(k) {
			t.Errorf("owned block %d was evicted by borrowed churn", i)
		}
	}
	st := ts.Stats()
	if st.OwnedBlocks != 8 {
		t.Errorf("owned blocks = %d, want 8", st.OwnedBlocks)
	}
	if st.BorrowedBlocks > st.BorrowedSlots {
		t.Errorf("borrowed %d exceeds its budget %d", st.BorrowedBlocks, st.BorrowedSlots)
	}
}

// TestTieredAdmissionRejectsOneShots: once the borrowed budget is full, a stream
// of never-seen blocks is refused rather than thrashing the warm entries.
func TestTieredAdmissionRejectsOneShots(t *testing.T) {
	ts := openTiered(t, 10, 0.8) // borrowed budget = 2
	data := bytes.Repeat([]byte{3}, 16)

	// Make two blocks genuinely popular and get them cached.
	warm := []BlockKey{{Chunk: 1, Block: 0}, {Chunk: 2, Block: 0}}
	for _, k := range warm {
		for i := 0; i < 10; i++ {
			ts.Get(k)
		}
		if ok, _ := ts.PutClass(k, data, Borrowed); !ok {
			t.Fatalf("warm block %+v was not admitted", k)
		}
	}

	// Now offer one-shot blocks the sketch has never seen.
	rejected := 0
	for i := 0; i < 50; i++ {
		k := BlockKey{Chunk: uint64(5000 + i), Block: 0}
		if ok, err := ts.PutClass(k, data, Borrowed); err != nil {
			t.Fatalf("put: %v", err)
		} else if !ok {
			rejected++
		}
	}
	if rejected == 0 {
		t.Error("expected admission to reject some one-shot candidates")
	}
	if st := ts.Stats(); st.AdmitRejected == 0 {
		t.Error("AdmitRejected stat not incremented")
	}
	// The warm blocks should still be there.
	for _, k := range warm {
		if !ts.Has(k) {
			t.Errorf("warm borrowed block %+v was evicted by one-shot churn", k)
		}
	}
}

func TestTieredPutDefaultsToBorrowed(t *testing.T) {
	ts := openTiered(t, 10, 0.8)
	k := BlockKey{Chunk: 1, Block: 1}
	if err := ts.Put(k, bytes.Repeat([]byte{9}, 8)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st := ts.Stats()
	if st.BorrowedBlocks != 1 || st.OwnedBlocks != 0 {
		t.Errorf("plain Put should land in borrowed: %+v", st)
	}
}

func TestTieredPutClassExistingKey(t *testing.T) {
	ts := openTiered(t, 10, 0.8)
	k := BlockKey{Chunk: 1, Block: 0}
	data := bytes.Repeat([]byte{4}, 16)
	// Owned first; a later Borrowed put must be a no-op success, not a duplicate.
	if ok, _ := ts.PutClass(k, data, Owned); !ok {
		t.Fatal("owned put failed")
	}
	if ok, err := ts.PutClass(k, data, Borrowed); !ok || err != nil {
		t.Errorf("borrowed put of an owned key = %v, %v; want admitted no-op", ok, err)
	}
	if st := ts.Stats(); st.BorrowedBlocks != 0 {
		t.Errorf("key duplicated into borrowed: %+v", st)
	}
}

func TestTieredGetReaderAndDelete(t *testing.T) {
	ts := openTiered(t, 10, 0.8)
	ownedKey := BlockKey{Chunk: 1, Block: 0}
	borrowedKey := BlockKey{Chunk: 2, Block: 0}
	_, _ = ts.PutClass(ownedKey, bytes.Repeat([]byte{1}, 16), Owned)
	_, _ = ts.PutClass(borrowedKey, bytes.Repeat([]byte{2}, 16), Borrowed)

	for _, k := range []BlockKey{ownedKey, borrowedKey} {
		r, n, ok, err := ts.GetReader(k)
		if err != nil || !ok || n != 16 || r == nil {
			t.Errorf("GetReader(%+v) = n=%d ok=%v err=%v", k, n, ok, err)
		}
	}
	if _, _, ok, _ := ts.GetReader(BlockKey{Chunk: 99}); ok {
		t.Error("GetReader(miss) should be false")
	}

	if !ts.Delete(ownedKey) || !ts.Delete(borrowedKey) {
		t.Error("Delete should report both existed")
	}
	if ts.Delete(BlockKey{Chunk: 99}) {
		t.Error("Delete(missing) should be false")
	}
	if ts.Len() != 0 {
		t.Errorf("Len = %d after deletes, want 0", ts.Len())
	}
}

func TestClassString(t *testing.T) {
	if Owned.String() != "owned" || Borrowed.String() != "borrowed" {
		t.Errorf("class names = %q / %q", Owned.String(), Borrowed.String())
	}
}

func TestTieredConcurrent(t *testing.T) {
	ts := openTiered(t, 64, 0.75)
	data := bytes.Repeat([]byte{5}, 32)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := BlockKey{Chunk: uint64(i % 100), Block: uint64(g % 2)}
				class := Borrowed
				if i%3 == 0 {
					class = Owned
				}
				if _, err := ts.PutClass(k, data, class); err != nil {
					t.Errorf("PutClass: %v", err)
					return
				}
				ts.Get(k)
				ts.Has(k)
				if i%97 == 0 {
					ts.Delete(k)
				}
				ts.Stats()
			}
		}(g)
	}
	wg.Wait()
}
