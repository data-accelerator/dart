package store

// Regression tests for issue #11 (store bundle). Each pins one item (S1–S5).

import (
	"testing"
)

// TestOwnedTrafficDoesNotDecayBorrowedEstimates pins S1: the estimator used to
// count ALL Get traffic while its halving cadence was sized for the borrowed
// budget — enough owned reads decayed a warm borrowed key's estimate to 0,
// where a one-shot was admitted 0 >= 0 and evicted it. Owned traffic must not
// feed the borrowed admission estimator at all.
func TestOwnedTrafficDoesNotDecayBorrowedEstimates(t *testing.T) {
	ts := openTiered(t, 8, 0.5) // 4 owned, 4 borrowed slots
	warm := BlockKey{Chunk: 900, Block: 0}
	if _, err := ts.PutClass(warm, make([]byte, 64), Borrowed); err != nil {
		t.Fatalf("PutClass: %v", err)
	}
	// Make the borrowed key genuinely warm.
	for i := 0; i < 20; i++ {
		if _, ok, _ := ts.Get(warm); !ok {
			t.Fatal("warm key missing")
		}
	}
	estBefore := ts.sk.Estimate(keyHash(warm))

	// Heavy owned-only traffic: many increments used to fire halving far
	// beyond the borrowed cadence.
	own := BlockKey{Chunk: 1, Block: 0}
	if _, err := ts.PutClass(own, make([]byte, 64), Owned); err != nil {
		t.Fatalf("PutClass owned: %v", err)
	}
	for i := 0; i < 20000; i++ {
		if _, ok, _ := ts.Get(own); !ok {
			t.Fatal("owned key missing")
		}
	}
	if estAfter := ts.sk.Estimate(keyHash(warm)); estAfter != estBefore {
		t.Fatalf("owned traffic moved a borrowed estimate %d -> %d; it must not feed the estimator at all", estBefore, estAfter)
	}
}

// TestHybridMemHitsFeedBackingEstimator pins the other half of S1: a Hybrid
// mem-tier hit used to bypass the backing Tiered's estimator entirely, so a
// hot key's borrowed estimate decayed while mem served all its reads.
func TestHybridMemHitsFeedBackingEstimator(t *testing.T) {
	ts := openTiered(t, 8, 0.5)
	mem, err := OpenMem(MemOptions{SlotSize: 64, Slots: 4})
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	h := NewHybrid(mem, ts)
	k := BlockKey{Chunk: 42, Block: 0}
	if _, err := h.PutClass(k, make([]byte, 64), Borrowed); err != nil {
		t.Fatalf("PutClass: %v", err)
	}
	if !mem.Has(k) {
		t.Fatal("PutClass must mirror into the mem tier")
	}
	before := ts.sk.Estimate(keyHash(k))
	for i := 0; i < 10; i++ {
		if _, ok, _ := h.Get(k); !ok {
			t.Fatal("key missing")
		}
	}
	after := ts.sk.Estimate(keyHash(k))
	if after <= before {
		t.Fatalf("mem hits did not feed the estimator: %d -> %d", before, after)
	}
}

// TestPutClassOwnedDropsBorrowedCopy pins S2: PutClass(k, Owned) used to leave
// the borrowed copy in place — Len() counted one block twice and the
// authoritative block stayed evictable.
func TestPutClassOwnedDropsBorrowedCopy(t *testing.T) {
	ts := openTiered(t, 8, 0.5)
	k := BlockKey{Chunk: 7, Block: 0}
	if _, err := ts.PutClass(k, make([]byte, 64), Borrowed); err != nil {
		t.Fatalf("borrowed: %v", err)
	}
	if _, err := ts.PutClass(k, make([]byte, 64), Owned); err != nil {
		t.Fatalf("owned: %v", err)
	}
	if got := ts.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (one block, one budget)", got)
	}
	if ts.borrowed.Has(k) {
		t.Fatal("borrowed copy must be dropped on promotion to owned")
	}
	if !ts.owned.Has(k) {
		t.Fatal("owned copy missing")
	}
}

// TestBorrowedPutClassOnOwnedKeyRefreshesRecency pins S3: the owned-hit branch
// of PutClass(Borrowed) returned without the recency refresh both the docs and
// the code comment promise, making the "refreshed" key the next eviction
// victim.
func TestBorrowedPutClassOnOwnedKeyRefreshesRecency(t *testing.T) {
	ts := openTiered(t, 4, 0.5) // 2 owned slots
	a := BlockKey{Chunk: 1, Block: 0}
	b := BlockKey{Chunk: 2, Block: 0}
	ts.PutClass(a, make([]byte, 64), Owned)
	ts.PutClass(b, make([]byte, 64), Owned)
	// Refresh a via the borrowed path (the engine re-puts on relay/serve).
	if _, err := ts.PutClass(a, make([]byte, 64), Borrowed); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	// A third owned block must evict b (least recently used), not a.
	c := BlockKey{Chunk: 3, Block: 0}
	ts.PutClass(c, make([]byte, 64), Owned)
	if !ts.owned.Has(a) {
		t.Fatal("the refreshed key was evicted; recency refresh did not happen")
	}
	if ts.owned.Has(b) && ts.owned.Has(c) {
		t.Fatal("expected exactly one eviction among {b,c} at capacity")
	}
}

// TestPutClassFeedsEstimator pins S5: a write-only insertion pattern used to
// leave every estimate at 0, where admission degenerates to always-admit.
// Insertion is itself an access.
func TestPutClassFeedsEstimator(t *testing.T) {
	ts := openTiered(t, 8, 0.5)
	k := BlockKey{Chunk: 5, Block: 0}
	if _, err := ts.PutClass(k, make([]byte, 64), Borrowed); err != nil {
		t.Fatalf("PutClass: %v", err)
	}
	if est := ts.sk.Estimate(keyHash(k)); est < 1 {
		t.Fatalf("estimate after a write-only insert = %d, want >= 1", est)
	}
}

// TestBorrowedAdmissionSerializesWithEviction pins S4's invariant: under
// concurrent PutClass at capacity, the store never exceeds its slot budget and
// stays consistent — the admit decision and the evicting Put run in one
// critical section. (The deterministic TOCTOU needed an injected preemption;
// this hammers the path under -race and checks the budget invariant.)
func TestBorrowedAdmissionSerializesWithEviction(t *testing.T) {
	ts := openTiered(t, 8, 0.5) // 4 borrowed slots
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func(g int) {
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				k := BlockKey{Chunk: uint64(g*10000 + (i % 64)), Block: 0}
				ts.PutClass(k, make([]byte, 64), Borrowed)
				ts.Get(k)
			}
		}(g)
	}
	for i := 0; i < 200; i++ {
		ts.PutClass(BlockKey{Chunk: uint64(500000 + i), Block: 0}, make([]byte, 64), Borrowed)
		if n := ts.borrowed.Len(); n > 4 {
			t.Fatalf("borrowed Len = %d, exceeds its 4-slot budget", n)
		}
	}
	close(done)
	st := ts.Stats()
	if st.BorrowedBlocks > 4 {
		t.Fatalf("stats report %d borrowed blocks > 4 slots", st.BorrowedBlocks)
	}
}

// TestTouchSkipsOwnedKeys pins the review follow-up to S1: PutClass(Owned)
// also mirrors the block into a Hybrid's mem tier, so a class-blind Touch
// would feed owned traffic right back into the borrowed estimator. Touch
// must skip owned keys.
func TestTouchSkipsOwnedKeys(t *testing.T) {
	ts := openTiered(t, 8, 0.5)
	own := BlockKey{Chunk: 1, Block: 0}
	if _, err := ts.PutClass(own, make([]byte, 64), Owned); err != nil {
		t.Fatalf("PutClass: %v", err)
	}
	before := ts.sk.Estimate(keyHash(own))
	for i := 0; i < 100; i++ {
		ts.Touch(own)
	}
	if after := ts.sk.Estimate(keyHash(own)); after != before {
		t.Fatalf("Touch on an owned key moved its estimate %d -> %d", before, after)
	}

	// The same through a Hybrid: an owned block mirrored in mem must not feed
	// the estimator on mem hits, while a borrowed key's must grow.
	mem, err := OpenMem(MemOptions{SlotSize: 64, Slots: 4})
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	h := NewHybrid(mem, ts)
	if _, ok, _ := h.Get(own); !ok {
		t.Fatal("owned key must be mirrored in mem")
	}
	for i := 0; i < 50; i++ {
		h.Get(own)
	}
	if after := ts.sk.Estimate(keyHash(own)); after != before {
		t.Fatalf("owned mem hit fed the estimator: %d -> %d", before, after)
	}
	bor := BlockKey{Chunk: 2, Block: 0}
	if _, err := h.PutClass(bor, make([]byte, 64), Borrowed); err != nil {
		t.Fatalf("PutClass: %v", err)
	}
	est0 := ts.sk.Estimate(keyHash(bor))
	for i := 0; i < 10; i++ {
		h.Get(bor)
	}
	if est1 := ts.sk.Estimate(keyHash(bor)); est1 <= est0 {
		t.Fatalf("borrowed mem hits did not feed the estimator: %d -> %d", est0, est1)
	}
}
