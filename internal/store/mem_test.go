package store

import (
	"bytes"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

func openMem(t *testing.T, slotSize int64, slots int) *MemStore {
	t.Helper()
	m, err := OpenMem(MemOptions{SlotSize: slotSize, Slots: slots})
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// --- MemStore ---

func TestMemBadOptions(t *testing.T) {
	if _, err := OpenMem(MemOptions{SlotSize: 0, Slots: 4}); err != ErrBadMemOptions {
		t.Errorf("SlotSize 0 = %v, want ErrBadMemOptions", err)
	}
	if _, err := OpenMem(MemOptions{SlotSize: 16, Slots: 0}); err != ErrBadMemOptions {
		t.Errorf("Slots 0 = %v, want ErrBadMemOptions", err)
	}
}

func TestMemRoundtrip(t *testing.T) {
	m := openMem(t, 4096, 4)
	want := bytes.Repeat([]byte{0x6B}, 300)
	if err := m.Put(bk(1), want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := m.Get(bk(1))
	if err != nil || !ok || !bytes.Equal(got, want) {
		t.Errorf("Get = ok=%v err=%v equal=%v", ok, err, bytes.Equal(got, want))
	}
	if !m.Has(bk(1)) || m.Len() != 1 || m.Slots() != 4 {
		t.Errorf("Has/Len/Slots = %v/%d/%d", m.Has(bk(1)), m.Len(), m.Slots())
	}
	if _, ok, _ := m.Get(bk(99)); ok {
		t.Error("Get(miss) should be false")
	}
}

func TestMemTailBlockAndErrors(t *testing.T) {
	m := openMem(t, 16, 2)
	short := []byte{1, 2, 3}
	if err := m.Put(bk(1), short); err != nil {
		t.Fatalf("Put short: %v", err)
	}
	got, _, _ := m.Get(bk(1))
	if len(got) != 3 || !bytes.Equal(got, short) {
		t.Errorf("short block = %v", got)
	}
	if err := m.Put(bk(2), nil); err != ErrEmptyBlock {
		t.Errorf("Put(empty) = %v, want ErrEmptyBlock", err)
	}
	if err := m.Put(bk(2), bytes.Repeat([]byte{9}, 17)); err != ErrBlockTooLarge {
		t.Errorf("Put(oversize) = %v, want ErrBlockTooLarge", err)
	}
}

func TestMemEvictionLRU(t *testing.T) {
	m := openMem(t, 16, 2)
	_ = m.Put(bk(1), []byte{1})
	_ = m.Put(bk(2), []byte{2})
	if _, ok, _ := m.Get(bk(1)); !ok { // make 2 the LRU
		t.Fatal("Get(1) miss")
	}
	_ = m.Put(bk(3), []byte{3})
	if !m.Has(bk(1)) || m.Has(bk(2)) || !m.Has(bk(3)) {
		t.Errorf("LRU eviction wrong: 1=%v 2=%v 3=%v", m.Has(bk(1)), m.Has(bk(2)), m.Has(bk(3)))
	}
	if m.Len() != 2 {
		t.Errorf("Len = %d, want 2 (capacity)", m.Len())
	}
}

// TestMemGetReturnsCopy: a caller must not be able to reach into a slab, since
// slabs are recycled after eviction.
func TestMemGetReturnsCopy(t *testing.T) {
	m := openMem(t, 16, 2)
	_ = m.Put(bk(1), []byte{1, 2, 3, 4})
	got, _, _ := m.Get(bk(1))
	got[0] = 0xFF
	again, _, _ := m.Get(bk(1))
	if again[0] != 1 {
		t.Errorf("Get returned aliased slab memory: %v", again)
	}
}

// TestMemSlabRecycledSafely: after a block is evicted and its slab reused, a
// copy taken earlier must still be intact.
func TestMemSlabRecycledSafely(t *testing.T) {
	m := openMem(t, 16, 1)
	_ = m.Put(bk(1), bytes.Repeat([]byte{0xAA}, 8))
	snapshot, _, _ := m.Get(bk(1))

	// Evict block 1 and reuse its slab for very different content.
	_ = m.Put(bk(2), bytes.Repeat([]byte{0xBB}, 8))
	if m.Has(bk(1)) {
		t.Fatal("block 1 should have been evicted")
	}
	if !bytes.Equal(snapshot, bytes.Repeat([]byte{0xAA}, 8)) {
		t.Errorf("earlier copy was corrupted by slab reuse: %v", snapshot)
	}
}

func TestMemGetReaderAndDelete(t *testing.T) {
	m := openMem(t, 4096, 4)
	want := bytes.Repeat([]byte{0x33}, 100)
	_ = m.Put(bk(1), want)

	r, n, ok, err := m.GetReader(bk(1))
	if err != nil || !ok || n != 100 {
		t.Fatalf("GetReader = n=%d ok=%v err=%v", n, ok, err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, want) {
		t.Errorf("streamed bytes mismatch")
	}
	if _, _, ok, _ := m.GetReader(bk(99)); ok {
		t.Error("GetReader(miss) should be false")
	}

	if !m.Delete(bk(1)) || m.Delete(bk(1)) {
		t.Error("Delete semantics wrong")
	}
	if m.Len() != 0 {
		t.Errorf("Len = %d after delete", m.Len())
	}
}

func TestMemPutExistingKeyKeepsOriginal(t *testing.T) {
	m := openMem(t, 16, 2)
	_ = m.Put(bk(1), []byte{1, 1})
	_ = m.Put(bk(1), []byte{2, 2}) // same key: immutable, refresh only
	got, _, _ := m.Get(bk(1))
	if !bytes.Equal(got, []byte{1, 1}) {
		t.Errorf("existing key overwritten: %v", got)
	}
	if m.Len() != 1 {
		t.Errorf("Len = %d, want 1", m.Len())
	}
}

func TestMemCloseIdempotent(t *testing.T) {
	m, err := OpenMem(MemOptions{SlotSize: 16, Slots: 2})
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	_ = m.Put(bk(1), []byte{1})
	if err := m.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if m.Len() != 0 {
		t.Errorf("Len after Close = %d", m.Len())
	}
}

func TestMemConcurrent(t *testing.T) {
	m := openMem(t, 64, 32)
	data := bytes.Repeat([]byte{5}, 32)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := bk(i % 200) // keyspace > capacity: eviction churn
				if err := m.Put(k, data); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, _, err := m.Get(k); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				m.Has(k)
				if i%97 == 0 {
					m.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
	if l := m.Len(); l < 0 || l > 32 {
		t.Errorf("Len = %d, out of range", l)
	}
}

// --- Hybrid ---

func openHybrid(t *testing.T, memSlots, diskSlots int) (*Hybrid, *MemStore, *Tiered) {
	t.Helper()
	mem := openMem(t, 64, memSlots)
	disk, err := OpenTiered(TieredOptions{
		Path: filepath.Join(t.TempDir(), "blocks"), SlotSize: 64, Slots: diskSlots,
	})
	if err != nil {
		t.Fatalf("OpenTiered: %v", err)
	}
	h := NewHybrid(mem, disk)
	t.Cleanup(func() { h.Close() })
	return h, mem, disk
}

// TestHybridWritesThroughToDisk: a write reaches both tiers, so memory eviction
// cannot make the block unavailable.
func TestHybridWritesThroughToDisk(t *testing.T) {
	h, mem, disk := openHybrid(t, 4, 20)
	data := bytes.Repeat([]byte{7}, 32)
	if ok, err := h.PutClass(bk(1), data, Owned); !ok || err != nil {
		t.Fatalf("PutClass = %v, %v", ok, err)
	}
	if !mem.Has(bk(1)) {
		t.Error("block not mirrored in memory")
	}
	if !disk.Has(bk(1)) {
		t.Error("block not written through to disk")
	}
	if st := h.Stats(); st.OwnedBlocks != 1 || st.MemBlocks != 1 || st.MemSlots != 4 {
		t.Errorf("stats = %+v", st)
	}
}

// TestHybridSurvivesMemEviction: churn memory past its capacity; every block is
// still readable because disk holds it.
func TestHybridSurvivesMemEviction(t *testing.T) {
	h, mem, _ := openHybrid(t, 2, 40) // tiny memory, roomy disk
	data := bytes.Repeat([]byte{3}, 32)

	var keys []BlockKey
	for i := 0; i < 10; i++ {
		k := bk(i)
		keys = append(keys, k)
		if ok, err := h.PutClass(k, data, Owned); !ok || err != nil {
			t.Fatalf("put %d: %v %v", i, ok, err)
		}
	}
	if mem.Len() > 2 {
		t.Errorf("memory exceeded its capacity: %d", mem.Len())
	}
	for i, k := range keys {
		got, ok, err := h.Get(k)
		if err != nil || !ok {
			t.Errorf("block %d unreadable after memory eviction: ok=%v err=%v", i, ok, err)
			continue
		}
		if !bytes.Equal(got, data) {
			t.Errorf("block %d bytes mismatch", i)
		}
	}
}

// TestHybridPromotesOnRead: a disk hit is pulled into memory so the next read is
// served from RAM.
func TestHybridPromotesOnRead(t *testing.T) {
	h, mem, disk := openHybrid(t, 2, 20)
	data := bytes.Repeat([]byte{4}, 32)

	// Put directly into disk so memory does not have it.
	if ok, err := disk.PutClass(bk(1), data, Owned); !ok || err != nil {
		t.Fatalf("disk put: %v %v", ok, err)
	}
	if mem.Has(bk(1)) {
		t.Fatal("precondition: memory should not hold the block yet")
	}

	if _, ok, err := h.Get(bk(1)); !ok || err != nil {
		t.Fatalf("Get = ok=%v err=%v", ok, err)
	}
	if !mem.Has(bk(1)) {
		t.Error("a disk hit should be promoted into memory")
	}
}

// TestHybridGetReaderDoesNotPromote: the streaming path is often a one-shot
// relay, so it must not buffer the block just to populate memory.
func TestHybridGetReaderDoesNotPromote(t *testing.T) {
	h, mem, disk := openHybrid(t, 2, 20)
	data := bytes.Repeat([]byte{8}, 32)
	if ok, _ := disk.PutClass(bk(1), data, Owned); !ok {
		t.Fatal("disk put failed")
	}

	r, n, ok, err := h.GetReader(bk(1))
	if err != nil || !ok || n != 32 {
		t.Fatalf("GetReader = n=%d ok=%v err=%v", n, ok, err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, data) {
		t.Errorf("streamed bytes mismatch")
	}
	if mem.Has(bk(1)) {
		t.Error("GetReader should not promote into memory")
	}
}

func TestHybridHasDeleteLenClose(t *testing.T) {
	h, mem, disk := openHybrid(t, 4, 20)
	data := bytes.Repeat([]byte{2}, 16)
	_, _ = h.PutClass(bk(1), data, Owned)
	_, _ = h.PutClass(bk(2), data, Borrowed)

	if !h.Has(bk(1)) || !h.Has(bk(2)) || h.Has(bk(99)) {
		t.Error("Has wrong")
	}
	if h.Len() != 2 {
		t.Errorf("Len = %d, want 2", h.Len())
	}
	if !h.Delete(bk(1)) {
		t.Error("Delete(1) should be true")
	}
	if mem.Has(bk(1)) || disk.Has(bk(1)) {
		t.Error("Delete should clear both tiers")
	}
	if h.Delete(bk(99)) {
		t.Error("Delete(missing) should be false")
	}
}

func TestHybridPutDefaultsToBorrowed(t *testing.T) {
	h, _, _ := openHybrid(t, 4, 20)
	if err := h.Put(bk(1), bytes.Repeat([]byte{1}, 16)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st := h.Stats()
	if st.BorrowedBlocks != 1 || st.OwnedBlocks != 0 {
		t.Errorf("plain Put should be borrowed: %+v", st)
	}
}

func TestHybridConcurrent(t *testing.T) {
	h, _, _ := openHybrid(t, 16, 64)
	data := bytes.Repeat([]byte{6}, 32)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := bk(i % 100)
				class := Borrowed
				if i%3 == 0 {
					class = Owned
				}
				if _, err := h.PutClass(k, data, class); err != nil {
					t.Errorf("PutClass: %v", err)
					return
				}
				h.Get(k)
				h.Has(k)
				h.Stats()
				if i%89 == 0 {
					h.Delete(k)
				}
			}
		}(g)
	}
	wg.Wait()
}
