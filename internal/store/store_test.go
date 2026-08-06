package store

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTest(t *testing.T, slotSize int64, slots int) *DiskStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blocks.dat")
	s, err := Open(Options{Path: path, SlotSize: slotSize, Slots: slots})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func bk(i int) BlockKey         { return BlockKey{Chunk: uint64(i), Block: uint64(i)} }
func data(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

func TestPutGetRoundtrip(t *testing.T) {
	s := openTest(t, 4096, 8)
	want := data(0xAB, 100)
	if err := s.Put(bk(1), want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !s.Has(bk(1)) {
		t.Error("Has(1) = false")
	}
	got, ok, err := s.Get(bk(1))
	if err != nil || !ok {
		t.Fatalf("Get(1) = ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Get(1) bytes mismatch")
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want 1", s.Len())
	}
}

func TestGetMiss(t *testing.T) {
	s := openTest(t, 4096, 8)
	got, ok, err := s.Get(bk(42))
	if got != nil || ok || err != nil {
		t.Errorf("Get(miss) = (%v, %v, %v)", got, ok, err)
	}
}

func TestTailBlockShorterThanSlot(t *testing.T) {
	s := openTest(t, 16, 4)
	want := data(0x7, 5) // 5 < slotSize 16
	if err := s.Put(bk(1), want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, _, _ := s.Get(bk(1))
	if len(got) != 5 || !bytes.Equal(got, want) {
		t.Errorf("tail block: got %d bytes %v, want 5 bytes", len(got), got)
	}
}

func TestPutErrors(t *testing.T) {
	s := openTest(t, 16, 4)
	if err := s.Put(bk(1), nil); err != ErrEmptyBlock {
		t.Errorf("Put(empty) = %v, want ErrEmptyBlock", err)
	}
	if err := s.Put(bk(1), data(1, 17)); err != ErrBlockTooLarge {
		t.Errorf("Put(too large) = %v, want ErrBlockTooLarge", err)
	}
	if s.Len() != 0 {
		t.Errorf("Len = %d after failed puts, want 0", s.Len())
	}
}

// TestPutExistingKeyDoesNotOverwrite documents the immutable-key contract:
// Put on an existing key refreshes recency but keeps the original bytes.
func TestPutExistingKeyDoesNotOverwrite(t *testing.T) {
	s := openTest(t, 16, 4)
	_ = s.Put(bk(1), data(0xAA, 8))
	_ = s.Put(bk(1), data(0xBB, 8)) // same key, different bytes
	got, _, _ := s.Get(bk(1))
	if !bytes.Equal(got, data(0xAA, 8)) {
		t.Errorf("existing key was overwritten: %v", got)
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want 1", s.Len())
	}
}

func TestEvictionLRU(t *testing.T) {
	s := openTest(t, 16, 2) // capacity 2
	_ = s.Put(bk(1), data(1, 4))
	_ = s.Put(bk(2), data(2, 4)) // full: [2(front),1(back)]
	// Touch 1 so 2 becomes least-recently-used.
	if _, ok, _ := s.Get(bk(1)); !ok {
		t.Fatal("Get(1) miss")
	}
	// Insert 3 -> must evict 2.
	_ = s.Put(bk(3), data(3, 4))
	if !s.Has(bk(1)) {
		t.Error("1 should survive (recently used)")
	}
	if s.Has(bk(2)) {
		t.Error("2 should have been evicted (LRU)")
	}
	if !s.Has(bk(3)) {
		t.Error("3 should be present")
	}
	if s.Len() != 2 {
		t.Errorf("Len = %d, want 2 (capacity)", s.Len())
	}
}

func TestDeleteAndSlotReuse(t *testing.T) {
	s := openTest(t, 16, 2)
	_ = s.Put(bk(1), data(1, 4))
	_ = s.Put(bk(2), data(2, 4))
	if !s.Delete(bk(1)) {
		t.Error("Delete(1) = false, want true")
	}
	if s.Delete(bk(1)) {
		t.Error("Delete(1) twice = true, want false")
	}
	if s.Has(bk(1)) || s.Len() != 1 {
		t.Errorf("after delete: Has(1)=%v Len=%d", s.Has(bk(1)), s.Len())
	}
	// The freed slot must be reusable without eviction.
	if err := s.Put(bk(3), data(3, 4)); err != nil {
		t.Fatalf("Put after delete: %v", err)
	}
	if !s.Has(bk(2)) || !s.Has(bk(3)) {
		t.Error("both 2 and 3 should be present after reusing the freed slot")
	}
}

func TestCopyOnGet(t *testing.T) {
	s := openTest(t, 16, 4)
	_ = s.Put(bk(1), data(0x5, 8))
	buf, _, _ := s.Get(bk(1))
	buf[0] = 0xFF // mutate the returned copy
	again, _, _ := s.Get(bk(1))
	if again[0] != 0x5 {
		t.Errorf("Get returned aliased data: second read = %#x", again[0])
	}
}

// TestReopenStartsCold documents that the store does not persist across restarts
// (read-only semantics make a cold cache safe: blocks are re-fetched from the
// origin, which is the source of truth). It also checks that the arena is
// rebuilt empty rather than left holding unreachable stale bytes.
func TestReopenStartsCold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blocks.dat")
	secret := []byte("SENSITIVE-ORIGIN-BYTES")

	s1, err := Open(Options{Path: path, SlotSize: 64, Slots: 4})
	if err != nil {
		t.Fatalf("Open1: %v", err)
	}
	if err := s1.Put(bk(1), secret); err != nil {
		t.Fatalf("Put: %v", err)
	}
	s1.Close()

	// Sanity: the bytes really were written to this file.
	if raw, err := os.ReadFile(path); err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if !bytes.Contains(raw, secret) {
		t.Fatal("precondition failed: the block was never written to disk")
	}

	s2, err := Open(Options{Path: path, SlotSize: 64, Slots: 4})
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer s2.Close()
	if s2.Len() != 0 || s2.Has(bk(1)) {
		t.Errorf("reopened store not cold: Len=%d Has(1)=%v", s2.Len(), s2.Has(bk(1)))
	}

	// Reopening rebuilds the arena, so the previous run's bytes are gone rather
	// than lingering unreachable on disk.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.Contains(raw, secret) {
		t.Error("stale bytes from the previous run are still on disk after reopen")
	}
	if int64(len(raw)) != 64*4 {
		t.Errorf("arena size = %d, want %d", len(raw), 64*4)
	}
}

func TestBadOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.dat")
	if _, err := Open(Options{Path: path, SlotSize: 0, Slots: 4}); err != ErrBadOptions {
		t.Errorf("SlotSize 0 = %v, want ErrBadOptions", err)
	}
	if _, err := Open(Options{Path: path, SlotSize: 16, Slots: 0}); err != ErrBadOptions {
		t.Errorf("Slots 0 = %v, want ErrBadOptions", err)
	}
}

func TestOpenFileError(t *testing.T) {
	// A path inside a non-existent directory cannot be created.
	path := filepath.Join(t.TempDir(), "missing-dir", "blocks.dat")
	if _, err := Open(Options{Path: path, SlotSize: 16, Slots: 4}); err == nil {
		t.Error("Open with uncreatable path should fail")
	}
}

func TestCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocks.dat")
	s, err := Open(Options{Path: path, SlotSize: 16, Slots: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close should be a no-op nil, got %v", err)
	}
}

// TestGetReaderStreams verifies the streaming read path: correct bytes, correct
// length, miss semantics, and that it refreshes LRU like Get.
func TestGetReaderStreams(t *testing.T) {
	s := openTest(t, 4096, 2)
	want := data(0x3C, 300)
	if err := s.Put(bk(1), want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r, n, ok, err := s.GetReader(bk(1))
	if err != nil || !ok {
		t.Fatalf("GetReader ok=%v err=%v", ok, err)
	}
	if n != int64(len(want)) {
		t.Errorf("length = %d, want %d", n, len(want))
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("streamed bytes mismatch (%d vs %d)", len(got), len(want))
	}

	// Miss.
	if _, _, ok, err := s.GetReader(bk(99)); ok || err != nil {
		t.Errorf("GetReader(miss) = ok=%v err=%v", ok, err)
	}

	// GetReader refreshes recency: fill capacity, touch 1, insert 3 -> 2 evicted.
	_ = s.Put(bk(2), data(2, 8))
	if _, _, ok, _ := s.GetReader(bk(1)); !ok {
		t.Fatal("GetReader(1) miss")
	}
	_ = s.Put(bk(3), data(3, 8))
	if !s.Has(bk(1)) || s.Has(bk(2)) {
		t.Errorf("LRU not refreshed by GetReader: Has(1)=%v Has(2)=%v", s.Has(bk(1)), s.Has(bk(2)))
	}
}

func TestGetReaderAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.dat")
	s, err := Open(Options{Path: path, SlotSize: 16, Slots: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = s.Put(bk(1), data(1, 8))
	s.Close()
	if _, _, _, err := s.GetReader(bk(1)); err == nil {
		t.Error("GetReader after Close should error")
	}
}

// TestConcurrent exercises the store under concurrent Put/Get/Delete/Has with
// eviction churn (keyspace >> capacity); run with -race.
func TestConcurrent(t *testing.T) {
	const slots = 64
	s := openTest(t, 64, slots)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 5000; i++ {
				k := bk(rng.Intn(200)) // keyspace > capacity -> eviction
				switch rng.Intn(4) {
				case 0:
					if err := s.Put(k, data(byte(k.Chunk), 1+rng.Intn(64))); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
				case 1:
					if _, _, err := s.Get(k); err != nil {
						t.Errorf("Get: %v", err)
						return
					}
				case 2:
					s.Has(k)
				case 3:
					s.Delete(k)
				}
			}
		}(int64(g + 1))
	}
	wg.Wait()
	if l := s.Len(); l < 0 || l > slots {
		t.Errorf("Len = %d, out of range [0,%d]", l, slots)
	}
}
