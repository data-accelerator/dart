package store

// Regression tests for issue #18 A1: MemStore.Len must be O(1) (maintained
// counter) and exact across every mutation path — Put, refresh-Put, evict,
// Delete, Close.

import (
	"testing"
)

func TestMemStoreLenCounterExact(t *testing.T) {
	m, err := OpenMem(MemOptions{SlotSize: 16, Slots: 4})
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	defer m.Close()

	k := func(i int) BlockKey { return BlockKey{Chunk: uint64(i), Block: 0} }
	if got := m.Len(); got != 0 {
		t.Fatalf("empty Len = %d", got)
	}
	// Insert 6 into 4 slots: 2 evictions along the way.
	for i := 0; i < 6; i++ {
		if err := m.Put(k(i), []byte("block")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if got := m.Len(); got != 4 {
		t.Fatalf("after 6 puts into 4 slots Len = %d, want 4", got)
	}
	// A refresh Put must not double-count.
	if err := m.Put(k(5), []byte("block")); err != nil {
		t.Fatal(err)
	}
	if got := m.Len(); got != 4 {
		t.Fatalf("after refresh Put Len = %d, want 4", got)
	}
	if !m.Delete(k(5)) {
		t.Fatal("Delete of a present key returned false")
	}
	if got := m.Len(); got != 3 {
		t.Fatalf("after Delete Len = %d, want 3", got)
	}
	if m.Delete(k(5)) {
		t.Fatal("double Delete returned true")
	}
	if got := m.Len(); got != 3 {
		t.Fatalf("after double Delete Len = %d, want 3", got)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("after Close Len = %d, want 0", got)
	}
}

// BenchmarkMemStoreLen documents the O(1) property: before the counter, Len
// scanned all slots under the lock; /admin/cache calls it on every scrape.
func BenchmarkMemStoreLen(b *testing.B) {
	m, err := OpenMem(MemOptions{SlotSize: 16, Slots: 10000})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	for i := 0; i < 10000; i++ {
		if err := m.Put(BlockKey{Chunk: uint64(i), Block: 0}, []byte("block")); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Len()
	}
}
