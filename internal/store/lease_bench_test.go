package store

import (
	"context"
	"runtime"
	"testing"
)

// The borrow benchmark measures ownership overhead, not transferred bandwidth:
// its reader intentionally does not traverse all bytes. Real HTTP measurements
// cover payload transfer. Get is the compatibility-copy control.
func BenchmarkManagedMemoryRead(b *testing.B) {
	const block = 4 << 20
	m, err := OpenMem(MemOptions{SlotSize: block, Slots: 32})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	data := make([]byte, block)
	for i := 0; i < 32; i++ {
		m.Put(BlockKey{Chunk: 1, Block: uint64(i)}, data)
	}
	b.Run("Borrow", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := uint64(0)
			for pb.Next() {
				i++
				l, ok, err := m.Acquire(BlockKey{Chunk: 1, Block: i % 32})
				if !ok || err != nil {
					panic("miss")
				}
				runtime.KeepAlive(l.Bytes())
				l.Release()
			}
		})
	})
	b.Run("OwnedCopy", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(block)
		b.RunParallel(func(pb *testing.PB) {
			i := uint64(0)
			for pb.Next() {
				i++
				data, ok, err := m.Get(BlockKey{Chunk: 1, Block: i % 32})
				if !ok || err != nil {
					panic("miss")
				}
				runtime.KeepAlive(data)
			}
		})
	})
}

func BenchmarkManagedMemoryRefill(b *testing.B) {
	const block = 4 << 20
	m, err := OpenMem(MemOptions{SlotSize: block, Slots: 16})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	data := make([]byte, block)
	for i := 0; i < 16; i++ {
		m.Put(BlockKey{Chunk: uint64(i)}, data)
	}
	b.ReportAllocs()
	b.SetBytes(block)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := m.Reserve(context.Background(), BlockKey{Chunk: uint64(i + 32)}, block, Owned)
		if err != nil {
			b.Fatal(err)
		}
		copy(r.Bytes(), data)
		l, err := r.Commit()
		if err != nil {
			b.Fatal(err)
		}
		l.Release()
	}
}
