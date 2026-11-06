package store

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
)

func TestLeasePinsDeletedSlot(t *testing.T) {
	m := openMem(t, 64, 1)
	_ = m.Put(bk(1), []byte("original"))
	l, ok, err := m.Acquire(bk(1))
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	clone := l.Clone()
	l.Release()
	l.Release()
	if !m.Delete(bk(1)) || m.Has(bk(1)) || m.Len() != 0 {
		t.Fatal("delete did not hide block")
	}
	if _, err := m.Reserve(context.Background(), bk(2), 8, Owned); !errors.Is(err, ErrNoSlot) {
		t.Fatalf("pinned slot reused: %v", err)
	}
	if string(clone.Bytes()) != "original" {
		t.Fatal("borrowed bytes changed")
	}
	clone.Release()
	r, err := m.Reserve(context.Background(), bk(2), 3, Owned)
	if err != nil {
		t.Fatal(err)
	}
	copy(r.Bytes(), "new")
	fresh, err := r.Commit()
	if err != nil {
		t.Fatal(err)
	}
	fresh.Release()
	data, _, _ := m.Get(bk(2))
	if string(data) != "new" {
		t.Fatalf("tail exposed: %q", data)
	}
}

func TestReservationPublicationAndAbort(t *testing.T) {
	m := openMem(t, 64, 2)
	r, err := m.Reserve(context.Background(), bk(1), 4, Owned)
	if err != nil {
		t.Fatal(err)
	}
	copy(r.Bytes(), "data")
	if m.Has(bk(1)) || m.Len() != 0 {
		t.Fatal("unvalidated fill visible")
	}
	r.Abort()
	r.Abort()
	r, err = m.Reserve(context.Background(), bk(1), 4, Owned)
	if err != nil {
		t.Fatal(err)
	}
	copy(r.Bytes(), "good")
	l, err := r.Commit()
	if err != nil {
		t.Fatal(err)
	}
	again, ok, err := m.Acquire(bk(1))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if &again.Bytes()[0] != &l.Bytes()[0] {
		t.Fatal("cache did not retain receive slot")
	}
	l.Release()
	again.Release()
	// A simultaneous fill never overwrites the first publication.
	a, _ := m.Reserve(context.Background(), bk(2), 4, Owned)
	m.Delete(bk(1))
	b, _ := m.Reserve(context.Background(), bk(2), 4, Owned)
	copy(a.Bytes(), "aaaa")
	copy(b.Bytes(), "bbbb")
	la, _ := a.Commit()
	lb, _ := b.Commit()
	got, _, _ := m.Get(bk(2))
	if string(got) != "aaaa" || string(lb.Bytes()) != "bbbb" {
		t.Fatal("immutable publication changed")
	}
	la.Release()
	lb.Release()
}

func TestLeasePressureCancellationAndClose(t *testing.T) {
	m := openMem(t, 16, 1)
	r, _ := m.Reserve(context.Background(), bk(1), 4, Owned)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Reserve(ctx, bk(2), 4, Owned); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	copy(r.Bytes(), "safe")
	l, _ := r.Commit()
	_ = m.Close()
	if string(l.Bytes()) != "safe" {
		t.Fatal("close invalidated active lease")
	}
	if _, _, err := m.Acquire(bk(1)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	l.Release()
	if _, err := m.Reserve(context.Background(), bk(3), 4, Owned); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestLeaseSteadyStateAllocations(t *testing.T) {
	m := openMem(t, 64, 8)
	_ = m.Put(bk(1), bytes.Repeat([]byte{7}, 64))
	if n := testing.AllocsPerRun(1000, func() {
		l, ok, err := m.Acquire(bk(1))
		if !ok || err != nil {
			panic("miss")
		}
		c := l.Clone()
		l.Release()
		c.Release()
	}); n != 0 {
		t.Fatalf("borrow allocations = %g", n)
	}
	// Reuse includes new chunk descriptors and both index deletion/insertion.
	i := uint64(10)
	if n := testing.AllocsPerRun(1000, func() {
		i++
		r, err := m.Reserve(context.Background(), BlockKey{Chunk: i, Block: i}, 64, Owned)
		if err != nil {
			panic(err)
		}
		copy(r.Bytes(), "filled")
		l, err := r.Commit()
		if err != nil {
			panic(err)
		}
		l.Release()
	}); n != 0 {
		t.Fatalf("steady refill allocations = %g", n)
	}
}

func TestFixedIndexChurnMatchesMap(t *testing.T) {
	idx := makeIndex(64)
	ref := make(map[[2]uint64]int)
	rng := rand.New(rand.NewSource(827))
	for step := 0; step < 50000; step++ {
		k := [2]uint64{uint64(rng.Intn(8)), uint64(rng.Intn(16))}
		if len(ref) >= 64 || rng.Intn(2) == 0 {
			idx.remove(k[0], k[1])
			delete(ref, k)
		} else {
			ref[k] = step
			idx.put(k[0], k[1], step)
		}
		for a := uint64(0); a < 8; a++ {
			for b := uint64(0); b < 16; b++ {
				want, ok := ref[[2]uint64{a, b}]
				got, found := idx.get(a, b)
				if found != ok || (ok && got != want) {
					t.Fatalf("step %d key %d/%d got %d/%v want %d/%v", step, a, b, got, found, want, ok)
				}
			}
		}
	}
}

func TestLeaseConcurrentReuse(t *testing.T) {
	m := openMem(t, 128, 16)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := BlockKey{Chunk: uint64(i % 23), Block: uint64(g)}
				r, err := m.Reserve(context.Background(), k, 128, Owned)
				if errors.Is(err, ErrNoSlot) {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				for j := range r.Bytes() {
					r.Bytes()[j] = byte(g)
				}
				l, err := r.Commit()
				if err != nil {
					t.Error(err)
					return
				}
				c := l.Clone()
				m.Delete(k)
				l.Release()
				for _, v := range c.Bytes() {
					if v != byte(g) {
						t.Error("slot reused while borrowed")
						break
					}
				}
				c.Release()
			}
		}(g)
	}
	wg.Wait()
}

func TestReservationCloseBeforePublication(t *testing.T) {
	m := openMem(t, 16, 1)
	r, err := m.Reserve(context.Background(), bk(1), 16, Owned)
	if err != nil {
		t.Fatal(err)
	}
	copy(r.Bytes(), "unpublished")
	m.Close()
	if _, err := r.Commit(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if m.pins != 0 || m.Len() != 0 || m.segments[0] != nil {
		t.Fatal("failed commit retained arena")
	}
}

func TestTieredBorrowedLeaseSurvivesPromotion(t *testing.T) {
	m, err := OpenTiered(TieredOptions{Memory: true, SlotSize: 16, Slots: 4, OwnedFraction: .5})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	key := bk(1)
	if ok, err := m.PutClass(key, []byte("data"), Borrowed); !ok || err != nil {
		t.Fatal(ok, err)
	}
	l, ok, err := m.Acquire(key)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, err := m.PutClass(key, []byte("data"), Owned); !ok || err != nil {
		t.Fatal(ok, err)
	}
	stats := m.Stats()
	if stats.OwnedBlocks != 1 || stats.BorrowedBlocks != 0 || string(l.Bytes()) != "data" {
		t.Fatal("promotion invalidated lease", stats)
	}
	l.Release()
	// Both admission and pressure remain best-effort, including owned writes.
	a, _ := m.Reserve(context.Background(), bk(2), 16, Owned)
	pin, _, _ := m.Acquire(key)
	if _, err := m.Reserve(context.Background(), bk(3), 16, Owned); !errors.Is(err, ErrNoSlot) {
		t.Fatal(err)
	}
	if m.Stats().SlotWaits != 1 || m.Stats().WriteBypasses != 1 {
		t.Fatal("pressure counters", m.Stats())
	}
	pin.Release()
	a.Abort()
}
