package store

import (
	"bytes"
	"math"
	"path/filepath"
	"sync"
	"testing"
)

func TestMemoryTieredBudgetsAndAdmission(t *testing.T) {
	ts, err := OpenTiered(TieredOptions{Memory: true, Path: filepath.Join(t.TempDir(), "unused", "blocks"), SlotSize: 64, Slots: 4, OwnedFraction: .5})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	data := []byte("immutable")
	for _, k := range []BlockKey{bk(1), bk(2)} {
		if ok, err := ts.PutClass(k, data, Owned); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	for _, k := range []BlockKey{bk(3), bk(4)} {
		if ok, err := ts.PutClass(k, data, Borrowed); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	for i := 0; i < 30; i++ {
		ts.Get(bk(3))
		ts.Get(bk(4))
	}
	if ok, err := ts.PutClass(bk(5), data, Borrowed); err != nil || ok {
		t.Fatalf("cold candidate admitted=%v err=%v", ok, err)
	}
	if !ts.Has(bk(1)) || !ts.Has(bk(2)) || !ts.Has(bk(3)) || !ts.Has(bk(4)) {
		t.Fatal("borrowed admission displaced a protected block")
	}
	if ok, err := ts.PutClass(bk(3), data, Owned); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ts.borrowed.Has(bk(3)) || !ts.owned.Has(bk(3)) {
		t.Fatal("promotion retained duplicate or wrong class")
	}
	if ok, err := ts.PutClass(bk(3), []byte("different"), Borrowed); err != nil || !ok {
		t.Fatal(ok, err)
	}
	got, ok, err := ts.Get(bk(3))
	if err != nil || !ok || !bytes.Equal(got, data) {
		t.Fatal("immutable owned data overwritten")
	}
	got[0] ^= 255
	again, _, _ := ts.Get(bk(3))
	if !bytes.Equal(again, data) {
		t.Fatal("Get leaked slab ownership")
	}
	st := ts.Stats()
	if st.OwnedSlots != 2 || st.BorrowedSlots != 2 || st.OwnedBlocks != 2 || st.BorrowedBlocks != 1 || st.MemSlots != 4 || st.MemBlocks != 3 || st.AdmitRejected != 1 || ts.Len() != 3 {
		t.Fatalf("stats %+v len=%d", st, ts.Len())
	}
	if err := ts.Close(); err != nil || ts.Len() != 0 || ts.Stats().MemBlocks != 0 {
		t.Fatal("close retained occupancy", err)
	}
}

func TestMemoryTieredValidationAndConcurrency(t *testing.T) {
	for _, opt := range []TieredOptions{{SlotSize: 0, Slots: 4}, {SlotSize: 16, Slots: 1}, {SlotSize: 16, Slots: 4, OwnedFraction: math.NaN()}, {SlotSize: 16, Slots: 4, OwnedFraction: 1}} {
		opt.Memory = true
		if _, err := OpenTiered(opt); err != ErrBadTieredOptions {
			t.Fatalf("invalid options: %+v %v", opt, err)
		}
	}
	ts, err := OpenTiered(TieredOptions{Memory: true, SlotSize: 16, Slots: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				k := bk(g*100 + i)
				if _, err := ts.PutClass(k, []byte{byte(i)}, Class(g%2)); err != nil {
					t.Error(err)
				}
				ts.Get(k)
				ts.GetReader(k)
				ts.Touch(k)
				ts.Has(k)
				ts.Delete(k)
				ts.Stats()
			}
		}(g)
	}
	wg.Wait()
	st := ts.Stats()
	if st.MemBlocks > st.MemSlots || st.OwnedBlocks > st.OwnedSlots || st.BorrowedBlocks > st.BorrowedSlots {
		t.Fatalf("capacity exceeded %+v", st)
	}
}

func TestMemAdmissionRejectKeepsVictim(t *testing.T) {
	m := openMem(t, 16, 1)
	m.Put(bk(1), []byte("a"))
	called := false
	ok, err := m.putIfAdmitted(bk(2), []byte("b"), func(victim BlockKey) bool {
		called = true
		if victim != bk(1) {
			t.Fatal("wrong LRU victim")
		}
		return false
	})
	if err != nil || ok || !called || !m.Has(bk(1)) || m.Has(bk(2)) {
		t.Fatalf("rejected candidate mutated store: %v %v", ok, err)
	}
	if err := m.Put(bk(2), []byte("b")); err != nil || m.Has(bk(1)) || !m.Has(bk(2)) {
		t.Fatal("ordinary LRU insertion broken", err)
	}
}
