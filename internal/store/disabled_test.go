package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSingleClassBudgetsAcrossBackends(t *testing.T) {
	for _, mode := range []string{"memory", "disk", "hybrid"} {
		for _, active := range []Class{Owned, Borrowed} {
			t.Run(mode+"/"+active.String(), func(t *testing.T) {
				fraction := 1.0
				disabled := Borrowed
				if active == Borrowed {
					fraction, disabled = 0, Owned
				}
				path := filepath.Join(t.TempDir(), "blocks")
				back, err := OpenTiered(TieredOptions{Path: path, SlotSize: 16, Slots: 1, OwnedFraction: fraction, OwnedFractionSet: true, Memory: mode == "memory"})
				if err != nil {
					t.Fatal(err)
				}
				var st ClassStore = back
				if mode == "hybrid" {
					mem, err := OpenMem(MemOptions{SlotSize: 16, Slots: 1})
					if err != nil {
						back.Close()
						t.Fatal(err)
					}
					st = NewHybrid(mem, back)
				}
				defer st.Close()
				if !back.ClassEnabled(active) || back.ClassEnabled(disabled) {
					t.Fatal("class policy mismatch")
				}
				if (back.sk == nil) != (active == Owned) {
					t.Fatal("disabled borrowed budget allocated an admission sketch")
				}
				stats := st.Stats()
				if stats.OwnedSlots+stats.BorrowedSlots != 1 || (stats.OwnedSlots == 0) != (active == Borrowed) {
					t.Fatal(stats)
				}
				if _, err := os.Stat(path + "." + disabled.String()); !os.IsNotExist(err) {
					t.Fatalf("disabled class opened a file: %v", err)
				}
				if mode != "memory" {
					if _, err := os.Stat(path + "." + active.String()); err != nil {
						t.Fatal(err)
					}
				}
				key := BlockKey{Chunk: 1}
				if ok, err := st.PutClass(key, []byte("retained"), active); err != nil || !ok {
					t.Fatalf("active class: %v %v", ok, err)
				}
				pin, ok, err := Acquire(st, key)
				if err != nil || !ok {
					t.Fatal(err)
				}
				defer pin.Release()
				var wg sync.WaitGroup
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						for j := 0; j < 32; j++ {
							k := BlockKey{Chunk: uint64(100 + i*32 + j)}
							if ok, err := st.PutClass(k, []byte("skip"), disabled); err != nil || ok {
								t.Errorf("disabled write admitted: %v %v", ok, err)
							}
							if st.Has(k) {
								t.Error("disabled class consumed another tier")
							}
							if r, _, hit, err := st.GetReader(k); err != nil || hit || r != nil {
								t.Error("disabled key readable")
							}
						}
					}(i)
				}
				wg.Wait()
				if string(pin.Bytes()) != "retained" || !st.Has(key) || st.Len() != 1 {
					t.Fatal("disabled writes displaced the active class")
				}
				if _, err := back.Reserve(context.Background(), BlockKey{Chunk: 9}, 4, disabled); !errors.Is(err, ErrNoSlot) {
					t.Fatalf("disabled reserve: %v", err)
				}
				if stats := st.Stats(); stats.SlotWaits != 0 || stats.AdmitRejected != 0 {
					t.Fatalf("disabled class waited or ran TinyLFU: %+v", stats)
				}
				if !st.Delete(key) || st.Delete(BlockKey{Chunk: 9}) || st.Len() != 0 {
					t.Fatal("delete across disabled budget failed")
				}
			})
		}
	}
}

func TestOwnedFractionUnsetAndExplicitZero(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		s, err := OpenTiered(TieredOptions{Memory: true, SlotSize: 16, Slots: 10, OwnedFractionSet: explicit})
		if err != nil {
			t.Fatal(err)
		}
		want := 8
		if explicit {
			want = 0
		}
		if s.Stats().OwnedSlots != want {
			t.Fatalf("explicit=%v: %+v", explicit, s.Stats())
		}
		s.Close()
	}
}
