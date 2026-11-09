package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHeapLeaseFinalRelease(t *testing.T) {
	var released atomic.Int32
	l := HeapLeaseWithRelease([]byte("immutable"), func() { released.Add(1) })
	last := l.Clone()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		owner := l.Clone()
		wg.Add(1)
		go func() {
			defer wg.Done()
			child := owner.Clone()
			if string(child.Bytes()) != "immutable" {
				t.Error("clone lost data")
			}
			owner.Release()
			owner.Release()
			child.Release()
		}()
	}
	l.Release()
	wg.Wait()
	if released.Load() != 0 || string(last.Bytes()) != "immutable" {
		t.Fatal("capacity returned before final reader")
	}
	last.Release()
	last.Release()
	if released.Load() != 1 {
		t.Fatal("final callback count", released.Load())
	}
	plain := HeapLeaseWithRelease([]byte("plain"), nil)
	plain.Release()
}

func TestReservationDetachRetainsUnpublishedSlot(t *testing.T) {
	m := openMem(t, 16, 1)
	r, err := m.Reserve(context.Background(), bk(1), 4, Owned)
	if err != nil {
		t.Fatal(err)
	}
	copy(r.Bytes(), "data")
	address := &r.Bytes()[0]
	l := r.Detach()
	r.Abort()
	clone := l.Clone()
	l.Release()
	if m.Has(bk(1)) || m.Len() != 0 || &clone.Bytes()[0] != address {
		t.Fatal("detach published or copied an uncached fill")
	}
	if _, err := m.Reserve(context.Background(), bk(2), 4, Owned); !errors.Is(err, ErrNoSlot) {
		t.Fatal("unpublished reader lost its pin", err)
	}
	clone.Release()
	next, err := m.Reserve(context.Background(), bk(2), 4, Owned)
	if err != nil {
		t.Fatal("last reader leaked slot", err)
	}
	next.Abort()
}
