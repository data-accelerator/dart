package store

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// The tiered layer splits the block cache into two physically isolated budgets:
//
//	owned    blocks this node is the authoritative holder of (HRW top-R).
//	         Default 80% of capacity, LRU, admitted unconditionally.
//	borrowed copies obtained because a local client asked for them or because
//	         this node relayed them. Default 20%, TinyLFU-admitted.
//
// The split is the point: without it a burst of hot borrowed copies would evict
// the owned shard set, destroying the cluster's balanced distributed cache (the
// aggregate capacity that placement depends on). Isolating the budgets means
// borrowed churn can only ever consume its own 20%.
//
// Admission matters for the same reason at a smaller scale: a stream of one-shot
// relayed blocks must not wipe warm borrowed entries, so a candidate is only
// admitted over an eviction victim when TinyLFU estimates it at least as popular
// (see sketch.go).

// Class says which budget a block belongs to.
type Class uint8

const (
	// Owned marks a block this node authoritatively holds (HRW top-R).
	Owned Class = iota
	// Borrowed marks a copy fetched for a local read or relayed for a peer.
	Borrowed
)

// String returns the class name.
func (c Class) String() string {
	if c == Owned {
		return "owned"
	}
	return "borrowed"
}

// ClassStore is a Store that also understands the owned/borrowed split. Plain
// Store methods behave as if writing Borrowed (the conservative default).
type ClassStore interface {
	Store
	// PutClass inserts a block into the given budget. admitted reports whether
	// the block was actually stored: a Borrowed candidate can be rejected by
	// admission, which is not an error.
	PutClass(k BlockKey, data []byte, c Class) (admitted bool, err error)
	// Stats reports per-class occupancy and capacity.
	Stats() TieredStats
}

// TieredStats is a snapshot of per-class occupancy.
type TieredStats struct {
	OwnedBlocks    int
	OwnedSlots     int
	BorrowedBlocks int
	BorrowedSlots  int
	// AdmitRejected counts Borrowed candidates refused by TinyLFU.
	AdmitRejected uint64
	// MemBlocks and MemSlots describe the in-memory hot set when the store is
	// wrapped in a Hybrid; both are zero for a plain Tiered store.
	MemBlocks int
	MemSlots  int
}

// TieredOptions configures a Tiered store.
type TieredOptions struct {
	// Path, SlotSize and Slots have the same meaning as store.Options; Slots is
	// the total capacity, split between the two budgets.
	Path     string
	SlotSize int64
	Slots    int
	// OwnedFraction is the share of Slots reserved for owned blocks
	// (0 < f < 1); 0 (the zero value) means "unset" and defaults to 0.8.
	// Negative or >= 1 values are rejected with ErrBadTieredOptions — never
	// silently substituted. Each budget always gets at least one slot.
	OwnedFraction float64
}

// Tiered is a ClassStore backed by two independent DiskStores over separate
// files, so neither budget can evict the other's blocks.
type Tiered struct {
	owned    *DiskStore
	borrowed *DiskStore

	sk *sketch

	mu       sync.Mutex
	rejected uint64
}

var _ ClassStore = (*Tiered)(nil)

// ErrBadTieredOptions is returned by OpenTiered for malformed options.
var ErrBadTieredOptions = errors.New("store: SlotSize and Slots must be > 0 and Slots >= 2")

// OpenTiered creates the two backing files (Path+".owned", Path+".borrowed")
// and returns an empty Tiered store.
func OpenTiered(opt TieredOptions) (*Tiered, error) {
	if opt.SlotSize <= 0 || opt.Slots < 2 {
		return nil, ErrBadTieredOptions
	}
	f := opt.OwnedFraction
	if f < 0 || f >= 1 {
		// Fail loudly: silently substituting 0.8 would turn "disable the owned
		// budget" into "reserve 80% of the cache for it" — the opposite.
		return nil, ErrBadTieredOptions
	}
	if f == 0 {
		f = 0.8 // zero value = unset; the node validates an explicitly-set 0
	}
	ownedSlots := int(float64(opt.Slots) * f)
	if ownedSlots < 1 {
		ownedSlots = 1
	}
	borrowedSlots := opt.Slots - ownedSlots
	if borrowedSlots < 1 {
		borrowedSlots = 1
		if ownedSlots > 1 {
			ownedSlots--
		}
	}

	o, err := Open(Options{Path: opt.Path + ".owned", SlotSize: opt.SlotSize, Slots: ownedSlots})
	if err != nil {
		return nil, fmt.Errorf("store: owned budget: %w", err)
	}
	b, err := Open(Options{Path: opt.Path + ".borrowed", SlotSize: opt.SlotSize, Slots: borrowedSlots})
	if err != nil {
		o.Close()
		return nil, fmt.Errorf("store: borrowed budget: %w", err)
	}
	return &Tiered{owned: o, borrowed: b, sk: newSketch(borrowedSlots)}, nil
}

// keyHash derives the sketch hash for a block key.
func keyHash(k BlockKey) uint64 { return fmix64(k.Chunk ^ (k.Block * 0x9e3779b97f4a7c15)) }

// Get returns the block from either budget. Owned is checked first: it is the
// authoritative copy. Only traffic the admission policy governs feeds the
// estimator — borrowed hits and misses. Owned hits are excluded: owned blocks
// are never evicted via the sketch, and counting them would drive the halving
// cadence (sized for the borrowed budget) far too fast, decaying warm borrowed
// estimates to zero where a one-shot could evict them.
func (t *Tiered) Get(k BlockKey) ([]byte, bool, error) {
	if data, ok, err := t.owned.Get(k); err != nil || ok {
		return data, ok, err
	}
	t.sk.Increment(keyHash(k))
	return t.borrowed.Get(k)
}

// GetReader streams the block from either budget, with the same estimator
// scoping as Get.
func (t *Tiered) GetReader(k BlockKey) (io.Reader, int64, bool, error) {
	if r, n, ok, err := t.owned.GetReader(k); err != nil || ok {
		return r, n, ok, err
	}
	t.sk.Increment(keyHash(k))
	return t.borrowed.GetReader(k)
}

// Touch records an access to k in the admission estimator without reading the
// block: used by wrappers (Hybrid) whose fast tier answers the read but must
// not let the key's borrowed-budget estimate decay while it does. Owned keys
// are skipped — owned traffic never feeds the estimator (see Get).
func (t *Tiered) Touch(k BlockKey) {
	if t.owned.Has(k) {
		return
	}
	t.sk.Increment(keyHash(k))
}

// Put inserts the block into the borrowed budget (the conservative default for
// callers that do not know the class). An admission rejection is not an error.
func (t *Tiered) Put(k BlockKey, data []byte) error {
	_, err := t.PutClass(k, data, Borrowed)
	return err
}

// PutClass inserts a block into the given budget.
//
// Owned blocks are admitted unconditionally: this node is responsible for them,
// so refusing one would create a hole in the distributed cache. Borrowed blocks
// go through TinyLFU admission once the budget is full, so a one-shot block
// cannot evict a warm one.
func (t *Tiered) PutClass(k BlockKey, data []byte, c Class) (bool, error) {
	if c == Owned {
		if err := t.owned.Put(k, data); err != nil {
			return false, err
		}
		// A block lives in exactly one budget: promoting to owned drops the
		// borrowed copy, or Len/Stats would double-count and the authoritative
		// block would stay evictable.
		t.borrowed.Delete(k)
		return true, nil
	}

	// Already owned: nothing to admit, but still refresh recency as promised —
	// the copy stays put (owned is authoritative; a borrowed copy must not
	// shadow it).
	if t.owned.Has(k) {
		return true, t.owned.Put(k, data)
	}
	if t.borrowed.Has(k) {
		return true, t.borrowed.Put(k, data)
	}

	// Insertion is itself an access: feed the estimator so a write-only
	// pattern does not leave every estimate at 0 (where admission degenerates
	// to always-admit).
	t.sk.Increment(keyHash(k))

	// Below capacity: admitted freely. At capacity: admitted only when at
	// least as popular as the block that would be evicted. The TinyLFU
	// comparison and the evict+insert run inside the borrowed store's lock
	// (putIfAdmitted), so the victim compared is exactly the victim evicted —
	// no concurrent Get/Delete/Put can reorder the LRU in between.
	admitted, err := t.borrowed.putIfAdmitted(k, data, func(victim BlockKey) bool {
		return t.sk.Admit(keyHash(k), keyHash(victim))
	})
	if err != nil {
		return false, err
	}
	if !admitted {
		t.mu.Lock()
		t.rejected++
		t.mu.Unlock()
	}
	return admitted, nil
}

// Has reports whether either budget holds the block.
func (t *Tiered) Has(k BlockKey) bool { return t.owned.Has(k) || t.borrowed.Has(k) }

// Delete removes the block from either budget.
func (t *Tiered) Delete(k BlockKey) bool {
	d1 := t.owned.Delete(k)
	d2 := t.borrowed.Delete(k)
	return d1 || d2
}

// Len returns the total number of cached blocks across both budgets.
func (t *Tiered) Len() int { return t.owned.Len() + t.borrowed.Len() }

// Stats reports per-class occupancy.
func (t *Tiered) Stats() TieredStats {
	t.mu.Lock()
	rejected := t.rejected
	t.mu.Unlock()
	return TieredStats{
		OwnedBlocks:    t.owned.Len(),
		OwnedSlots:     t.owned.Slots(),
		BorrowedBlocks: t.borrowed.Len(),
		BorrowedSlots:  t.borrowed.Slots(),
		AdmitRejected:  rejected,
	}
}

// Close releases both budgets.
func (t *Tiered) Close() error {
	err1 := t.owned.Close()
	if err2 := t.borrowed.Close(); err1 == nil {
		err1 = err2
	}
	return err1
}
