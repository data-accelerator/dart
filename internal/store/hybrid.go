package store

import (
	"io"
)

// Hybrid puts a small in-memory hot set in front of a larger backing store
// (typically a Tiered disk store).
//
// Both tiers are only caches. DART is a read-only cache, so the source of truth
// is always the remote origin (a registry, OSS bucket, package repo): evicting
// from memory *or* from disk can never lose data, it only costs a re-fetch. The
// tiers differ in what they save: memory removes a disk read from the hot path,
// disk removes an origin fetch.
//
// The memory tier is therefore a mirror of the hot subset, not a layer of
// record: every write goes through to the backing store, and reads check memory
// first, promoting a backing-store hit so a repeatedly-read block ends up served
// from RAM. Capacity accounting and the owned/borrowed budgets stay with the
// backing store.
type Hybrid struct {
	mem  *MemStore
	back ClassStore
}

var _ ClassStore = (*Hybrid)(nil)

// NewHybrid wraps back with an in-memory hot set. It takes ownership of both:
// Close closes each. mem and back must use the same slot size.
func NewHybrid(mem *MemStore, back ClassStore) *Hybrid {
	return &Hybrid{mem: mem, back: back}
}

// Get returns the block, preferring memory and promoting a backing-store hit
// into memory so subsequent reads avoid the disk.
// toucher is implemented by backing stores with an admission estimator
// (Tiered): a mem-tier hit must still feed it, or the key's estimate decays
// while mem serves the reads — and a later demotion back to the borrowed
// budget would face admission with a cold estimate.
type toucher interface{ Touch(BlockKey) }

func (h *Hybrid) touchBack(k BlockKey) {
	if t, ok := h.back.(toucher); ok {
		t.Touch(k)
	}
}

func (h *Hybrid) Get(k BlockKey) ([]byte, bool, error) {
	if data, ok, err := h.mem.Get(k); err != nil || ok {
		if ok {
			h.touchBack(k)
		}
		return data, ok, err
	}
	data, ok, err := h.back.Get(k)
	if err != nil || !ok {
		return data, ok, err
	}
	// Promote into the hot set (best effort: a full memory tier just evicts).
	_ = h.mem.Put(k, data)
	return data, true, nil
}

// Acquire borrows a hot memory hit; a backing hit keeps the caller-owned copy
// while promoting it. A disk miss never pins a reusable disk slot.
func (h *Hybrid) Acquire(k BlockKey) (Lease, bool, error) {
	if l, ok, err := h.mem.Acquire(k); err != nil || ok {
		if ok {
			h.touchBack(k)
		}
		return l, ok, err
	}
	data, ok, err := h.Get(k)
	return HeapLease(data), ok, err
}

// GetReader streams the block, preferring memory.
//
// Unlike Get this does not promote: the streaming path is typically a one-shot
// relay, and promoting would require buffering the whole block, defeating the
// point of streaming. A block that is actually hot gets promoted via Get.
func (h *Hybrid) GetReader(k BlockKey) (io.Reader, int64, bool, error) {
	if r, n, ok, err := h.mem.GetReader(k); err != nil || ok {
		if ok {
			h.touchBack(k)
		}
		return r, n, ok, err
	}
	return h.back.GetReader(k)
}

// Put writes through to the backing store as a borrowed block and mirrors it in
// memory.
func (h *Hybrid) Put(k BlockKey, data []byte) error {
	_, err := h.PutClass(k, data, Borrowed)
	return err
}

// PutClass writes through to the backing store and mirrors the block in memory.
//
// admitted is true when the block ended up cached anywhere: the backing store
// may decline a borrowed block by admission policy while memory still keeps it
// as part of the hot set, which is a legitimate outcome rather than a failure.
func (h *Hybrid) PutClass(k BlockKey, data []byte, c Class) (bool, error) {
	if !h.ClassEnabled(c) {
		// Preserve the backing store's input validation without using the hot
		// set as an implicit replacement for a disabled persistent class.
		return h.back.PutClass(k, data, c)
	}
	admitted, err := h.back.PutClass(k, data, c)
	if err != nil {
		return false, err
	}
	memAdmitted, _ := h.mem.putIfAdmitted(k, data, nil)
	return admitted || memAdmitted, nil
}

// ClassEnabled preserves a backing store's disabled classes across both tiers.
// Backends without a class policy retain the normal hot-set admission behavior.
func (h *Hybrid) ClassEnabled(c Class) bool {
	if s, ok := h.back.(interface{ ClassEnabled(Class) bool }); ok {
		return s.ClassEnabled(c)
	}
	return true
}

// Has reports whether either tier holds the block.
func (h *Hybrid) Has(k BlockKey) bool { return h.mem.Has(k) || h.back.Has(k) }

// Delete removes the block from both tiers.
func (h *Hybrid) Delete(k BlockKey) bool {
	inMem := h.mem.Delete(k)
	inBack := h.back.Delete(k)
	return inMem || inBack
}

// Len returns the number of distinct blocks cached. Memory mirrors the backing
// store, so the backing count is the authoritative total; blocks that live only
// in memory (declined by the backing admission policy) are added on top.
func (h *Hybrid) Len() int { return h.back.Len() + h.memOnlyLen() }

// memOnlyLen counts memory entries the backing store does not have.
func (h *Hybrid) memOnlyLen() int {
	n := 0
	for _, k := range h.mem.keys() {
		if !h.back.Has(k) {
			n++
		}
	}
	return n
}

// Stats reports the backing store's per-class occupancy plus the memory tier.
func (h *Hybrid) Stats() TieredStats {
	st := h.back.Stats()
	st.MemBlocks = h.mem.Len()
	st.MemSlots = h.mem.Slots()
	st.SlotWaits += h.mem.waits.Load()
	st.WriteBypasses += h.mem.bypasses.Load()
	return st
}

// Close closes both tiers.
func (h *Hybrid) Close() error {
	err := h.mem.Close()
	if err2 := h.back.Close(); err == nil {
		err = err2
	}
	return err
}

// keys returns a snapshot of the cached keys (diagnostics / Len accounting).
func (m *MemStore) keys() []BlockKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]BlockKey, 0, m.Len())
	for i := range m.entries {
		if m.entries[i].published {
			out = append(out, m.entries[i].key)
		}
	}
	return out
}
