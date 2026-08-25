// Package store is DART's block cache: fixed-size blocks keyed by BlockKey,
// backed by a preallocated slot arena for capacity beyond RAM.
//
// This first implementation is the disk tier: a single preallocated file split
// into equal-size slots, with an in-memory index, an LRU eviction list, and a
// free list. Because blocks are fixed-size and read-only, eviction is just
// "free a slot" — O(1), zero fragmentation, no compaction.
//
// Persistence across restarts is intentionally NOT provided yet: the store
// starts cold every boot. Under DART's read-only semantics a cold cache is
// always safe (blocks are simply re-fetched from origin), and this avoids any
// risk of serving stale/torn data after a crash. Warm restart (a WAL or
// per-slot commit header) is a future addition; see docs/store.md §8.
package store

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// BlockKey identifies one block: the chunk it belongs to (the HRW/placement
// unit) and the block index within the object.
type BlockKey struct {
	Chunk uint64
	Block uint64
}

// Store is a cache of fixed-size blocks keyed by BlockKey.
//
// Implementations are safe for concurrent use. Get returns a copy of the block
// bytes; the caller owns it and may modify it freely.
type Store interface {
	// Get returns the block's bytes if cached. ok is false on a miss.
	Get(k BlockKey) (data []byte, ok bool, err error)
	// GetReader returns a reader over the cached block plus its length, for
	// streaming the block out without copying it into a caller buffer. ok is
	// false on a miss.
	//
	// The reader is valid only until the block's slot is reused (eviction), so
	// callers must consume it promptly; for long-lived use, copy the bytes.
	GetReader(k BlockKey) (r io.Reader, n int64, ok bool, err error)
	// Put inserts or refreshes a block. len(data) must be in (0, SlotSize].
	// Inserting when at capacity evicts the least-recently-used block.
	Put(k BlockKey, data []byte) error
	// Has reports whether the block is cached (without touching LRU).
	Has(k BlockKey) bool
	// Delete removes a block if present, returning whether it existed.
	Delete(k BlockKey) bool
	// Len returns the number of cached blocks.
	Len() int
	// Close releases resources.
	Close() error
}

// Options configures a DiskStore.
type Options struct {
	// Path is the backing data file; it is created and truncated to
	// SlotSize*Slots. Its directory must exist and be writable.
	Path string
	// SlotSize is the fixed bytes per slot, i.e. the maximum block size.
	SlotSize int64
	// Slots is the number of slots, i.e. the capacity in blocks.
	Slots int
}

var (
	// ErrBadOptions is returned by Open for malformed Options.
	ErrBadOptions = errors.New("store: SlotSize must be > 0 and Slots must be > 0")
	// ErrBlockTooLarge is returned by Put when len(data) > SlotSize.
	ErrBlockTooLarge = errors.New("store: block larger than slot size")
	// ErrEmptyBlock is returned by Put for zero-length data.
	ErrEmptyBlock = errors.New("store: empty block")
)

type slotMeta struct {
	key    BlockKey
	length int64
	inUse  bool
}

// DiskStore is a disk-backed, capacity-bounded, LRU block cache. Safe for
// concurrent use (guarded by a single mutex in this first cut; per-shard
// locking is a future optimization).
type DiskStore struct {
	slotSize int64

	mu    sync.Mutex
	f     *os.File
	index map[BlockKey]int      // key -> slot id
	meta  []slotMeta            // per-slot metadata (len == Slots)
	free  []int                 // free slot ids (stack)
	lru   *list.List            // in-use slot ids, most-recent at Front
	elem  map[int]*list.Element // slot id -> its LRU element
}

var _ Store = (*DiskStore)(nil)

// Open creates (or truncates) the backing file and returns an empty DiskStore.
// The store always starts cold; any pre-existing file contents are discarded.
func Open(opt Options) (*DiskStore, error) {
	if opt.SlotSize <= 0 || opt.Slots <= 0 {
		return nil, ErrBadOptions
	}
	// O_TRUNC: the index lives only in memory, so a reopened store is always cold
	// and previously-written slots can never be reused. Keeping their contents
	// would therefore buy nothing while leaving stale bytes readable on disk
	// (including bytes fetched from an authenticated origin), so the arena is
	// rebuilt empty. It also makes occupancy predictable: a fresh process starts
	// from zero allocated blocks rather than inheriting whatever the last run left.
	//
	// This assumes one process per cache directory. Two instances sharing a path
	// would already corrupt each other's slots; with O_TRUNC the second one wipes
	// the first immediately.
	f, err := os.OpenFile(opt.Path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open %q: %w", opt.Path, err)
	}
	// Grow to the full arena size. Truncating up leaves a hole, so the file is
	// sparse: it reports its full size but only occupies the blocks actually
	// written.
	if err := f.Truncate(opt.SlotSize * int64(opt.Slots)); err != nil {
		f.Close()
		return nil, fmt.Errorf("store: truncate %q: %w", opt.Path, err)
	}
	s := &DiskStore{
		slotSize: opt.SlotSize,
		f:        f,
		index:    make(map[BlockKey]int, opt.Slots),
		meta:     make([]slotMeta, opt.Slots),
		free:     make([]int, opt.Slots),
		lru:      list.New(),
		elem:     make(map[int]*list.Element, opt.Slots),
	}
	for i := 0; i < opt.Slots; i++ {
		s.free[i] = opt.Slots - 1 - i // pop from the end gives ascending ids
	}
	return s, nil
}

func (s *DiskStore) offset(slot int) int64 { return int64(slot) * s.slotSize }

// Get returns a copy of the block's bytes if cached.
func (s *DiskStore) Get(k BlockKey) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, ok := s.index[k]
	if !ok {
		return nil, false, nil
	}
	buf := make([]byte, s.meta[slot].length)
	if _, err := s.f.ReadAt(buf, s.offset(slot)); err != nil {
		return nil, false, fmt.Errorf("store: read slot %d: %w", slot, err)
	}
	s.lru.MoveToFront(s.elem[slot])
	return buf, true, nil
}

// GetReader returns an io.Reader over the cached block and its length, so the
// caller can stream it out (e.g. straight to a socket) without materializing a
// copy. It refreshes LRU recency like Get.
//
// The returned reader is an io.SectionReader over the backing file at the
// block's slot: it is valid until that slot is reused by eviction, so consume it
// promptly rather than holding it.
func (s *DiskStore) GetReader(k BlockKey) (io.Reader, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, ok := s.index[k]
	if !ok {
		return nil, 0, false, nil
	}
	if s.f == nil {
		return nil, 0, false, errors.New("store: closed")
	}
	n := s.meta[slot].length
	s.lru.MoveToFront(s.elem[slot])
	return io.NewSectionReader(s.f, s.offset(slot), n), n, true, nil
}

// Put inserts or refreshes a block, evicting the LRU block if at capacity.
func (s *DiskStore) Put(k BlockKey, data []byte) error {
	_, err := s.putIfAdmitted(k, data, nil)
	return err
}

// putIfAdmitted is Put with an admission gate on eviction: when the store is
// full and inserting k would evict the LRU block, the gate is consulted with
// the victim's key and a false answer aborts the insertion (admitted=false,
// nil) without evicting. The whole decide-then-evict-then-insert sequence runs
// under s.mu, so the victim compared is exactly the victim evicted — no
// concurrent Get/Delete/Put can reorder the LRU in between.
func (s *DiskStore) putIfAdmitted(k BlockKey, data []byte, admit func(victim BlockKey) bool) (bool, error) {
	if len(data) == 0 {
		return false, ErrEmptyBlock
	}
	if int64(len(data)) > s.slotSize {
		return false, ErrBlockTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if slot, ok := s.index[k]; ok {
		s.lru.MoveToFront(s.elem[slot])
		return true, nil
	}

	if len(s.free) == 0 {
		back := s.lru.Back()
		victim := s.meta[back.Value.(int)].key
		if admit != nil && !admit(victim) {
			return false, nil
		}
	}

	slot := s.acquireSlot()
	if _, err := s.f.WriteAt(data, s.offset(slot)); err != nil {
		s.free = append(s.free, slot)
		return false, fmt.Errorf("store: write slot %d: %w", slot, err)
	}
	s.meta[slot] = slotMeta{key: k, length: int64(len(data)), inUse: true}
	s.index[k] = slot
	s.elem[slot] = s.lru.PushFront(slot)
	return true, nil
}

// acquireSlot returns a free slot id, evicting the LRU block if none are free.
// Caller must hold s.mu.
func (s *DiskStore) acquireSlot() int {
	if n := len(s.free); n > 0 {
		slot := s.free[n-1]
		s.free = s.free[:n-1]
		return slot
	}
	// Evict the least-recently-used block and reuse its slot directly.
	back := s.lru.Back()
	slot := back.Value.(int)
	s.lru.Remove(back)
	delete(s.elem, slot)
	delete(s.index, s.meta[slot].key)
	s.meta[slot].inUse = false
	return slot
}

// Has reports whether the block is cached, without affecting LRU order.
func (s *DiskStore) Has(k BlockKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.index[k]
	return ok
}

// Delete removes a block if present.
func (s *DiskStore) Delete(k BlockKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, ok := s.index[k]
	if !ok {
		return false
	}
	s.lru.Remove(s.elem[slot])
	delete(s.elem, slot)
	delete(s.index, k)
	s.meta[slot].inUse = false
	s.free = append(s.free, slot)
	return true
}

// Len returns the number of cached blocks.
func (s *DiskStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.index)
}

// Slots returns the store's capacity in blocks.
func (s *DiskStore) Slots() int { return len(s.meta) }

// Close closes the backing file. The store must not be used afterwards.
func (s *DiskStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
