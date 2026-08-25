package store

import (
	"bytes"
	"container/list"
	"errors"
	"io"
	"sync"
)

// MemStore is an in-memory block cache: a fixed number of fixed-size slabs with
// LRU eviction.
//
// Slabs are allocated once (capacity * SlotSize) and recycled through a
// sync.Pool, so steady-state operation performs no per-block allocation. That
// matters at DART's target throughput: churning multi-megabyte buffers would
// otherwise dominate GC time. Blocks are stored inside a slab and copied out on
// Get, so a caller can never observe a slab that has been recycled.
type MemStore struct {
	slotSize int64
	slots    int

	pool sync.Pool // *[]byte slabs of slotSize

	mu    sync.Mutex
	index map[BlockKey]*memEntry
	lru   *list.List // *memEntry, most-recent at Front
}

// memEntry is one cached block: a slab plus how much of it is in use.
type memEntry struct {
	key  BlockKey
	buf  *[]byte // slab from the pool; len(*buf) == slotSize
	n    int     // valid bytes in buf
	elem *list.Element
}

var _ Store = (*MemStore)(nil)

// MemOptions configures a MemStore.
type MemOptions struct {
	// SlotSize is the fixed bytes per slab, i.e. the maximum block size.
	SlotSize int64
	// Slots is the number of slabs, i.e. the capacity in blocks.
	Slots int
}

// ErrBadMemOptions is returned by OpenMem for malformed options.
var ErrBadMemOptions = errors.New("store: SlotSize and Slots must be > 0")

// OpenMem creates an empty MemStore. Memory is allocated lazily per slab (and
// then recycled), so the store does not reserve SlotSize*Slots up front.
func OpenMem(opt MemOptions) (*MemStore, error) {
	if opt.SlotSize <= 0 || opt.Slots <= 0 {
		return nil, ErrBadMemOptions
	}
	m := &MemStore{
		slotSize: opt.SlotSize,
		slots:    opt.Slots,
		index:    make(map[BlockKey]*memEntry, opt.Slots),
		lru:      list.New(),
	}
	m.pool.New = func() any {
		b := make([]byte, m.slotSize)
		return &b
	}
	return m, nil
}

// Get returns a copy of the block's bytes if cached.
func (m *MemStore) Get(k BlockKey) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.index[k]
	if !ok {
		return nil, false, nil
	}
	m.lru.MoveToFront(e.elem)
	out := make([]byte, e.n)
	copy(out, (*e.buf)[:e.n])
	return out, true, nil
}

// GetReader returns a reader over a copy of the block. The copy is deliberate:
// the slab may be recycled as soon as the lock is released, so handing out a
// reader over live slab memory would risk serving recycled bytes.
func (m *MemStore) GetReader(k BlockKey) (io.Reader, int64, bool, error) {
	data, ok, err := m.Get(k)
	if err != nil || !ok {
		return nil, 0, ok, err
	}
	return bytes.NewReader(data), int64(len(data)), true, nil
}

// Put inserts or refreshes a block, evicting the least-recently-used block when
// at capacity. Blocks are immutable, so an existing key only refreshes recency.
func (m *MemStore) Put(k BlockKey, data []byte) error {
	if len(data) == 0 {
		return ErrEmptyBlock
	}
	if int64(len(data)) > m.slotSize {
		return ErrBlockTooLarge
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.index[k]; ok {
		m.lru.MoveToFront(e.elem)
		return nil
	}
	if len(m.index) >= m.slots {
		m.evictLocked()
	}
	buf := m.pool.Get().(*[]byte)
	copy(*buf, data)
	e := &memEntry{key: k, buf: buf, n: len(data)}
	e.elem = m.lru.PushFront(e)
	m.index[k] = e
	return nil
}

// evictLocked drops the least-recently-used entry and returns its slab to the
// pool. Caller must hold m.mu.
func (m *MemStore) evictLocked() {
	back := m.lru.Back()
	if back == nil {
		return
	}
	e := back.Value.(*memEntry)
	m.lru.Remove(back)
	delete(m.index, e.key)
	m.pool.Put(e.buf)
	e.buf = nil
}

// Has reports whether the block is cached, without affecting LRU order.
func (m *MemStore) Has(k BlockKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.index[k]
	return ok
}

// Delete removes a block if present, recycling its slab.
func (m *MemStore) Delete(k BlockKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.index[k]
	if !ok {
		return false
	}
	m.lru.Remove(e.elem)
	delete(m.index, k)
	m.pool.Put(e.buf)
	e.buf = nil
	return true
}

// Len returns the number of cached blocks.
func (m *MemStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.index)
}

// Slots returns the capacity in blocks.
func (m *MemStore) Slots() int { return m.slots }

// Close drops all entries. A MemStore holds no OS resources, so Close is only
// about releasing memory promptly; it is idempotent.
func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for m.lru.Len() > 0 {
		m.evictLocked()
	}
	m.index = make(map[BlockKey]*memEntry)
	return nil
}
