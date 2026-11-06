package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// MemStore manages fixed slots in lazy arena segments. Descriptor arrays and
// two-level indices are allocated at OpenMem. Metadata locking never spans
// payload copies, arena allocation or network operations.
type MemStore struct {
	pins                            int
	slotSize                        int64
	slots                           int
	mu                              sync.Mutex
	closed                          bool
	entries                         []memEntry
	chunks                          []memChunk
	chunkIndex, blockIndex          fixedIndex
	free, freeChunk, oldest, newest int
	n                               atomic.Int64
	waits, bypasses                 atomic.Uint64
	available                       chan struct{}
	arenaMu                         sync.Mutex
	segments                        [][]byte
	segmentSlots                    int
}
type memEntry struct {
	key            BlockKey
	chunk, n, refs int
	generation     uint64
	published      bool
	prev, next     int
	data           []byte
}
type memChunk struct {
	key          uint64
	blocks, next int
}

var _ Store = (*MemStore)(nil)

// MemOptions configures capacity. Payload segments are at most 64 MiB, or one
// block if SlotSize exceeds that size; unused segments are not allocated.
type MemOptions struct {
	SlotSize int64
	Slots    int
}

// ErrBadMemOptions reports invalid or unrepresentable geometry.
var ErrBadMemOptions = errors.New("store: SlotSize and Slots must be > 0")

// OpenMem allocates bounded metadata and starts without payload segments.
func OpenMem(opt MemOptions) (*MemStore, error) {
	maxInt := int(^uint(0) >> 1)
	if opt.SlotSize <= 0 || opt.Slots <= 0 || opt.SlotSize > int64(maxInt) || opt.Slots > maxInt/4 {
		return nil, ErrBadMemOptions
	}
	segmentSlots := int((64 << 20) / opt.SlotSize)
	if segmentSlots < 1 {
		segmentSlots = 1
	}
	if segmentSlots > opt.Slots {
		segmentSlots = opt.Slots
	}
	m := &MemStore{slotSize: opt.SlotSize, slots: opt.Slots,
		entries: make([]memEntry, opt.Slots), chunks: make([]memChunk, opt.Slots),
		chunkIndex: makeIndex(opt.Slots), blockIndex: makeIndex(opt.Slots),
		oldest: -1, newest: -1, available: make(chan struct{}, 1), segmentSlots: segmentSlots,
		segments: make([][]byte, (opt.Slots-1)/segmentSlots+1)}
	for i := range m.entries {
		m.entries[i].next = i + 1
		m.chunks[i].next = i + 1
	}
	m.entries[opt.Slots-1].next = -1
	m.chunks[opt.Slots-1].next = -1
	return m, nil
}
func (m *MemStore) find(k BlockKey) (int, bool) {
	c, ok := m.chunkIndex.get(k.Chunk, 0)
	if !ok {
		return 0, false
	}
	return m.blockIndex.get(uint64(c), k.Block)
}

// Acquire pins a block without allocating or copying its payload.
func (m *MemStore) Acquire(k BlockKey) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Lease{}, false, ErrClosed
	}
	i, ok := m.find(k)
	if !ok {
		return Lease{}, false, nil
	}
	e := &m.entries[i]
	if e.refs == 0 {
		m.unlink(i)
	}
	e.refs++
	m.pins++
	return Lease{data: e.data[:e.n:e.n], mem: m, slot: i, generation: e.generation}, true, nil
}

// Get preserves the caller-owned copy contract, copying outside the lock.
func (m *MemStore) Get(k BlockKey) ([]byte, bool, error) {
	l, ok, err := m.Acquire(k)
	if !ok || err != nil {
		return nil, ok, err
	}
	defer l.Release()
	return append([]byte(nil), l.Bytes()...), true, nil
}

// GetReader returns an independent copy for legacy callers.
func (m *MemStore) GetReader(k BlockKey) (io.Reader, int64, bool, error) {
	data, ok, err := m.Get(k)
	if !ok || err != nil {
		return nil, 0, ok, err
	}
	return bytes.NewReader(data), int64(len(data)), true, nil
}

// Reserve obtains unpublished storage. If every slot is pinned/filling it waits
// at most 2 ms, cancellably. Class is ignored by this unclassified store.
func (m *MemStore) Reserve(ctx context.Context, k BlockKey, n int64, _ Class) (Reservation, error) {
	return m.reserve(ctx, k, n, nil)
}
func (m *MemStore) reserve(ctx context.Context, k BlockKey, n int64, admit func(BlockKey) bool) (Reservation, error) {
	if n <= 0 {
		return Reservation{}, ErrEmptyBlock
	}
	if n > m.slotSize {
		return Reservation{}, ErrBlockTooLarge
	}
	var timer *time.Timer
	expired := false
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return Reservation{}, err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return Reservation{}, ErrClosed
		}
		i := m.free
		if i < 0 && m.oldest >= 0 {
			i = m.oldest
			if admit != nil && !admit(m.entries[i].key) {
				m.mu.Unlock()
				m.bypasses.Add(1)
				return Reservation{}, ErrNoSlot
			}
			m.remove(i)
			i = m.free
		}
		if i >= 0 {
			e := &m.entries[i]
			m.free = e.next
			e.refs = 1
			m.pins++
			e.generation++
			e.published = false
			generation := e.generation
			m.mu.Unlock()
			data := m.slotBytes(i)
			m.mu.Lock()
			e.data = data
			m.mu.Unlock()
			return Reservation{lease: Lease{data: data[:n:n], mem: m, slot: i, generation: generation}, key: k}, nil
		}
		m.mu.Unlock()
		if expired {
			m.bypasses.Add(1)
			return Reservation{}, ErrNoSlot
		}
		if timer == nil {
			m.waits.Add(1)
			timer = time.NewTimer(2 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return Reservation{}, ctx.Err()
		case <-timer.C:
			// Notifications may coalesce. Recheck capacity under the lock once
			// more before reporting exhaustion; never start another wait.
			expired = true
		case <-m.available:
		}
	}
}
func (m *MemStore) slotBytes(i int) []byte {
	m.arenaMu.Lock()
	defer m.arenaMu.Unlock()
	s, off := i/m.segmentSlots, i%m.segmentSlots
	if m.segments[s] == nil {
		n := m.segmentSlots
		if left := m.slots - s*m.segmentSlots; left < n {
			n = left
		}
		m.segments[s] = make([]byte, int64(n)*m.slotSize)
	}
	start := int64(off) * m.slotSize
	return m.segments[s][start : start+m.slotSize : start+m.slotSize]
}
func (m *MemStore) publish(i int, k BlockKey, n int) {
	c, ok := m.chunkIndex.get(k.Chunk, 0)
	if !ok {
		c = m.freeChunk
		m.freeChunk = m.chunks[c].next
		m.chunks[c] = memChunk{key: k.Chunk}
		m.chunkIndex.put(k.Chunk, 0, c)
	}
	m.chunks[c].blocks++
	e := &m.entries[i]
	e.key, e.chunk, e.n, e.published = k, c, n, true
	m.blockIndex.put(uint64(c), k.Block, i)
	m.n.Add(1)
}
func (m *MemStore) retain(i int, generation uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := &m.entries[i]
	if e.generation != generation || e.refs == 0 {
		panic("store: clone of released lease")
	}
	e.refs++
	m.pins++
}
func (m *MemStore) release(i int, generation uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := &m.entries[i]
	if e.generation != generation || e.refs == 0 {
		panic("store: release of stale lease")
	}
	e.refs--
	m.pins--
	if e.refs == 0 {
		if e.published {
			m.link(i)
		} else {
			e.data = nil
			e.next = m.free
			m.free = i
		}
		select {
		case m.available <- struct{}{}:
		default:
		}
	}
	if m.closed && m.pins == 0 {
		m.clearArena()
	}
}
func (m *MemStore) link(i int) {
	e := &m.entries[i]
	e.prev = m.newest
	e.next = -1
	if m.newest >= 0 {
		m.entries[m.newest].next = i
	} else {
		m.oldest = i
	}
	m.newest = i
}
func (m *MemStore) unlink(i int) {
	e := &m.entries[i]
	if e.prev >= 0 {
		m.entries[e.prev].next = e.next
	} else {
		m.oldest = e.next
	}
	if e.next >= 0 {
		m.entries[e.next].prev = e.prev
	} else {
		m.newest = e.prev
	}
}
func (m *MemStore) remove(i int) {
	e := &m.entries[i]
	if e.refs == 0 {
		m.unlink(i)
	}
	m.blockIndex.remove(uint64(e.chunk), e.key.Block)
	c := &m.chunks[e.chunk]
	c.blocks--
	if c.blocks == 0 {
		m.chunkIndex.remove(c.key, 0)
		c.next = m.freeChunk
		m.freeChunk = e.chunk
	}
	e.published = false
	m.n.Add(-1)
	if e.refs == 0 {
		e.data = nil
		e.next = m.free
		m.free = i
	}
}

// Put copies into an unpublished slot outside the index lock.
func (m *MemStore) Put(k BlockKey, data []byte) error {
	_, err := m.putIfAdmitted(k, data, nil)
	return err
}
func (m *MemStore) putIfAdmitted(k BlockKey, data []byte, admit func(BlockKey) bool) (bool, error) {
	if len(data) == 0 {
		return false, ErrEmptyBlock
	}
	if int64(len(data)) > m.slotSize {
		return false, ErrBlockTooLarge
	}
	if l, ok, err := m.Acquire(k); err != nil {
		return false, err
	} else if ok {
		l.Release()
		return true, nil
	}
	r, err := m.reserve(context.Background(), k, int64(len(data)), admit)
	if errors.Is(err, ErrNoSlot) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	copy(r.Bytes(), data)
	l, err := r.Commit()
	l.Release()
	return err == nil, err
}

// Has checks visibility without borrowing the payload.
func (m *MemStore) Has(k BlockKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.find(k)
	return !m.closed && ok
}

// Delete hides a block; outstanding leases postpone physical reuse.
func (m *MemStore) Delete(k BlockKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.find(k)
	if ok {
		m.remove(i)
	}
	return ok
}

// Len reports visible blocks in O(1).
func (m *MemStore) Len() int { return int(m.n.Load()) }

// Slots reports capacity in blocks.
func (m *MemStore) Slots() int { return m.slots }

// Close rejects new access and hides entries. Existing leases remain valid.
// Node shutdown still joins all store users before closing.
func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		for i := range m.entries {
			if m.entries[i].published {
				m.remove(i)
			}
		}
	}
	if m.pins == 0 {
		m.clearArena()
	}
	return nil
}

func (m *MemStore) clearArena() {
	m.arenaMu.Lock()
	defer m.arenaMu.Unlock()
	for i := range m.segments {
		m.segments[i] = nil
	}
}
