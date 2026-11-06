package store

import (
	"context"
	"errors"
)

// Lease pins immutable bytes until Release. Move by assignment; call Clone to
// create another owner. Do not mutate Bytes or use a released lease. Different
// owners are safe concurrently; operations on one variable must be serialized.
type Lease struct {
	data       []byte
	mem        *MemStore
	slot       int
	generation uint64
}

// Bytes returns a read-only view valid until this owner releases its lease.
func (l Lease) Bytes() []byte { return l.data }

// HeapLease takes ownership of data as immutable bytes for compatibility or
// pressure fallback. Release drops its reference for the Go collector.
func HeapLease(data []byte) Lease { return Lease{data: data} }

// Clone creates an independent owner without copying the payload.
func (l Lease) Clone() Lease {
	if l.mem != nil {
		l.mem.retain(l.slot, l.generation)
	}
	return l
}

// Release ends ownership. Releasing the same variable again is safe.
func (l *Lease) Release() {
	if l.mem != nil {
		l.mem.release(l.slot, l.generation)
	}
	*l = Lease{}
}

// LeaseStore optionally exposes immutable cache hits without copying.
type LeaseStore interface {
	Acquire(BlockKey) (Lease, bool, error)
}

// Acquire borrows from a LeaseStore, or wraps a traditional Store's owned copy.
func Acquire(s Store, k BlockKey) (Lease, bool, error) {
	if b, ok := s.(LeaseStore); ok {
		return b.Acquire(k)
	}
	data, ok, err := s.Get(k)
	return HeapLease(data), ok, err
}

// ErrNoSlot means admission declined or no slot became reusable in time.
var ErrNoSlot = errors.New("store: no reusable memory slot")

// ErrNoReservation means the backend uses the buffered compatibility path.
var ErrNoReservation = errors.New("store: backend does not reserve memory slots")

// ErrClosed reports access to a closed memory store.
var ErrClosed = errors.New("store: memory store closed")

// Reserver optionally receives complete blocks into unpublished cache storage.
// Reserve waits only briefly under pressure and honors ctx.
type Reserver interface {
	Reserve(context.Context, BlockKey, int64, Class) (Reservation, error)
}

// Reservation exclusively owns an unpublished slot. Bytes is writable until
// Commit or Abort. Move by assignment; do not copy/share. Each reservation must
// have exactly one Commit or Abort.
type Reservation struct {
	lease Lease
	key   BlockKey
	tier  *Tiered
	class Class
}

// Bytes returns the writable requested range, without the unused slot tail.
func (r Reservation) Bytes() []byte { return r.lease.data }

// Abort discards an unpublished fill and releases its slot.
func (r *Reservation) Abort() { r.lease.Release(); *r = Reservation{} }

// Commit publishes a fully validated block and transfers ownership to a lease.
// Stop writing before Commit. A concurrent publication keeps the original
// entry; this result remains valid as an uncached lease.
func (r *Reservation) Commit() (Lease, error) {
	if r.lease.mem == nil {
		return Lease{}, ErrClosed
	}
	if r.tier != nil {
		return r.tier.commit(r)
	}
	return r.commit()
}

func (r *Reservation) commit() (Lease, error) {
	m := r.lease.mem
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		r.Abort()
		return Lease{}, ErrClosed
	}
	if _, ok := m.find(r.key); !ok {
		m.publish(r.lease.slot, r.key, len(r.lease.data))
	}
	m.mu.Unlock()
	l := r.lease
	*r = Reservation{}
	return l, nil
}
