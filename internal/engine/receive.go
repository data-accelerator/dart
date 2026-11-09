package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

// ErrReceiveBudget means temporary receive capacity remained full for the
// bounded admission wait. Cache hits and reserved arena fills do not use it.
var ErrReceiveBudget = errors.New("engine: temporary receive budget exhausted")

var errSpeculativeReceive = fmt.Errorf("%w: speculative fill skipped", ErrReceiveBudget)

const receiveWait = 2 * time.Millisecond

type speculativeKey struct{}

func speculativeContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, speculativeKey{}, true)
}
func isSpeculative(ctx context.Context) bool {
	v, _ := ctx.Value(speculativeKey{}).(bool)
	return v
}

// receiveBudget accounts for live temporary payloads, including completed
// results held by shared callers or slow writers. Admission cannot indefinitely
// hold up a peer whose own request already owns receive capacity at this node.
type receiveBudget struct {
	mu                         sync.Mutex
	maxBytes, bytes, blockSize int64
	maxLoads, loads            int
	changed                    chan struct{}
	mx                         *Metrics
}

func (b *receiveBudget) acquire(ctx context.Context, n int64, speculative bool) error {
	var timer *time.Timer
	var deadline time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	expired := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		maxBytes, maxLoads := b.maxBytes, b.maxLoads
		if speculative {
			// A completed future must not consume the last capacity needed by
			// the earlier block that its handler is waiting to write.
			maxBytes -= b.blockSize
			maxLoads--
		}
		if b.loads < maxLoads && n <= maxBytes-b.bytes {
			b.loads++
			b.bytes += n
			b.recordUsage()
			b.mu.Unlock()
			return nil
		}
		if speculative {
			b.mu.Unlock()
			return errSpeculativeReceive
		}
		if expired || (!deadline.IsZero() && !time.Now().Before(deadline)) {
			b.mu.Unlock()
			return ErrReceiveBudget
		}
		if b.changed == nil {
			b.changed = make(chan struct{})
		}
		changed := b.changed
		b.mu.Unlock()
		if timer == nil {
			deadline = time.Now().Add(receiveWait)
			timer = time.NewTimer(time.Until(deadline))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-timer.C:
			expired = true // recheck once, without restarting the wait
		}
	}
}

func (b *receiveBudget) release(n int64) {
	b.mu.Lock()
	b.loads--
	b.bytes -= n
	b.recordUsage()
	if b.changed != nil {
		close(b.changed)
		b.changed = nil
	}
	b.mu.Unlock()
}

// Called under mu so concurrent final releases cannot publish a stale gauge.
func (b *receiveBudget) recordUsage() {
	if b.mx != nil {
		b.mx.receiveLoads.Set(float64(b.loads))
		b.mx.receiveBytes.Set(float64(b.bytes))
	}
}

// blockBuffer exclusively owns either an unpublished arena slot or an admitted
// heap payload. finishBuffer moves that ownership to its immutable result.
type blockBuffer struct {
	reservation store.Reservation
	temporary   store.Lease
	cache       bool
}

func (b blockBuffer) bytes() []byte {
	if data := b.reservation.Bytes(); data != nil {
		return data
	}
	return b.temporary.Bytes()
}
func (b *blockBuffer) abort() {
	b.reservation.Abort()
	b.temporary.Release()
}

func (e *Engine) receiveBuffer(ctx context.Context, key store.BlockKey, n int64, speculative bool) (blockBuffer, error) {
	cache := true
	if s, ok := e.store.(store.Reserver); ok {
		r, err := s.Reserve(ctx, key, n, e.classOf(key.Chunk))
		if err == nil {
			return blockBuffer{reservation: r}, nil
		}
		if errors.Is(err, store.ErrNoSlot) || errors.Is(err, store.ErrBlockTooLarge) {
			cache = false
		} else if !errors.Is(err, store.ErrNoReservation) {
			return blockBuffer{}, err
		}
	}
	if err := e.receive.acquire(ctx, n, speculative); err != nil {
		if errors.Is(err, ErrReceiveBudget) && e.mx != nil {
			e.mx.receiveRejected.Inc()
		}
		return blockBuffer{}, err
	}
	e.mx.recordTemporary(n)
	l := store.HeapLeaseWithRelease(make([]byte, n), func() { e.receive.release(n) })
	return blockBuffer{temporary: l, cache: cache}, nil
}

func (e *Engine) finishBuffer(b *blockBuffer, key store.BlockKey, publish bool) (store.Lease, error) {
	if b.reservation.Bytes() != nil {
		if publish {
			return b.reservation.Commit()
		}
		return b.reservation.Detach(), nil
	}
	if publish && b.cache {
		e.putBlock(key, b.temporary.Bytes(), key.Chunk)
	}
	l := b.temporary
	b.temporary = store.Lease{}
	return l, nil
}
