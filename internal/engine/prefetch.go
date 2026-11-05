package engine

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/data-accelerator/dart/internal/chunk"
)

// Reservations cover speculative payloads until consumed, including completed
// results. Admission never blocks demand, and no reservation spans a handler exit.
type prefetchBudget struct {
	mu              sync.Mutex
	maxBytes, bytes int64
	maxLoads, loads int
}

func (b *prefetchBudget) acquire(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loads >= b.maxLoads || n > b.maxBytes-b.bytes {
		return false
	}
	b.loads++
	b.bytes += n
	return true
}

func (b *prefetchBudget) release(n int64) {
	b.mu.Lock()
	b.loads--
	b.bytes -= n
	b.mu.Unlock()
}

type blockFuture struct {
	done     chan struct{}
	data     []byte
	err      error
	reserved int64
}

// Workers belong to this Serve call. Cancel and join before releasing results
// or returning, preserving the handler/store lifetime even on a write failure.
func (e *Engine) serveBlocks(ctx context.Context, w io.Writer, url string, size, start, end int64) error {
	objectID, _ := chunk.ObjectID(url)
	first, last := e.cfg.BlockIndex(start), e.cfg.BlockIndex(end)
	// Keep single-block reads and explicit serial mode synchronous: no worker,
	// derived context or result allocation on the common small-Range path.
	if first == last || e.prefetchWindow == 1 {
		for index := first; index <= last; index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			data, err := e.block(ctx, url, objectID, size, index, 0)
			if err != nil {
				return err
			}
			if err := e.writeRangeBlock(w, data, index, size, start, end); err != nil {
				return err
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	pending := make(map[int64]*blockFuture)
	defer func() {
		cancel()
		workers.Wait()
		for _, f := range pending {
			if f.reserved != 0 {
				e.prefetch.release(f.reserved)
			}
		}
	}()
	launch := func(index, reserved int64) *blockFuture {
		f := &blockFuture{done: make(chan struct{}), reserved: reserved}
		pending[index] = f
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer close(f.done)
			f.data, f.err = e.block(ctx, url, objectID, size, index, 0)
		}()
		return f
	}
	for index := first; index <= last; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		f := pending[index]
		if f == nil {
			f = launch(index, 0) // demand always progresses without speculative capacity
		}
		for ahead := int64(1); ahead < int64(e.prefetchWindow) && ahead <= last-index; ahead++ {
			next := index + ahead
			if pending[next] != nil {
				continue
			}
			n := e.blockLen(size, next)
			if !e.prefetch.acquire(n) {
				break // retain priority for the nearest unrequested block
			}
			launch(next, n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.done:
		}
		if f.err != nil {
			return f.err // an ahead failure never discards valid preceding blocks
		}
		if err := e.writeRangeBlock(w, f.data, index, size, start, end); err != nil {
			return err
		}
		if f.reserved != 0 {
			e.prefetch.release(f.reserved)
		}
		delete(pending, index)
	}
	return nil
}

func (e *Engine) writeRangeBlock(w io.Writer, data []byte, index, size, start, end int64) error {
	blockStart := e.cfg.BlockStart(index)
	lo, hi := int64(0), e.blockLen(size, index)
	if start > blockStart {
		lo = start - blockStart
	}
	if end-blockStart+1 < hi {
		hi = end - blockStart + 1
	}
	if hi > int64(len(data)) {
		return fmt.Errorf("engine: short block %d: have %d bytes, need %d", index, len(data), hi)
	}
	n, err := w.Write(data[lo:hi])
	e.mx.recordClientBytes(n)
	if err != nil {
		return err
	}
	if int64(n) != hi-lo {
		return io.ErrShortWrite
	}
	return nil
}
