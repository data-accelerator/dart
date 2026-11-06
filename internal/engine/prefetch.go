package engine

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/store"
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
	index         int64
	active, ready bool
	lease         store.Lease
	err           error
	reserved      int64
}
type blockJob struct{ index int64 }
type blockResult struct {
	index int64
	lease store.Lease
	err   error
}

// serveBlocks reuses a fixed future ring and a bounded set of request-owned
// workers. It never allocates a future, channel or goroutine per covering block.
// All workers join before pins and speculative reservations are released.
func (e *Engine) serveBlocks(ctx context.Context, w io.Writer, url string, size, start, end int64) error {
	objectID, _ := chunk.ObjectID(url)
	first, last := e.cfg.BlockIndex(start), e.cfg.BlockIndex(end)
	if first == last || e.prefetchWindow == 1 {
		for index := first; index <= last; index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			l, err := e.borrowBlock(ctx, url, objectID, size, index, 0)
			if err != nil {
				return err
			}
			err = func() error {
				defer l.Release()
				return e.writeRangeBlock(w, l.Bytes(), index, size, start, end)
			}()
			if err != nil {
				return err
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	window := e.prefetchWindow
	if int64(window) > last-first+1 {
		window = int(last - first + 1)
	}
	var pending [64]blockFuture
	jobs := make(chan blockJob, window)
	results := make(chan blockResult, window)
	var workers sync.WaitGroup
	for worker := 0; worker < window; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-jobs:
					l, err := e.borrowBlock(ctx, url, objectID, size, job.index, 0)
					results <- blockResult{index: job.index, lease: l, err: err}
				}
			}
		}()
	}
	defer func() {
		cancel()
		workers.Wait()
		for {
			select {
			case r := <-results:
				r.lease.Release()
			default:
				goto drained
			}
		}
	drained:
		for i := range pending {
			f := &pending[i]
			f.lease.Release()
			if f.active && f.reserved != 0 {
				e.prefetch.release(f.reserved)
			}
		}
	}()
	future := func(index int64) *blockFuture { return &pending[index%int64(window)] }
	launch := func(index, reserved int64) {
		*future(index) = blockFuture{index: index, active: true, reserved: reserved}
		jobs <- blockJob{index: index}
	}
	for index := first; index <= last; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		f := future(index)
		if !f.active {
			launch(index, 0)
		} // demand bypasses speculative admission
		for ahead := int64(1); ahead < int64(window) && ahead <= last-index; ahead++ {
			next := index + ahead
			if future(next).active {
				continue
			}
			n := e.blockLen(size, next)
			if !e.prefetch.acquire(n) {
				break
			}
			launch(next, n)
		}
		for !f.ready {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r := <-results:
				done := future(r.index)
				done.lease, done.err, done.ready = r.lease, r.err, true
			}
		}
		if f.err != nil {
			return f.err
		}
		if err := e.writeRangeBlock(w, f.lease.Bytes(), index, size, start, end); err != nil {
			return err
		}
		f.lease.Release()
		if f.reserved != 0 {
			e.prefetch.release(f.reserved)
		}
		*f = blockFuture{}
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
