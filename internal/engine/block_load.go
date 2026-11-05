package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/data-accelerator/dart/internal/store"
)

type blockSource uint8

const (
	blockFromCache blockSource = iota
	blockFromPeer
	blockFromOrigin
)

type blockRead struct {
	data   []byte
	source blockSource
}

// Relay depth must stay in the flight identity: joining a shallower request
// when a skewed tree loops back would turn the hop bound into a wait cycle.
type blockLoadKey struct {
	block store.BlockKey
	hop   int
}

type blockLoad struct {
	done     chan struct{}
	url      string
	read     blockRead
	err      error
	canceled bool
	shared   bool // protected by blockLoads.mu while the flight is published
}

// blockLoads shares a buffered miss through validation and cache publication.
// The leader runs in its caller's goroutine, keeping all store access within
// that handler's lifetime. Waiters own no goroutine and can cancel independently.
// A canceled leader releases the flight; a surviving waiter retries on its own
// context and credential, rather than inheriting the leader's cancellation.
type blockLoads struct {
	mu sync.Mutex
	m  map[blockLoadKey]*blockLoad
}

var errBlockLoadAborted = errors.New("engine: block load aborted")

func (g *blockLoads) do(ctx context.Context, key blockLoadKey, url string, load func() (blockRead, error)) (blockRead, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return blockRead{}, false, err
		}
		g.mu.Lock()
		if c := g.m[key]; c != nil {
			c.shared = true
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return blockRead{}, false, ctx.Err()
			case <-c.done:
			}
			// Successful immutable bytes are shared across URL aliases, just
			// like cache hits. A failed credential/endpoint is not evidence
			// against a different caller's URL; retry with that caller's own.
			if c.err != nil && (c.canceled || c.url != url) {
				continue
			}
			return c.read, true, c.err
		}
		if g.m == nil {
			g.m = make(map[blockLoadKey]*blockLoad)
		}
		c := &blockLoad{done: make(chan struct{}), url: url, err: errBlockLoadAborted}
		g.m[key] = c
		g.mu.Unlock()
		return g.lead(ctx, key, c, load)
	}
}

func (g *blockLoads) lead(ctx context.Context, key blockLoadKey, c *blockLoad, load func() (blockRead, error)) (blockRead, bool, error) {
	defer func() {
		g.mu.Lock()
		delete(g.m, key)
		shared := c.shared
		g.mu.Unlock()
		// The leader returns caller-owned bytes. Take the waiters' snapshot
		// before letting it return, so it cannot race their later copies.
		// Admission has ended, and unrelated keys are not blocked by copying.
		if shared && c.err == nil {
			c.read.data = append([]byte(nil), c.read.data...)
		}
		// Publish only after the caller's load has validated and attempted
		// cache insertion. Deleting before waking waiters never retains a
		// completed error; a new leader rechecks cache before doing I/O.
		close(c.done)
	}()
	c.read, c.err = load()
	c.canceled = ctx.Err() != nil
	return c.read, false, c.err
}
