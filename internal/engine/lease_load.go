package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/data-accelerator/dart/internal/store"
)

type leasedRead struct {
	lease     store.Lease
	source    blockSource
	wireBytes int
}
type leaseCall struct {
	key               blockLoadKey
	done              chan struct{}
	url               string
	refs              int
	finished, claimed bool
	read              leasedRead
	err               error
	cancel            context.CancelFunc
}

// leaseLoads owns one result reference until every admitted consumer detaches.
// Block flights run inline. Origin flights have a shared cancellation owner;
// their final departing consumer cancels and joins the backend before returning.
// No lease or reservation survives without a serving caller owning that join.
type leaseLoads struct {
	mu sync.Mutex
	m  map[blockLoadKey]*leaseCall
}

func (g *leaseLoads) do(ctx context.Context, key blockLoadKey, url string, sharedWorker bool, load func(context.Context) (leasedRead, error)) (leasedRead, error) {
	for {
		if err := ctx.Err(); err != nil {
			return leasedRead{}, err
		}
		g.mu.Lock()
		if g.m == nil {
			g.m = make(map[blockLoadKey]*leaseCall)
		}
		c := g.m[key]
		lead := c == nil
		var work context.Context
		if lead {
			c = &leaseCall{done: make(chan struct{}), url: url, key: key}
			g.m[key] = c
			if sharedWorker {
				work, c.cancel = context.WithCancel(context.Background())
			} else {
				work = ctx
			}
		}
		c.refs++
		g.mu.Unlock()
		if lead {
			run := func() {
				// Publish failure even if an inline backend panics, releasing waiters.
				finished := false
				defer func() {
					if !finished {
						g.finish(key, c, leasedRead{}, errBlockLoadAborted)
					}
				}()
				r, err := load(work)
				g.finish(key, c, r, err)
				finished = true
			}
			if sharedWorker {
				go run()
			} else {
				run()
			}
		}
		select {
		case <-ctx.Done():
			g.drop(c)
			return leasedRead{}, ctx.Err()
		case <-c.done:
		}
		if err := ctx.Err(); err != nil {
			g.drop(c)
			return leasedRead{}, err
		}
		g.mu.Lock()
		r, err := c.read, c.err
		if err == nil {
			r.lease = r.lease.Clone()
			if c.claimed {
				r.wireBytes = 0
			} else {
				c.claimed = true
			}
		}
		retry := err != nil && !lead && (c.url != url || (!sharedWorker && contextError(err)))
		g.mu.Unlock()
		g.drop(c)
		if retry {
			continue
		}
		return r, err
	}
}

func contextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
func (g *leaseLoads) finish(key blockLoadKey, c *leaseCall, r leasedRead, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m[key] == c {
		delete(g.m, key)
	}
	c.read, c.err, c.finished = r, err, true
	if c.refs == 0 {
		c.read.lease.Release()
	}
	close(c.done)
}
func (g *leaseLoads) drop(c *leaseCall) {
	g.mu.Lock()
	c.refs--
	last := c.refs == 0
	finished := c.finished
	// Stop admission before canceling the last owner's worker. A later caller
	// must lead a fresh flight while this abandoned backend is still unwinding.
	if last && g.m[c.key] == c {
		delete(g.m, c.key)
	}
	if last && finished {
		c.read.lease.Release()
	}
	g.mu.Unlock()
	if last && c.cancel != nil {
		c.cancel()
		if !finished {
			<-c.done
		}
	}
}
