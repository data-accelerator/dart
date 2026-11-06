package engine

import (
	"context"
	"sync"
	"time"

	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/store"
)

type leasedRead struct {
	shared    bool
	lease     store.Lease
	source    blockSource
	wireBytes int
}
type leaseCall struct {
	stale             time.Time
	leader            context.Context
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
	maxFlight time.Duration // immutable after Engine construction; zero in unbounded unit adapters
	mu        sync.Mutex
	m         map[blockLoadKey]*leaseCall
}

const (
	leaseReady = iota
	leaseCanceled
	leaseRetry
)

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
		var cancelOld context.CancelFunc
		if c != nil && ((c.leader != nil && c.leader.Err() != nil) || (!c.stale.IsZero() && !time.Now().Before(c.stale))) {
			// The original handler retains its cleanup ownership. A canceled inline
			// leader must not retain admission while joining that cleanup.
			delete(g.m, key)
			cancelOld = c.cancel
			c = nil
		}
		lead := c == nil
		var work context.Context
		if lead {
			c = &leaseCall{done: make(chan struct{}), url: url, key: key}
			g.m[key] = c
			var deadline time.Time
			if g.maxFlight > 0 {
				deadline = time.Now().Add(g.maxFlight)
				c.stale = deadline.Add(fetch.FlightGrace(g.maxFlight))
			}
			if sharedWorker {
				if g.maxFlight > 0 {
					work, c.cancel = context.WithDeadline(context.Background(), deadline)
				} else {
					work, c.cancel = context.WithCancel(context.Background())
				}
			} else {
				work = ctx
				c.leader = ctx
			}
		}
		c.refs++
		g.mu.Unlock()
		if cancelOld != nil {
			cancelOld()
		}
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
		var abandoned <-chan struct{}
		if !lead && c.leader != nil {
			abandoned = c.leader.Done()
		}
		// Inline leaders keep cleanup ownership synchronously. Other consumers
		// may stop waiting at the admission expiry and join a fresh replacement.
		outcome := func() int {
			var expired <-chan time.Time
			if (sharedWorker || !lead) && !c.stale.IsZero() {
				remaining := time.Until(c.stale)
				if remaining <= 0 {
					return leaseRetry
				}
				timer := time.NewTimer(remaining)
				defer timer.Stop()
				expired = timer.C
			}
			select {
			case <-ctx.Done():
				return leaseCanceled
			case <-abandoned:
				return leaseRetry
			case <-expired:
				return leaseRetry
			case <-c.done:
				return leaseReady
			}
		}()
		if outcome != leaseReady {
			g.drop(c)
			if outcome == leaseCanceled {
				return leasedRead{shared: !lead}, ctx.Err()
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			g.drop(c)
			return leasedRead{shared: !lead}, err
		}
		g.mu.Lock()
		r, err := c.read, c.err
		r.shared = !lead
		if err == nil {
			r.lease = r.lease.Clone()
			if c.claimed {
				r.wireBytes = 0
			} else {
				c.claimed = true
			}
		} else {
			// The flight retains any failed result until consumers detach;
			// an error does not transfer an un-cloned owner to this caller.
			r.lease = store.Lease{}
		}
		// Only cancellation of the serving leader permits same-URL takeover.
		// A backend timeout with a live leader is a shared failure.
		retry := err != nil && !lead && (c.url != url || (!sharedWorker && c.leader.Err() != nil))
		g.mu.Unlock()
		g.drop(c)
		if retry {
			continue
		}
		return r, err
	}
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
