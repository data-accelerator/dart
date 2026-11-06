package engine

import (
	"context"
	"errors"

	"github.com/data-accelerator/dart/internal/store"
)

type blockSource uint8

const (
	blockFromCache blockSource = iota
	blockFromPeer
	blockFromOrigin
)

type blockRead struct {
	lease     store.Lease
	data      []byte
	source    blockSource
	wireBytes int // zero when an underlying origin flight supplied shared bytes
}

// Relay depth must stay in the flight identity: joining a shallower request
// when a skewed tree loops back would turn the hop bound into a wait cycle.
type blockLoadKey struct {
	block store.BlockKey
	hop   int
}

// blockLoads is the owned-copy adapter over the same lifetime algorithm used
// by borrowed loads. Its callers receive independent mutable copies; the
// underlying result stays pinned until each admitted consumer has copied it.
type blockLoads struct{ leaseLoads }

var errBlockLoadAborted = errors.New("engine: block load aborted")

func (g *blockLoads) do(ctx context.Context, key blockLoadKey, url string, load func() (blockRead, error)) (blockRead, bool, error) {
	r, err := g.leaseLoads.do(ctx, key, url, false, func(context.Context) (leasedRead, error) {
		v, err := load()
		l := v.lease
		if l.Bytes() == nil {
			l = store.HeapLease(v.data)
		}
		return leasedRead{lease: l, source: v.source, wireBytes: v.wireBytes}, err
	})
	if err != nil {
		return blockRead{}, r.shared, err
	}
	defer r.lease.Release()
	return blockRead{data: append([]byte(nil), r.lease.Bytes()...), source: r.source, wireBytes: r.wireBytes}, r.shared, nil
}
