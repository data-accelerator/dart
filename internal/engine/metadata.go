package engine

import (
	"context"
	"errors"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
)

type metadataFetcher struct{ e *Engine }

func (f metadataFetcher) Fetch(ctx context.Context, url string, _, _ int64) (fetch.Range, error) {
	e := f.e
	if !e.p2p {
		return fetch.Range{}, errNoRoot
	}
	view := e.cluster.Current()
	if view == nil {
		return fetch.Range{}, errNoRoot
	}
	oid, _ := chunk.ObjectID(url)
	roots := hashring.Rank(e.fileKey(oid), view.Roots())
	var last error = errNoRoot
	for i, n := range roots {
		if i >= maxHop {
			break
		}
		if err := ctx.Err(); err != nil {
			return fetch.Range{}, err
		}
		m, ok := view.Get(n.ID)
		if !ok || m.Addr == "" {
			continue
		}
		meta, err := e.peer.Probe(ctx, m.Addr, url)
		if err == nil {
			return fetch.Range{Total: meta.Size, RangeIgnored: meta.RangeUnsupported}, nil
		}
		if errors.Is(err, peer.ErrUpstreamRefused) {
			return fetch.Range{}, err
		}
		last = err
	}
	return fetch.Range{}, last
}

// ProbeSource provides terminal metadata service on root-eligible nodes.
// A non-root node returns nil, so wiring it cannot enable origin egress.
func (e *Engine) ProbeSource() func(context.Context, string) (peer.Metadata, error) {
	if !e.role.CanRoot() {
		return nil
	}
	return func(ctx context.Context, url string) (peer.Metadata, error) {
		size, err := e.Size(ctx, url)
		return peer.Metadata{Size: size, RangeUnsupported: e.RangeUnsupported(url)}, err
	}
}
