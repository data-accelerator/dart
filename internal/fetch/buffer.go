package fetch

import (
	"context"
	"errors"
	"io"
)

// ErrBodyTooLong reports a body exceeding the caller's bounded buffer.
var ErrBodyTooLong = errors.New("fetch: response body exceeds buffer")

// ReadBounded reads into dst and checks at most one additional byte for EOF.
// A clean short body returns its actual window; transport failures propagate.
// No payload buffer is allocated. The returned slice aliases dst.
func ReadBounded(r io.Reader, dst []byte) ([]byte, error) {
	n := 0
	for n < len(dst) {
		count, err := r.Read(dst[n:])
		n += count
		if err != nil {
			if err == io.EOF {
				return dst[:n], nil
			}
			return nil, err
		}
	}
	var extra [1]byte
	count, err := io.ReadFull(r, extra[:])
	if count != 0 {
		return nil, ErrBodyTooLong
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return dst, nil
}

// IntoFetcher optionally fills an exclusive caller-provided range buffer.
// Into fills are not shared: the engine coalesces immutable leases instead.
// Implementations must not retain or access dst after returning.
type IntoFetcher interface {
	FetchInto(context.Context, string, int64, int64, []byte) (Range, error)
}

// FetchInto uses direct ingress when supported, otherwise copies a Fetcher's
// read-only result into dst. Both paths retain the Fetcher's validation rules.
func FetchInto(ctx context.Context, f Fetcher, url string, start, end int64, dst []byte) (Range, error) {
	if into, ok := f.(IntoFetcher); ok {
		return into.FetchInto(ctx, url, start, end, dst)
	}
	r, err := f.Fetch(ctx, url, start, end)
	if err != nil {
		return Range{}, err
	}
	if len(r.Data) > len(dst) {
		return Range{}, ErrBodyTooLong
	}
	copy(dst, r.Data)
	r.Data = dst[:len(r.Data)]
	return r, nil
}

// FetchInto delegates exclusive buffer fills to the inner fetcher. Callers
// coordinate shared leases above this layer. MaxFlight still bounds the call;
// work runs inline and has exited before the buffer can be released.
func (c *Coalescing) FetchInto(ctx context.Context, url string, start, end int64, dst []byte) (Range, error) {
	ctx, cancel := context.WithTimeout(ctx, c.maxFlight())
	defer cancel()
	return FetchInto(ctx, c.F, url, start, end, dst)
}

// FetchInto fetches one inclusive range directly into dst, validating framing,
// length and offsets exactly as Fetch does. dst must match the requested range.
func (f *HTTPFetcher) FetchInto(ctx context.Context, url string, start, end int64, dst []byte) (Range, error) {
	if start < 0 || end < start || end-start == int64(^uint64(0)>>1) || end-start+1 != int64(len(dst)) {
		return Range{}, errors.New("fetch: buffer does not match requested range")
	}
	return f.fetch(ctx, url, start, end, dst)
}
