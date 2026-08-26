package fetch

// Regression tests for issue #52: block geometry that cannot be represented
// in int64 must be rejected before the fetcher is invoked. Before the guard,
// a peer-wire index above MaxInt64 (converted with int64(req.Key.Block)) or
// whose start-offset multiplication overflowed produced a wrapped start that
// either recycled to 0 — silently fetching block 0's bytes for a nonsensical
// index — or went negative, which HTTPFetcher.Fetch reads as "no Range
// header": a one-block fetch degrading into a whole-object GET.

import (
	"context"
	"math"
	"strings"
	"testing"
)

// captureFetcher records the requested window instead of performing I/O, so a
// test can prove the overflow guard fires before any origin contact.
type captureFetcher struct {
	calls      int
	start, end int64
}

func (f *captureFetcher) Fetch(_ context.Context, _ string, start, end int64) (Range, error) {
	f.calls++
	f.start, f.end = start, end
	return Range{Data: make([]byte, end-start+1), Total: -1}, nil
}

// TestFetchBlockRejectsOverflowGeometry: negative indices (the result of
// int64() on a wire value above MaxInt64) and positive indices whose window
// overflows int64 must error, with size both known and unknown, and must
// never reach the fetcher. The boundary values are the independently computed
// MaxBlockIndex+1 for each block size (see chunk.TestMaxBlockIndexGolden).
func TestFetchBlockRejectsOverflowGeometry(t *testing.T) {
	cases := []struct {
		name             string
		blockSize, index int64
	}{
		{"uint64 2^63 converts to MinInt64", 4096, math.MinInt64},
		{"uint64 2^64-1 converts to -1", 4096, -1},
		{"MaxInt64: start multiplication wraps negative", 4096, math.MaxInt64},
		{"2^52 * 4096 wraps to 0", 4096, 1 << 52},
		{"first index past the int64 window, block size 4096", 4096, 2251799813685248},
		{"first index past the int64 window, block size 16", 16, 576460752303423488},
		{"non-positive block size", 0, 5},
	}
	for _, tc := range cases {
		for _, size := range []int64{4500, -1} { // size probed / unknown
			f := &captureFetcher{}
			_, err := FetchBlock(context.Background(), f, "http://origin/blob", tc.blockSize, tc.index, size)
			if err == nil {
				t.Errorf("%s (size %d): want an error, got none", tc.name, size)
			} else if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("%s (size %d): error %q does not identify the cause", tc.name, size, err)
			}
			if f.calls != 0 {
				t.Errorf("%s (size %d): fetcher invoked %d times; overflow geometry must not reach origin", tc.name, size, f.calls)
			}
		}
	}
}

// TestFetchBlockMaxInt64Window pins the valid side of the boundary: the
// largest representable index must reach the fetcher with the exact window
// [index*blockSize, MaxInt64] — no wrap, no spurious error. Golden values
// (arbitrary precision): 2251799813685247*4096 = 9223372036854771712, and
// +4095 = math.MaxInt64 exactly.
func TestFetchBlockMaxInt64Window(t *testing.T) {
	f := &captureFetcher{}
	if _, err := FetchBlock(context.Background(), f, "http://origin/blob", 4096, 2251799813685247, -1 /* size unknown: no tail clamp */); err != nil {
		t.Fatalf("largest representable block: %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("fetcher calls = %d, want 1", f.calls)
	}
	if f.start != 9223372036854771712 || f.end != math.MaxInt64 {
		t.Fatalf("window = [%d, %d], want [9223372036854771712, %d]", f.start, f.end, int64(math.MaxInt64))
	}

	// Control: ordinary tail-block clamping is unaffected by the guard.
	f2 := &captureFetcher{}
	if _, err := FetchBlock(context.Background(), f2, "http://origin/blob", 4096, 1, 4500); err != nil {
		t.Fatalf("tail block: %v", err)
	}
	if f2.calls != 1 || f2.start != 4096 || f2.end != 4499 {
		t.Fatalf("tail window = [%d, %d] (calls %d), want [4096, 4499] (calls 1)", f2.start, f2.end, f2.calls)
	}
}
