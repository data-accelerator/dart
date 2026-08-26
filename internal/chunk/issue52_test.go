package chunk

// Regression test for issue #52: the peer wire carries block indices as an
// unrestricted uint64, and the relay boundary declines anything above
// MaxBlockIndex. Pin the bound against independently computed (arbitrary
// precision) values so it neither re-admits overflow geometry nor rejects
// legitimate tail blocks.

import (
	"math"
	"testing"
)

// TestMaxBlockIndexGolden pins Config.MaxBlockIndex: the largest block index
// whose whole window [i*BlockSize, i*BlockSize+BlockSize-1] fits in int64.
// Golden values were computed with arbitrary-precision arithmetic (Python):
//
//	bs=1      9223372036854775807*1       + 0    = MaxInt64
//	bs=3      3074457345618258601*3       = ...805, + 2    = ...805 <= MaxInt64; next start ...806, end ...808 > MaxInt64
//	bs=16     576460752303423487*16       = ...792, + 15   = MaxInt64 exactly
//	bs=4096   2251799813685247*4096       = ...712, + 4095 = MaxInt64 exactly
//	bs=4MiB   2199023255551*4194304       = ...504, + 4095*1024-1 = MaxInt64 exactly
//
// In every case the next index's window end exceeds MaxInt64.
func TestMaxBlockIndexGolden(t *testing.T) {
	cases := []struct {
		blockSize int64
		want      int64
	}{
		{1, 9223372036854775807},
		{3, 3074457345618258601},
		{16, 576460752303423487},
		{4096, 2251799813685247},
		{4 * MiB, 2199023255551},
	}
	for _, tc := range cases {
		c := Config{ChunkSize: tc.blockSize * 2, BlockSize: tc.blockSize}
		if got := c.MaxBlockIndex(); got != tc.want {
			t.Errorf("BlockSize %d: MaxBlockIndex = %d, want %d", tc.blockSize, got, tc.want)
		}
		// The pinned index's window must end within int64 (computed in the
		// subtraction form so the check itself cannot wrap).
		if start := tc.want * tc.blockSize; start > math.MaxInt64-tc.blockSize+1 {
			t.Errorf("BlockSize %d: window of the pinned MaxBlockIndex does not fit int64", tc.blockSize)
		}
		// The next index must NOT fit: its start alone exceeds the largest
		// start whose window end stays in range. (When the bound is MaxInt64
		// the next index is not even representable — trivially "does not
		// fit"; computing it would wrap this very check.)
		if tc.want != math.MaxInt64 {
			if next := tc.want + 1; next <= (math.MaxInt64-tc.blockSize+1)/tc.blockSize {
				t.Errorf("BlockSize %d: index %d above the pinned bound still fits — the bound is too low", tc.blockSize, next)
			}
		}
	}
}
