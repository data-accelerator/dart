package chunk

import (
	"strings"
	"testing"
)

// digest is a canonical OCI-style sha256 blob digest (64 hex chars).
var digest = "sha256:" + strings.Repeat("ab", 32)

// TestChunkKeyGolden pins the ChunkKey serialization against the independent
// Python reference contrib/golden/golden_reference.py (CI diffs it against the
// live Go code under DART_GOLDEN_REF=1). ChunkKey selects owners, so it is part of the wire protocol;
// a change here reshuffles placement and must be treated as breaking.
func TestChunkKeyGolden(t *testing.T) {
	cases := []struct {
		ns   string
		oid  string
		ci   int64
		want uint64
	}{
		{"dart", digest, 0, 284481562108898346},
		{"dart", digest, 1, 5100546052700164472},
		{"dart", "https://registry.example.com/v2/lib/nginx/blobs", 0, 4130664464540416952},
		{"ns2", digest, 0, 7473071683502782500},
	}
	for _, c := range cases {
		if got := ChunkKey(c.ns, c.oid, c.ci); got != c.want {
			t.Errorf("ChunkKey(%q, oid, %d) = %d (%#016x), want %d", c.ns, c.ci, got, got, c.want)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Errorf("default config invalid: %v", err)
	}
	bad := []Config{
		{ChunkSize: 0, BlockSize: 4},
		{ChunkSize: 16, BlockSize: 0},
		{ChunkSize: 10, BlockSize: 4}, // not a multiple
		{ChunkSize: -16, BlockSize: 4},
	}
	for _, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("expected invalid config %+v to fail Validate", c)
		}
	}
	if n := DefaultConfig().BlocksPerChunk(); n != 64 {
		t.Errorf("BlocksPerChunk = %d, want 64", n)
	}
}

func TestGridMath(t *testing.T) {
	c := Config{ChunkSize: 16, BlockSize: 4} // 4 blocks/chunk
	tests := []struct {
		offset                     int64
		chunkIdx, blockIdx, bStart int64
	}{
		{0, 0, 0, 0},
		{3, 0, 0, 0},
		{4, 0, 1, 4},
		{15, 0, 3, 12},
		{16, 1, 4, 16},
		{31, 1, 7, 28},
		{32, 2, 8, 32},
	}
	for _, tt := range tests {
		if got := c.ChunkIndex(tt.offset); got != tt.chunkIdx {
			t.Errorf("ChunkIndex(%d) = %d, want %d", tt.offset, got, tt.chunkIdx)
		}
		if got := c.BlockIndex(tt.offset); got != tt.blockIdx {
			t.Errorf("BlockIndex(%d) = %d, want %d", tt.offset, got, tt.blockIdx)
		}
		if got := c.ChunkOfBlock(tt.blockIdx); got != tt.chunkIdx {
			t.Errorf("ChunkOfBlock(%d) = %d, want %d", tt.blockIdx, got, tt.chunkIdx)
		}
		if got := c.BlockStart(tt.blockIdx); got != tt.bStart {
			t.Errorf("BlockStart(%d) = %d, want %d", tt.blockIdx, got, tt.bStart)
		}
	}
}

func TestSegmentsSingleBlock(t *testing.T) {
	c := DefaultConfig()
	segs := c.Segments(0, 4095) // 4 KiB read within block 0
	if len(segs) != 1 {
		t.Fatalf("len = %d, want 1", len(segs))
	}
	want := Segment{ChunkIndex: 0, BlockIndex: 0, From: 0, To: 4095}
	if segs[0] != want {
		t.Errorf("seg = %+v, want %+v", segs[0], want)
	}
}

func TestSegmentsCrossBlockAndChunk(t *testing.T) {
	c := Config{ChunkSize: 16, BlockSize: 4} // blocks 0..3 => chunk0, 4..7 => chunk1
	// Range 14..18 crosses block 3 (chunk0) into block 4 (chunk1).
	segs := c.Segments(14, 18)
	want := []Segment{
		{ChunkIndex: 0, BlockIndex: 3, From: 14, To: 15},
		{ChunkIndex: 1, BlockIndex: 4, From: 16, To: 18},
	}
	if len(segs) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(segs), len(want), segs)
	}
	for i := range want {
		if segs[i] != want[i] {
			t.Errorf("seg[%d] = %+v, want %+v", i, segs[i], want[i])
		}
	}
}

func TestSegmentsInvalidRange(t *testing.T) {
	c := DefaultConfig()
	if c.Segments(-1, 10) != nil {
		t.Error("negative start should return nil")
	}
	if c.Segments(10, 5) != nil {
		t.Error("end<start should return nil")
	}
}

// TestSegmentsCoverProperty: over many ranges, the segments must tile [start,end]
// exactly — contiguous, no gaps/overlaps, each within its own block, correct
// chunk mapping.
func TestSegmentsCoverProperty(t *testing.T) {
	c := Config{ChunkSize: 16, BlockSize: 4}
	for start := int64(0); start <= 40; start++ {
		for end := start; end <= 40; end++ {
			segs := c.Segments(start, end)
			if len(segs) == 0 {
				t.Fatalf("empty segments for [%d,%d]", start, end)
			}
			if segs[0].From != start {
				t.Fatalf("[%d,%d] first.From=%d", start, end, segs[0].From)
			}
			if segs[len(segs)-1].To != end {
				t.Fatalf("[%d,%d] last.To=%d", start, end, segs[len(segs)-1].To)
			}
			for i, s := range segs {
				if s.From > s.To {
					t.Fatalf("[%d,%d] seg %d empty: %+v", start, end, i, s)
				}
				bStart := c.BlockStart(s.BlockIndex)
				bEnd := bStart + c.BlockSize - 1
				if s.From < bStart || s.To > bEnd {
					t.Fatalf("[%d,%d] seg %d outside block bounds [%d,%d]: %+v", start, end, i, bStart, bEnd, s)
				}
				if s.ChunkIndex != c.ChunkOfBlock(s.BlockIndex) {
					t.Fatalf("[%d,%d] seg %d chunk mismatch: %+v", start, end, i, s)
				}
				if i > 0 && s.From != segs[i-1].To+1 {
					t.Fatalf("[%d,%d] gap/overlap at seg %d: prev.To=%d from=%d", start, end, i, segs[i-1].To, s.From)
				}
			}
		}
	}
}

func TestObjectID(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantID    string
		wantCAddr bool
	}{
		{"oci blob", "https://reg.example.com/v2/lib/nginx/blobs/" + digest, digest, true},
		{"digest uppercased", "https://reg.example.com/v2/x/blobs/sha256:" + strings.Repeat("AB", 32), "sha256:" + strings.Repeat("ab", 32), true},
		{"no digest strips query", "https://reg.example.com/v2/x/manifests/latest?token=abc", "https://reg.example.com/v2/x/manifests/latest", false},
		{"host lowercased", "HTTPS://Reg.Example.COM/v2/x", "https://reg.example.com/v2/x", false},
		{"host port not a digest", "http://localhost:5000/v2/x/blobs/latest", "http://localhost:5000/v2/x/blobs/latest", false},
		{"relative path no digest", "/v2/x/manifests/latest", "/v2/x/manifests/latest", false},
		{"unparseable falls back", ":nope", ":nope", false},
		{"unparseable with digest", ":x/blobs/" + digest, digest, true},
	}
	for _, tt := range tests {
		id, ca := ObjectID(tt.in)
		if id != tt.wantID || ca != tt.wantCAddr {
			t.Errorf("%s: ObjectID(%q) = (%q, %v), want (%q, %v)", tt.name, tt.in, id, ca, tt.wantID, tt.wantCAddr)
		}
	}
}

func TestIsDigest(t *testing.T) {
	good := []string{"sha256:" + strings.Repeat("a", 64), "sha512:" + strings.Repeat("f", 128), "md5x:" + strings.Repeat("0", 32)}
	bad := []string{"", "sha256:", ":abcabcabcabcabcabcabcabcabcabcab", "sha256:xyz", "localhost:5000", "sha256:" + strings.Repeat("a", 31), "SHA256:" + strings.Repeat("a", 64)}
	for _, s := range good {
		if !IsDigest(s) {
			t.Errorf("IsDigest(%q) = false, want true", s)
		}
	}
	for _, s := range bad {
		if IsDigest(s) {
			t.Errorf("IsDigest(%q) = true, want false", s)
		}
	}
}
