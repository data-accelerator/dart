// Package chunk defines DART's three-level addressing — object -> chunk ->
// block — and the pure, deterministic keying and range decomposition built on
// it. See docs/chunk.md.
//
//	object   whole blob (GB-scale)
//	chunk    large logical unit (default 256 MiB): HRW placement + P2P tree
//	block    small physical unit (default 4 MiB): transfer / cache slot / on-demand read
//
// Placement and the distribution tree operate on chunks (via ChunkKey);
// transfer and caching operate on blocks. The HTTP service layer accepts an
// arbitrary byte range and Segments decomposes it into the covering blocks.
//
// All functions here are pure and deterministic. ChunkKey is part of the wire
// protocol (it selects owners); its construction is pinned by a cross-language
// golden test and changing it must be treated as a breaking change.
package chunk

import (
	"encoding/binary"
	"errors"
	"math"
	"net/url"
	"strings"
)

// Size constants for convenience.
const (
	MiB = 1 << 20
	GiB = 1 << 30
)

// Config fixes the object -> chunk -> block grid. ChunkSize must be a positive
// multiple of a positive BlockSize so blocks nest exactly within chunks.
type Config struct {
	ChunkSize int64 // bytes, e.g. 256*MiB
	BlockSize int64 // bytes, e.g. 4*MiB
}

// DefaultConfig returns the recommended defaults: 256 MiB chunks, 4 MiB blocks
// (64 blocks per chunk).
func DefaultConfig() Config { return Config{ChunkSize: 256 * MiB, BlockSize: 4 * MiB} }

// ErrInvalidConfig is returned by Validate for a malformed Config.
var ErrInvalidConfig = errors.New("chunk: invalid config (need BlockSize>0, ChunkSize>0, ChunkSize%BlockSize==0)")

// Validate reports whether the Config is well-formed.
func (c Config) Validate() error {
	if c.BlockSize <= 0 || c.ChunkSize <= 0 || c.ChunkSize%c.BlockSize != 0 {
		return ErrInvalidConfig
	}
	return nil
}

// BlocksPerChunk returns ChunkSize/BlockSize. Requires a valid Config.
func (c Config) BlocksPerChunk() int64 { return c.ChunkSize / c.BlockSize }

// ChunkIndex returns the chunk index containing the given absolute byte offset
// (offset >= 0).
func (c Config) ChunkIndex(offset int64) int64 { return offset / c.ChunkSize }

// BlockIndex returns the global block index containing the given absolute byte
// offset (offset >= 0).
func (c Config) BlockIndex(offset int64) int64 { return offset / c.BlockSize }

// ChunkOfBlock returns the chunk index that owns the given global block index.
func (c Config) ChunkOfBlock(blockIndex int64) int64 { return blockIndex / c.BlocksPerChunk() }

// BlockStart returns the absolute byte offset at which the given block begins.
func (c Config) BlockStart(blockIndex int64) int64 { return blockIndex * c.BlockSize }

// MaxBlockIndex returns the largest block index whose whole byte range
// [index*BlockSize, index*BlockSize+BlockSize-1] is representable in int64
// arithmetic. Indices above it wrap: the start-offset multiplication (or the
// end-offset addition) overflows int64, and a wrapped start can silently
// recycle to 0 (fetching the wrong block) or go negative (where an HTTP
// fetcher reads start < 0 as "no Range header", degrading a one-block fetch
// into a whole-object GET — see issue #52).
//
// Block indices arrive from two places: byte offsets of a clamped client
// range (BlockIndex(offset), always <= MaxInt64/BlockSize, which is <= this
// bound for the power-of-two block sizes DART uses) and the peer wire, which
// carries an unrestricted uint64. Callers accepting a peer-supplied index
// must reject anything above this bound before doing range geometry with it.
// Requires BlockSize > 0.
func (c Config) MaxBlockIndex() int64 { return (math.MaxInt64 - c.BlockSize + 1) / c.BlockSize }

// Segment is the intersection of a requested byte range with a single block:
// which chunk and block it belongs to, and the absolute inclusive byte range
// [From, To] to actually read/serve from that block.
type Segment struct {
	ChunkIndex int64
	BlockIndex int64
	From       int64 // inclusive absolute offset, >= block start and >= range start
	To         int64 // inclusive absolute offset, <= block end and <= range end
}

// Segments decomposes the inclusive absolute byte range [start, end] into the
// ordered list of blocks covering it. Each returned Segment identifies the
// chunk/block and the sub-range within that block to serve.
//
// start and end are inclusive; the caller must have clamped them to the object
// bounds (0 <= start <= end <= size-1). For an invalid range (start < 0 or
// end < start) Segments returns nil. Requires a valid Config.
func (c Config) Segments(start, end int64) []Segment {
	if start < 0 || end < start {
		return nil
	}
	first := start / c.BlockSize
	last := end / c.BlockSize
	segs := make([]Segment, 0, last-first+1)
	for b := first; b <= last; b++ {
		bStart := b * c.BlockSize
		bEnd := bStart + c.BlockSize - 1
		from := bStart
		if start > from {
			from = start
		}
		to := bEnd
		if end < to {
			to = end
		}
		segs = append(segs, Segment{
			ChunkIndex: b / c.BlocksPerChunk(),
			BlockIndex: b,
			From:       from,
			To:         to,
		})
	}
	return segs
}

// ChunkKey computes the 64-bit key for a chunk of an object, used by the
// placement layer (hashring.Rank) to select the owner and replicas.
//
// It is a deterministic FNV-1a hash over the canonical serialization
// (namespace, 0x1F, objectID, 0x1F, big-endian chunkIndex) plus an fmix64
// finalizer. Fully specified for cross-implementation reproducibility; pinned
// by a golden test. Changing it reshuffles placement and is a breaking change.
func ChunkKey(namespace, objectID string, chunkIndex int64) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	mix := func(b byte) { h ^= uint64(b); h *= prime64 }
	for i := 0; i < len(namespace); i++ {
		mix(namespace[i])
	}
	mix(UnitSep)
	for i := 0; i < len(objectID); i++ {
		mix(objectID[i])
	}
	mix(UnitSep)
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], uint64(chunkIndex))
	for _, b := range num {
		mix(b)
	}
	return fmix64(h)
}

func fmix64(z uint64) uint64 {
	z ^= z >> 33
	z *= 0xff51afd7ed558ccd
	z ^= z >> 33
	z *= 0xc4ceb9fe1a85ec53
	z ^= z >> 33
	return z
}

// ObjectID derives the caching identity of a blob URL, recognizing Docker
// Distribution's object-storage layout in addition to OCI blob paths. See
// ObjectIDLayout for the full contract.
func ObjectID(rawURL string) (id string, contentAddressed bool) {
	return ObjectIDLayout(rawURL, LayoutDistribution)
}

// DigestLayout selects which URL shapes are searched for a content digest.
type DigestLayout int

const (
	// LayoutDistribution recognizes both an OCI blob path ("<algo>:<hex>") and
	// Docker Distribution's object-storage layout
	// (".../blobs/<algo>/<hex[0:2]>/<hex>[/data]"). It is the default because an
	// upstream is often a *presigned object-storage URL* pointing into a
	// registry's backing bucket, where the digest appears only in that second
	// form.
	LayoutDistribution DigestLayout = iota
	// LayoutOCIOnly recognizes only "<algo>:<hex>". Use it when the backing store
	// layout is unknown or non-standard: object identity then falls back to the
	// canonical URL, which costs deduplication across endpoints but cannot
	// possibly conflate two distinct objects.
	LayoutOCIOnly
)

// ObjectIDLayout derives the caching identity of a blob URL.
//
// It prefers content addressing: if a content digest can be recovered from the
// URL path, that digest (lower-cased) is returned and contentAddressed is true —
// enabling dedup across origins/registries and marking the object immutable.
// Otherwise a canonical form of the URL (lower-cased scheme+host, path without
// query/fragment) is returned and contentAddressed is false.
//
// The query is always excluded, which matters beyond tidiness: a presigned
// upstream carries a signature there that differs on every request, so including
// it would give the same object a new identity each time.
//
// Note: non-OCI integrity schemes (npm/pypi "sha512-<base64>", etc.) are not
// recognized as digests here; mapping those is a policy-layer concern.
func ObjectIDLayout(rawURL string, layout DigestLayout) (id string, contentAddressed bool) {
	u, err := url.Parse(rawURL)
	if err == nil {
		segs := strings.Split(u.Path, "/")
		for _, seg := range segs {
			if IsDigest(seg) {
				return strings.ToLower(seg), true
			}
		}
		if layout == LayoutDistribution {
			if d, ok := distributionDigest(segs); ok {
				return d, true
			}
		}
		host := strings.ToLower(u.Host)
		scheme := strings.ToLower(u.Scheme)
		if scheme != "" || host != "" {
			return stripUnitSep(scheme + "://" + host + u.Path), false
		}
	}
	// Unparseable or scheme/host-less: fall back to the raw string with the
	// query and fragment cut — identity must not move with a signature (the
	// query is always excluded, §3.5) — but still honor an embedded digest.
	raw := rawURL
	if i := strings.IndexAny(raw, "?#"); i >= 0 {
		raw = raw[:i]
	}
	for _, seg := range strings.Split(raw, "/") {
		if IsDigest(seg) {
			return strings.ToLower(seg), true
		}
	}
	return stripUnitSep(raw), false
}

// UnitSep is the field separator ChunkKey mixes between namespace and
// objectID. It must never appear inside either field, or serialization would
// not be injective: ChunkKey("a","b␟c",0) would equal ChunkKey("a␟b","c",0).
// Namespaces containing it are rejected at engine construction; derived object
// identities have it stripped (stripUnitSep).
const UnitSep = 0x1F

// stripUnitSep removes the ChunkKey field separator from a derived object
// identity. A URL can deliver a literal 0x1F via a percent-encoded "%1F" in
// the path; dropping it keeps ChunkKey injective. (Digest identities are pure
// hex and never contain it.)
func stripUnitSep(s string) string {
	if strings.IndexByte(s, UnitSep) < 0 {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == UnitSep {
			return -1
		}
		return r
	}, s)
}

// digestHexLen maps a digest algorithm to its hex-encoded length. Only
// algorithms whose length we can verify are accepted, because the length check is
// part of what makes recognition safe.
var digestHexLen = map[string]int{
	"sha256": 64,
	"sha512": 128,
}

// distributionDigest recovers a digest from Docker Distribution's blob layout:
//
//	.../blobs/<algo>/<hex[0:2]>/<hex>[/data]
//
// e.g. "/docker/registry/v2/blobs/sha256/ab/ab<62 more hex>/data".
//
// Recognition is deliberately strict, because a false positive would map two
// *different* objects onto one cache key — a correctness bug, not a performance
// one. Three independent conditions must all hold:
//
//  1. the algorithm is one whose digest length we know;
//  2. the hex is well-formed and exactly that length;
//  3. the intermediate segment equals the hex's own first two characters.
//
// Condition 3 is the strong one: it is a self-consistency check that arbitrary
// paths are vanishingly unlikely to satisfy by accident.
func distributionDigest(segs []string) (string, bool) {
	for i := 0; i+3 < len(segs); i++ {
		if segs[i] != "blobs" {
			continue
		}
		algo, prefix, hex := strings.ToLower(segs[i+1]), segs[i+2], strings.ToLower(segs[i+3])
		want, known := digestHexLen[algo]
		if !known || len(hex) != want || len(prefix) != 2 {
			continue
		}
		if !isHex(hex) || hex[:2] != strings.ToLower(prefix) {
			continue
		}
		return algo + ":" + hex, true
	}
	return "", false
}

// isHex reports whether s is non-empty and all hex digits.
func isHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// isDigest reports whether s looks like "<algo>:<hex>" with at least 32 hex
// digits (covers sha256 = 64, sha512 = 128).
// IsDigest reports whether s is a recognizable content digest
// "<algorithm>:<hex>" (lowercase alnum algorithm, >= 32 hex chars). It is the
// shared recognizer used by ObjectIDLayout and by the registry mirror, which
// must classify cacheable paths exactly as content-addressed ones.
func IsDigest(s string) bool {
	i := strings.IndexByte(s, ':')
	if i <= 0 || i == len(s)-1 {
		return false
	}
	algo, hex := s[:i], s[i+1:]
	if len(hex) < 32 {
		return false
	}
	for j := 0; j < len(algo); j++ {
		if c := algo[j]; !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	for j := 0; j < len(hex); j++ {
		c := hex[j]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
