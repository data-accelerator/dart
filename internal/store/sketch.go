package store

import "sync"

// sketch is a TinyLFU frequency estimator: a 4-bit count-min sketch plus a
// doorkeeper bloom filter, with periodic halving so the estimate tracks recent
// popularity instead of all-time totals.
//
// It answers "is this key popular enough to be worth caching?" in O(1) space
// proportional to the cache capacity, not the key space — the admission gate for
// the borrowed budget, which must not let a one-shot relayed block evict a block
// that is actually being reused.
//
// The doorkeeper absorbs the long tail: a key seen for the very first time only
// sets its doorkeeper bits, so single-touch keys never consume sketch counters
// and never look popular.
type sketch struct {
	mu sync.Mutex

	// counters holds 4-bit counters packed two per byte.
	counters []byte
	mask     uint64 // len(counters)*2 - 1 (a power of two minus one)

	// door is the doorkeeper bloom filter (1 bit per slot).
	door     []byte
	doorMask uint64

	adds    int // increments since the last reset
	resetAt int // halve everything once adds reaches this
}

// fmix64 is the murmur3-style finalizer, used to derive a second independent
// hash from the key hash so the sketch gets four spread-out positions.
func fmix64(z uint64) uint64 {
	z ^= z >> 33
	z *= 0xff51afd7ed558ccd
	z ^= z >> 33
	z *= 0xc4ceb9fe1a85ec53
	z ^= z >> 33
	return z
}

// minSketchKeys floors the sketch size. Sizing purely by cache capacity is a
// trap: a small budget (say 2 slots) would yield a handful of counters, every
// key would alias onto them, and an unseen candidate would inherit a warm
// entry's frequency — defeating admission entirely. The sketch must be sized for
// the number of *distinct keys seen in a window*, which is far larger than the
// budget, so we keep a floor regardless of capacity.
const minSketchKeys = 256

// newSketch sizes a sketch for roughly capacity distinct hot keys (floored at
// minSketchKeys).
func newSketch(capacity int) *sketch {
	if capacity < minSketchKeys {
		capacity = minSketchKeys
	}
	// Four counters per tracked key keeps the count-min error low; round the
	// counter count up to a power of two so indexing is a mask.
	n := nextPow2(uint64(capacity) * 4)
	doorBits := nextPow2(uint64(capacity) * 8)
	s := &sketch{
		counters: make([]byte, n/2),
		mask:     n - 1,
		door:     make([]byte, doorBits/8),
		doorMask: doorBits - 1,
		// Halve after ~10 increments per tracked key: frequent enough to forget
		// stale popularity, rare enough that estimates stay meaningful.
		resetAt: capacity * 10,
	}
	return s
}

func nextPow2(v uint64) uint64 {
	if v < 2 {
		return 2
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}

// four independent-ish positions derived from one 64-bit hash.
func (s *sketch) positions(h uint64) [4]uint64 {
	h2 := fmix64(h ^ 0x9e3779b97f4a7c15)
	return [4]uint64{
		h & s.mask,
		(h >> 16) & s.mask,
		h2 & s.mask,
		(h2 >> 16) & s.mask,
	}
}

// get4 reads the 4-bit counter at index i.
func (s *sketch) get4(i uint64) byte {
	b := s.counters[i/2]
	if i%2 == 0 {
		return b & 0x0F
	}
	return b >> 4
}

// inc4 increments the 4-bit counter at index i, saturating at 15.
func (s *sketch) inc4(i uint64) {
	b := s.counters[i/2]
	if i%2 == 0 {
		if v := b & 0x0F; v < 15 {
			s.counters[i/2] = (b & 0xF0) | (v + 1)
		}
		return
	}
	if v := b >> 4; v < 15 {
		s.counters[i/2] = (b & 0x0F) | ((v + 1) << 4)
	}
}

// doorTest reports whether the doorkeeper has seen h, and sets its bits.
// It returns true when all bits were already set (i.e. a probable repeat).
func (s *sketch) doorTestAndSet(h uint64) bool {
	seen := true
	for _, p := range s.positions(h) {
		bit := p & s.doorMask
		idx, off := bit/8, byte(1)<<(bit%8)
		if s.door[idx]&off == 0 {
			seen = false
			s.door[idx] |= off
		}
	}
	return seen
}

// Increment records an access to the key hash.
func (s *sketch) Increment(h uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// First sighting only primes the doorkeeper: the long tail of one-shot keys
	// never reaches the counters.
	if !s.doorTestAndSet(h) {
		return
	}
	for _, p := range s.positions(h) {
		s.inc4(p)
	}
	if s.adds++; s.adds >= s.resetAt {
		s.resetLocked()
	}
}

// Estimate returns the approximate recent frequency of h: the doorkeeper
// contributes 1, the count-min sketch the rest.
func (s *sketch) Estimate(h uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.estimateLocked(h)
}

func (s *sketch) estimateLocked(h uint64) int {
	min := byte(15)
	for _, p := range s.positions(h) {
		if v := s.get4(p); v < min {
			min = v
		}
	}
	est := int(min)
	if s.doorSeenLocked(h) {
		est++
	}
	return est
}

func (s *sketch) doorSeenLocked(h uint64) bool {
	for _, p := range s.positions(h) {
		bit := p & s.doorMask
		if s.door[bit/8]&(byte(1)<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// resetLocked halves every counter and clears the doorkeeper, so popularity
// decays and the sketch tracks a recent window.
func (s *sketch) resetLocked() {
	for i := range s.counters {
		// Halve both nibbles at once; the low bits of each are dropped.
		s.counters[i] = (s.counters[i] >> 1) & 0x77
	}
	for i := range s.door {
		s.door[i] = 0
	}
	s.adds = 0
}

// Admit reports whether a candidate key should be admitted in place of the
// current eviction victim: the candidate wins only if it is estimated to be at
// least as popular as the victim. This is the TinyLFU admission rule, and it is
// what stops a stream of one-shot blocks from wiping a warm cache.
func (s *sketch) Admit(candidate, victim uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.estimateLocked(candidate) >= s.estimateLocked(victim)
}
