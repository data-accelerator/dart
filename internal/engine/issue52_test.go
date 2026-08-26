package engine

// Regression tests for issue #52: the peer wire carries the block index as an
// unrestricted uint64, but relay geometry is int64 arithmetic. Indices that
// cannot be represented — above MaxInt64, or whose window start
// index*BlockSize would overflow int64 — must be declined at the relay
// boundary, before cache lookup, relay selection, size probe, or origin I/O.
// Before the guard the wrapped start either recycled to 0 (a 2^63 index was
// served block 0's bytes, held=true) or went negative (the fetcher omitted
// the Range header and pulled the whole object — the amplification shape of
// issue #15, re-entering via the peer path).

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// malformedWireIndices cannot be represented in signed range geometry under
// testCfg() (BlockSize 16, chunk.MaxBlockIndex 576460752303423487). The last
// entry is the first index whose window overflows: 576460752303423488*16 =
// 2^63 (independently computed, see chunk.TestMaxBlockIndexGolden).
var malformedWireIndices = []struct {
	name  string
	block uint64
}{
	{"2^63 wraps to MinInt64 on int64 conversion", 1 << 63},
	{"2^64-1 wraps to -1", math.MaxUint64},
	{"first index whose byte window overflows int64", 576460752303423488},
}

// TestPeerSourceRejectsMalformedBlockIndex: the buffered relay source must
// decline a malformed wire index cheaply — no error (the fault is the
// requester's; a 500 would charge this healthy relay on the requester's
// circuit breaker), no origin contact (not even the size probe), no cache
// pollution.
func TestPeerSourceRejectsMalformedBlockIndex(t *testing.T) {
	content := blob(100)
	var reqs int64
	origin := countingOrigin(t, content, &reqs)

	st := openStoreAt(t)
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := e.PeerSource()

	for _, tc := range malformedWireIndices {
		atomic.StoreInt64(&reqs, 0)
		key := store.BlockKey{Chunk: 0, Block: tc.block}
		data, held, err := src(context.Background(),
			peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1})
		if err != nil || held || data != nil {
			t.Errorf("%s: data=%d bytes held=%v err=%v, want a cheap decline", tc.name, len(data), held, err)
		}
		if got := atomic.LoadInt64(&reqs); got != 0 {
			t.Errorf("%s: origin contacted %d times for a malformed index (size probe or fetch)", tc.name, got)
		}
		if st.Has(key) {
			t.Errorf("%s: a malformed-index block entered the cache", tc.name)
		}
	}

	// Control: a legitimate relay request still fetches and serves block 1.
	atomic.StoreInt64(&reqs, 0)
	key := store.BlockKey{Chunk: blockKeyFor(origin.URL).Chunk, Block: 1}
	data, held, err := src(context.Background(),
		peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1})
	if err != nil || !held {
		t.Fatalf("legitimate relay: held=%v err=%v", held, err)
	}
	if !bytes.Equal(data, content[16:32]) {
		t.Fatalf("legitimate relay served %d wrong bytes", len(data))
	}
	if atomic.LoadInt64(&reqs) == 0 {
		t.Fatal("control: the legitimate relay must actually reach origin")
	}
}

// TestPeerStreamSourceRejectsMalformedBlockIndex: same contract on the
// cut-through path — decline, nothing streamed, no size announced, no origin
// contact.
func TestPeerStreamSourceRejectsMalformedBlockIndex(t *testing.T) {
	content := blob(100)
	var reqs int64
	origin := countingOrigin(t, content, &reqs)

	st := openStoreAt(t)
	e, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := e.PeerStreamSource()

	for _, tc := range malformedWireIndices {
		atomic.StoreInt64(&reqs, 0)
		key := store.BlockKey{Chunk: 0, Block: tc.block}
		var buf bytes.Buffer
		var sized int64
		n, ok, err := src(context.Background(),
			peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1},
			&buf, func(m int64) { sized = m })
		if err != nil || ok || n != 0 {
			t.Errorf("%s: n=%d ok=%v err=%v, want a cheap decline", tc.name, n, ok, err)
		}
		if buf.Len() != 0 {
			t.Errorf("%s: %d bytes streamed for a malformed index", tc.name, buf.Len())
		}
		if sized != 0 {
			t.Errorf("%s: sizer called with %d for a malformed index", tc.name, sized)
		}
		if got := atomic.LoadInt64(&reqs); got != 0 {
			t.Errorf("%s: origin contacted %d times for a malformed index (size probe or fetch)", tc.name, got)
		}
		if st.Has(key) {
			t.Errorf("%s: a malformed-index block entered the cache", tc.name)
		}
	}

	// Control: a legitimate relay request still streams block 1.
	atomic.StoreInt64(&reqs, 0)
	key := store.BlockKey{Chunk: blockKeyFor(origin.URL).Chunk, Block: 1}
	var buf bytes.Buffer
	n, ok, err := src(context.Background(),
		peer.BlockRequest{Key: key, URL: origin.URL, Hop: 1},
		&buf, func(int64) {})
	if err != nil || !ok {
		t.Fatalf("legitimate stream relay: ok=%v err=%v", ok, err)
	}
	if n != 16 || !bytes.Equal(buf.Bytes(), content[16:32]) {
		t.Fatalf("legitimate stream relay wrote %d wrong bytes", n)
	}
	if atomic.LoadInt64(&reqs) == 0 {
		t.Fatal("control: the legitimate relay must actually reach origin")
	}
}
