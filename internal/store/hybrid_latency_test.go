package store

import (
	"bytes"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// The reader pins a borrowed RAM hit while a writer cycles distinct blocks
// through the owned disk budget. Report sampled request latency as well as
// throughput. Disk writes go through the same store as production writes.
func BenchmarkHybridHitDuringOwnedWrites(b *testing.B) {
	const blockSize = 1 << 20
	disk, err := OpenTiered(TieredOptions{Path: filepath.Join(b.TempDir(), "blocks"), SlotSize: blockSize, Slots: 16})
	if err != nil {
		b.Fatal(err)
	}
	mem, err := OpenMem(MemOptions{SlotSize: blockSize, Slots: 2})
	if err != nil {
		b.Fatal(err)
	}
	h := NewHybrid(mem, disk)
	defer h.Close()
	body := bytes.Repeat([]byte("x"), blockSize)
	hot := BlockKey{Chunk: 1}
	if ok, err := h.PutClass(hot, body, Borrowed); err != nil || !ok {
		b.Fatal(ok, err)
	}
	var stop atomic.Bool
	done := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		for i := uint64(2); !stop.Load(); i++ {
			_, err := disk.PutClass(BlockKey{Chunk: i}, body, Owned)
			if i == 2 {
				close(ready)
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	<-ready
	defer func() {
		stop.Store(true)
		if err := <-done; err != nil {
			b.Error(err)
		}
	}()
	const maxSamples = 16000
	stride := (b.N-1)/maxSamples + 1
	samples := make([]int64, 0, min(b.N, maxSamples))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sample := i%stride == 0
		var start time.Time
		if sample {
			start = time.Now()
		}
		l, ok, err := h.Acquire(hot)
		if !ok || err != nil {
			b.Fatal(ok, err)
		}
		l.Release()
		if sample {
			samples = append(samples, time.Since(start).Nanoseconds())
		}
	}
	b.StopTimer()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	b.ReportMetric(float64(samples[(len(samples)-1)*99/100]), "p99-ns")
}

func TestHybridHitDuringOwnedDiskIO(t *testing.T) {
	h, _, disk := openHybrid(t, 2, 8)
	key := BlockKey{Chunk: 31}
	if ok, err := h.PutClass(key, []byte("hot"), Borrowed); !ok || err != nil {
		t.Fatal(ok, err)
	}
	owned := disk.owned.(*DiskStore)
	owned.mu.Lock()
	defer owned.mu.Unlock()
	done := make(chan bool, 1)
	go func() {
		l, ok, err := h.Acquire(key)
		if ok {
			defer l.Release()
		}
		done <- ok && err == nil && string(l.Bytes()) == "hot"
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("hot memory data unavailable")
		}
	case <-time.After(time.Second):
		t.Fatal("memory hit waiting on owned disk I/O")
	}
}
