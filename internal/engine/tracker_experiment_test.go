package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/chunk"
)

// Opt-in traffic experiment, not a timing assertion. A serialized 40 ms root
// service per 4 MiB block models a finite root bottleneck. Registry time is held
// within the initial tick to isolate publication from expiry. Real HTTP JOINs,
// cache deadlines and payload transfers remain active. No origin is contacted.
func TestTrackerStartupExperiment(t *testing.T) {
	if os.Getenv("DART_TRACKER_STARTUP_EXPERIMENT") != "1" {
		t.Skip("opt-in startup traffic experiment")
	}
	for _, scenario := range []struct {
		name  string
		order []int
		gap   time.Duration
	}{
		{"burst", []int{7, 0, 4, 2, 6, 1, 5, 3}, time.Millisecond},
		{"staggered", []int{7, 0, 4, 2, 6, 1, 5, 3}, 30 * time.Millisecond},
		{"reverse", []int{7, 6, 5, 4, 3, 2, 1, 0}, 30 * time.Millisecond},
		{"slow-growth", []int{7, 6, 5, 4, 3, 2, 1, 0}, 120 * time.Millisecond},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s := newStartupCluster(t, 8, 40*time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			var mu sync.Mutex
			var latencies []float64
			start := time.Now()
			for slot, index := range scenario.order {
				wg.Add(1)
				go func(slot, index int) {
					defer wg.Done()
					timer := time.NewTimer(time.Until(start.Add(time.Duration(slot) * scenario.gap)))
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-ctx.Done():
						return
					}
					for off := int64(0); off < int64(len(s.content)); off += chunk.MiB {
						var out bytes.Buffer
						began := time.Now()
						err := s.readers[index].Serve(ctx, &out, s.url, off, off+chunk.MiB-1)
						elapsed := time.Since(began).Seconds() * 1000
						if err != nil || !bytes.Equal(out.Bytes(), s.content[off:off+chunk.MiB]) {
							t.Errorf("reader %d offset %d: %v", index, off, err)
							return
						}
						mu.Lock()
						latencies = append(latencies, elapsed)
						mu.Unlock()
					}
				}(slot, index)
			}
			wg.Wait()
			elapsed := time.Since(start).Seconds()
			s.closeHTTP()
			if elapsed+0.001 < float64(s.rootBytes.Load())/float64(4*chunk.MiB)*0.040 {
				t.Fatal("serialized root service limit was not exercised")
			}
			if len(latencies) != 8*32 {
				t.Fatalf("incomplete reads: %d", len(latencies))
			}
			sort.Float64s(latencies)
			result := map[string]any{
				"scenario": scenario.name, "policy": os.Getenv("DART_REFRESH_POLICY"),
				"seconds": elapsed, "aggregate_gbps": float64(8*len(s.content)) * 8 / elapsed / 1e9,
				"root_mib": float64(s.rootBytes.Load()) / float64(chunk.MiB), "peer_mib": float64(s.peerBytes.Load()) / float64(chunk.MiB),
				"joins": s.joins.Load(), "join_response_bytes": s.joinBytes.Load(), "join_handler_ms": float64(s.joinNanos.Load()) / 1e6,
				"range_p50_ms": latencies[len(latencies)/2], "range_p95_ms": latencies[len(latencies)*95/100], "range_p99_ms": latencies[len(latencies)*99/100],
			}
			encoded, _ := json.Marshal(result)
			fmt.Printf("STARTUP %s\n", encoded)
		})
	}
}
