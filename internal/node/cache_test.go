package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

func memoryConfig(t *testing.T) config {
	t.Helper()
	// This path cannot be a directory even as root. Memory mode must not use it.
	blocker := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(blocker, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	return config{cacheMode: "memory", cacheDir: filepath.Join(blocker, "cache"), cacheSize: 256, blockSize: 16, chunkSize: 64, memSize: 1 << 20, ownedFraction: .5, prefix: "dart", adminAddr: ":0"}
}

func TestCacheModesAndRestart(t *testing.T) {
	for _, mode := range []string{"", "hybrid", "disk", "memory"} {
		t.Run(mode, func(t *testing.T) {
			cfg := memoryConfig(t)
			cfg.cacheMode = mode
			if mode != "memory" {
				cfg.cacheDir = filepath.Join(t.TempDir(), "cache")
				cfg.memSize = 32
			}
			st, closeCache, err := openCache(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := st.(*store.Hybrid); ok != (mode == "" || mode == "hybrid") {
				t.Fatalf("unexpected cache backend %T", st)
			}
			key := store.BlockKey{Chunk: 1, Block: 1}
			st.PutClass(key, []byte("data"), store.Owned)
			stats := st.Stats()
			if mode == "memory" {
				if stats.MemBlocks != 1 || stats.MemSlots != 16 {
					t.Fatalf("memory stats %+v", stats)
				}
			} else {
				if _, err := os.Stat(filepath.Join(cfg.cacheDir, "blocks.owned")); err != nil {
					t.Fatal("disk backend missing", err)
				}
				if mode == "disk" && stats.MemSlots != 0 {
					t.Fatal("disk mode allocated memory hot set")
				}
			}
			if err := closeCache.Close(); err != nil {
				t.Fatal(err)
			}
			fresh, closer, err := openCache(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer closer.Close()
			if fresh.Len() != 0 || fresh.Has(key) {
				t.Fatal("restart reused old cache entries")
			}
		})
	}
}

func TestCacheModeValidationAndFlags(t *testing.T) {
	cfg := memoryConfig(t)
	for _, change := range []func(*config){func(c *config) { c.cacheMode = "unknown" }, func(c *config) { c.cacheSize = 0 }, func(c *config) { c.cacheSize = 16 }, func(c *config) { c.ownedFraction = 1 }} {
		bad := cfg
		change(&bad)
		if _, _, err := openCache(bad); err == nil {
			t.Fatalf("bad cache config accepted %+v", bad)
		}
	}
	parsed, err := parseFlags([]string{"-cache-mode", "memory", "-cache-size", "8MiB"}, io.Discard, nil)
	if err != nil || parsed.cacheMode != "memory" || parsed.cacheSize != 8<<20 {
		t.Fatalf("flags %+v %v", parsed, err)
	}
	defaults, err := parseFlags(nil, io.Discard, nil)
	if err != nil || defaults.cacheMode != "hybrid" {
		t.Fatal("default changed", err)
	}
	cfg.cacheMode = "hybrid"
	cfg.memSize = 0
	cfg.cacheDir = t.TempDir()
	st, closer, err := openCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if _, ok := st.(*store.Tiered); !ok || st.Stats().MemSlots != 0 {
		t.Fatal("legacy mem-size=0 changed")
	}
	if cacheDescription(memoryConfig(t)) != "memory" {
		t.Fatal("memory startup log exposes unused path")
	}
}

func requestCache(t *testing.T, n *node, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	n.client.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}
func adminCache(t *testing.T, n *node, path string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	n.admin.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

func TestMemoryOnlyNodeOriginAndPeer(t *testing.T) {
	for _, rootMode := range []string{"memory", "disk", "hybrid"} {
		for _, leafMode := range []string{"memory", "disk", "hybrid"} {
			t.Run(rootMode+"-to-"+leafMode, func(t *testing.T) {
				content := blob(100)
				var originCalls atomic.Int64
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					originCalls.Add(1)
					http.ServeContent(w, r, "blob", time.Time{}, bytes.NewReader(content))
				}))
				defer origin.Close()
				rootCfg := memoryConfig(t)
				rootCfg.origin = origin.URL + "/blob"
				rootCfg.cacheMode = rootMode
				if rootMode != "memory" {
					rootCfg.cacheDir = t.TempDir()
					rootCfg.memSize = 32
				}
				rootCfg.selfID = "A"
				rootCfg.peers = "A@127.0.0.1:1"
				root, err := build(rootCfg, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				defer root.closer.Close()
				warm := requestCache(t, root, "/file")
				if warm.Code != 200 || !bytes.Equal(warm.Body.Bytes(), content) {
					t.Fatal("root warm", warm.Code, warm.Body.String())
				}
				parent := httptest.NewServer(root.peer)
				defer parent.Close()
				leafCfg := memoryConfig(t)
				leafCfg.origin = rootCfg.origin
				leafCfg.cacheMode = leafMode
				if leafMode != "memory" {
					leafCfg.cacheDir = t.TempDir()
					leafCfg.memSize = 32
				}
				leafCfg.selfID = "B"
				leafCfg.peers = "A@" + strings.TrimPrefix(parent.URL, "http://")
				leaf, err := build(leafCfg, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				defer leaf.closer.Close()
				before := originCalls.Load()
				for i := 0; i < 2; i++ {
					got := requestCache(t, leaf, "/file")
					if got.Code != 200 || !bytes.Equal(got.Body.Bytes(), content) {
						t.Fatal("leaf read", got.Code, got.Body.String())
					}
				}
				if originCalls.Load() != before+1 {
					t.Fatalf("peer read unexpectedly fetched origin data: %d -> %d", before, originCalls.Load())
				}
				metrics := adminCache(t, leaf, "/metrics")
				for _, line := range []string{`dart_store_blocks{class="borrowed"} 7`, `dart_bytes_total{direction="peer_in"} 100`} {
					if !strings.Contains(metrics, line) {
						t.Fatalf("missing metric %s in %s", line, metrics)
					}
				}
				memBlocks, memSlots := 0, 0
				if leafMode == "memory" {
					memBlocks, memSlots = 7, 16
				} else if leafMode == "hybrid" {
					memBlocks, memSlots = 2, 2
				}
				for _, line := range []string{fmt.Sprintf(`dart_store_blocks{class="mem"} %d`, memBlocks), fmt.Sprintf(`dart_store_slots{class="mem"} %d`, memSlots)} {
					if !strings.Contains(metrics, line) {
						t.Fatalf("missing metric %s", line)
					}
				}
				var stats struct {
					CachedBlocks int `json:"cached_blocks"`
				}
				if err := json.Unmarshal([]byte(adminCache(t, leaf, "/admin/stats")), &stats); err != nil || stats.CachedBlocks != 7 {
					t.Fatalf("admin cache count: %+v %v", stats, err)
				}

			})
		}
	}
}
