package node

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/data-accelerator/dart/internal/store"
)

func TestSingleClassCacheConfiguration(t *testing.T) {
	for _, mode := range []string{"memory", "disk", "hybrid"} {
		for _, fraction := range []string{"0", "1"} {
			t.Run(mode+"/"+fraction, func(t *testing.T) {
				parsed, err := parseFlags([]string{"-owned-fraction", fraction}, io.Discard, nil)
				if err != nil || !parsed.ownedFractionSet {
					t.Fatalf("explicit share lost: %+v %v", parsed, err)
				}
				cfg := memoryConfig(t)
				cfg.cacheMode, cfg.cacheDir = mode, filepath.Join(t.TempDir(), "cache")
				cfg.cacheSize, cfg.memSize = cfg.blockSize, cfg.blockSize
				cfg.ownedFraction, cfg.ownedFractionSet = parsed.ownedFraction, parsed.ownedFractionSet
				st, closer, err := openCache(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer closer.Close()
				active, disabled := store.Owned, store.Borrowed
				if fraction == "0" {
					active, disabled = store.Borrowed, store.Owned
				}
				if ok, err := st.PutClass(store.BlockKey{Chunk: 1}, []byte("active"), active); err != nil || !ok {
					t.Fatal(ok, err)
				}
				if ok, err := st.PutClass(store.BlockKey{Chunk: 2}, []byte("skip"), disabled); err != nil || ok {
					t.Fatal(ok, err)
				}
				if st.Len() != 1 || !st.Has(store.BlockKey{Chunk: 1}) {
					t.Fatal("class budget was not preserved")
				}
			})
		}
	}
}
