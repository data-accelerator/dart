package node

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/data-accelerator/dart/internal/store"
)

func openCache(cfg config) (store.ClassStore, closers, error) {
	mode := cfg.cacheMode
	if mode == "" {
		mode = "hybrid"
	}
	if mode != "memory" && mode != "disk" && mode != "hybrid" {
		return nil, nil, fmt.Errorf("unknown cache-mode %q (want memory, disk, or hybrid)", mode)
	}
	if cfg.cacheSize < cfg.blockSize {
		return nil, nil, fmt.Errorf("cache-size %d must be >= block-size %d", cfg.cacheSize, cfg.blockSize)
	}
	if cfg.cacheSize/cfg.blockSize < 2 {
		return nil, nil, fmt.Errorf("cache-size %d must contain at least two blocks of %d bytes", cfg.cacheSize, cfg.blockSize)
	}
	opt := store.TieredOptions{SlotSize: cfg.blockSize, Slots: int(cfg.cacheSize / cfg.blockSize), OwnedFraction: cfg.ownedFraction, Memory: mode == "memory"}
	if mode == "memory" {
		st, err := store.OpenTiered(opt)
		if err != nil {
			return nil, nil, err
		}
		return st, closers{st}, nil
	}
	if err := os.MkdirAll(cfg.cacheDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create cache dir: %w", err)
	}
	// Lock before opening arenas: their creation truncates existing files.
	lock, err := store.LockDir(cfg.cacheDir)
	if err != nil {
		return nil, nil, err
	}
	opt.Path = filepath.Join(cfg.cacheDir, "blocks")
	disk, err := store.OpenTiered(opt)
	if err != nil {
		lock.Close()
		return nil, nil, err
	}
	var st store.ClassStore = disk
	if mode == "hybrid" && cfg.memSize >= cfg.blockSize {
		mem, err := store.OpenMem(store.MemOptions{SlotSize: cfg.blockSize, Slots: int(cfg.memSize / cfg.blockSize)})
		if err != nil {
			disk.Close()
			lock.Close()
			return nil, nil, err
		}
		st = store.NewHybrid(mem, disk)
	}
	return st, closers{st, lock}, nil
}

func cacheDescription(cfg config) string {
	if cfg.cacheMode == "memory" {
		return "memory"
	}
	mode := "disk"
	if cfg.cacheMode != "disk" && cfg.memSize >= cfg.blockSize {
		mode = "hybrid"
	}
	return mode + " " + cfg.cacheDir
}
