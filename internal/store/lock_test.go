//go:build unix

package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLockDirExcludesSecondHolder is the point of the lock: because Open
// truncates, a second instance on the same directory must be refused rather than
// silently wiping the first one's cache.
func TestLockDirExcludesSecondHolder(t *testing.T) {
	dir := t.TempDir()
	l1, err := LockDir(dir)
	if err != nil {
		t.Fatalf("first LockDir: %v", err)
	}
	defer l1.Close()

	_, err = LockDir(dir)
	if err == nil {
		t.Fatal("second LockDir succeeded; the directory is not actually guarded")
	}
	// The message must point the operator at the cause and the fix.
	msg := err.Error()
	for _, want := range []string{dir, "already in use", "-cache-dir"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
	if !strings.Contains(msg, "pid") {
		t.Errorf("error %q does not identify the holder", msg)
	}
}

// TestLockDirReleasedOnClose: a clean shutdown must let the next instance start.
func TestLockDirReleasedOnClose(t *testing.T) {
	dir := t.TempDir()
	l1, err := LockDir(dir)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l2, err := LockDir(dir)
	if err != nil {
		t.Fatalf("LockDir after release: %v", err)
	}
	l2.Close()
}

func TestLockDirCloseIdempotent(t *testing.T) {
	l, err := LockDir(t.TempDir())
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	var nilLock *DirLock
	if err := nilLock.Close(); err != nil {
		t.Errorf("nil Close: %v", err)
	}
}

// TestLockDirDistinctDirs: the lock is per directory, so unrelated instances do
// not interfere.
func TestLockDirDistinctDirs(t *testing.T) {
	a, err := LockDir(t.TempDir())
	if err != nil {
		t.Fatalf("LockDir a: %v", err)
	}
	defer a.Close()
	b, err := LockDir(t.TempDir())
	if err != nil {
		t.Fatalf("LockDir b: %v", err)
	}
	defer b.Close()
}

func TestLockDirRecordsPID(t *testing.T) {
	dir := t.TempDir()
	l, err := LockDir(dir)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	defer l.Close()
	raw, err := os.ReadFile(filepath.Join(dir, lockName))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		t.Error("lock file does not record the holding pid")
	}
}

func TestLockDirMissingDirectory(t *testing.T) {
	if _, err := LockDir(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Error("expected an error for a nonexistent directory")
	}
}

// TestLockGuardsArenaFromTruncation ties the lock to the hazard it exists for:
// while the lock is held, a second instance never reaches the truncating Open,
// so the first instance's cached data survives.
func TestLockGuardsArenaFromTruncation(t *testing.T) {
	dir := t.TempDir()
	lock, err := LockDir(dir)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	defer lock.Close()

	path := filepath.Join(dir, "blocks.dat")
	s, err := Open(Options{Path: path, SlotSize: 64, Slots: 4})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if err := s.Put(bk(1), data(7, 32)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A second instance is refused at the lock, before it can open (and truncate)
	// the arena.
	if _, err := LockDir(dir); err == nil {
		t.Fatal("second instance was not refused")
	}
	if !s.Has(bk(1)) {
		t.Error("cached block was lost despite holding the lock")
	}
}
