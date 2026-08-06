//go:build unix

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

// lockName is the file used to guard a cache directory.
const lockName = ".dart.lock"

// DirLock is an exclusive advisory lock on a cache directory.
//
// It exists because opening the arena is destructive: Open truncates, so a second
// process pointed at the same -cache-dir would silently wipe the first one's
// cache. The lock turns that misconfiguration into a startup error instead.
//
// The lock is held via flock(2) on the open file descriptor, so the kernel
// releases it if the process dies. That means a crash never leaves a stale lock
// behind requiring manual cleanup.
type DirLock struct {
	f *os.File
}

// LockDir takes the exclusive lock for dir. It fails immediately (rather than
// waiting) when another process already holds it.
//
// Callers MUST acquire the lock before opening any store in that directory,
// otherwise the truncating open would destroy the other instance's data before
// the conflict is detected.
func LockDir(dir string) (*DirLock, error) {
	path := filepath.Join(dir, lockName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open lock %q: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		holder := readPID(f)
		f.Close()
		return nil, fmt.Errorf("store: cache directory %q is already in use%s: "+
			"run one dart instance per -cache-dir (%w)", dir, holder, err)
	}
	// Record the owner so an operator can identify the holder. Best effort: the
	// lock is already held, so a write failure here is not fatal.
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	return &DirLock{f: f}, nil
}

// readPID reports the recorded holder for an error message, or "" if unknown.
func readPID(f *os.File) string {
	buf := make([]byte, 32)
	n, _ := f.ReadAt(buf, 0)
	if n <= 0 {
		return ""
	}
	pid := string(buf[:n])
	for i, c := range pid {
		if c == '\n' {
			pid = pid[:i]
			break
		}
	}
	if _, err := strconv.Atoi(pid); err != nil {
		return ""
	}
	return " by pid " + pid
}

// Close releases the lock. It is safe to call once; the kernel would release the
// lock on process exit regardless.
func (l *DirLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	// Closing the descriptor releases the flock; unlock explicitly so the
	// intent is clear and the ordering is not left to the close.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}
