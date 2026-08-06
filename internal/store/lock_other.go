//go:build !unix

package store

// DirLock is a no-op on platforms without flock(2).
//
// DART targets Linux (DaemonSet deployment, sendfile/splice data path), so the
// lock is only implemented there. On other platforms LockDir succeeds without
// guarding anything rather than failing the build, and the single-instance rule
// from docs/store.md must be enforced by the operator.
type DirLock struct{}

// LockDir returns an unlocked DirLock; see the type comment.
func LockDir(dir string) (*DirLock, error) { return &DirLock{}, nil }

// Close releases nothing.
func (l *DirLock) Close() error { return nil }
