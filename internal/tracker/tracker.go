// Package tracker maintains the per-file active reader set S: which nodes are
// currently reading a given object.
//
// The distribution tree is built over S rather than over all members, so a
// node's parent is always another reader (and therefore a node that actually
// wants the data) instead of an arbitrary member that would have to
// fetch-on-behalf. See docs/tracker.md.
//
// Design properties:
//
//   - Leases: a reader JOINs with a TTL and refreshes; when it stops reading the
//     lease lapses and it drops out of S, shrinking the tree. S is soft state, so
//     a tracker restart self-heals as readers renew.
//   - Tick freeze: the set is only recomputed on a fixed tick, so the topology
//     (and epochS) is stable between ticks and TCP connections do not churn.
//   - Control plane only: small JSON messages, no data flows through a tracker.
//
// Which node is the tracker for a file is decided by the caller (HRW top-1 over
// the file key), so this package holds no placement logic.
package tracker

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Defaults for the tick and lease. The lease is deliberately a multiple of the
// tick so a reader that renews once per tick is never dropped spuriously.
const (
	DefaultTick     = 3 * time.Second
	DefaultLeaseTTL = 2 * DefaultTick
	// MaxLeaseTTL bounds any client-requested lease. Leases are meant to be
	// refreshed on a seconds cadence, so ten minutes is already generous;
	// without a bound a caller could pin a dead reader (and with it the whole
	// file entry, blocking idle eviction) for an arbitrary duration.
	MaxLeaseTTL = 10 * time.Minute
	// DefaultIdleGrace is how long an empty, unqueried file entry is kept
	// before idle eviction forgets it.
	DefaultIdleGrace = time.Minute
)

// JoinRequest is a reader announcing (or refreshing) interest in a file.
type JoinRequest struct {
	// File identifies the object (an opaque key; callers use the file/object id).
	File string `json:"file"`
	// Node is the reader's stable cluster ID.
	Node string `json:"node"`
	// TTLMs is the requested lease in milliseconds; 0 means the tracker default.
	TTLMs int64 `json:"ttlMs,omitempty"`
}

// JoinResponse is the frozen reader set for a file.
type JoinResponse struct {
	// EpochS changes whenever the frozen set changes; readers compare it to
	// detect a topology change without diffing the member list.
	EpochS uint64 `json:"epochS"`
	// Readers is the frozen set, sorted by node ID (deterministic across nodes).
	Readers []string `json:"readers"`
	// TTLMs is the granted lease in milliseconds.
	TTLMs int64 `json:"ttlMs"`
}

// lease is one reader's membership deadline.
type lease struct {
	node     string
	expireAt time.Time
}

// fileState is the live and frozen reader sets for one file.
type fileState struct {
	leases map[string]*lease // node -> lease (live, updated on every join)
	frozen []string          // set published to readers, recomputed on a tick
	epoch  uint64            // bumped when frozen changes
	nextAt time.Time         // when the next recompute is due
	// lastActivity is the last Join/Readers for this file; it drives idle
	// eviction (see Registry.sweepLocked).
	lastActivity time.Time
}

// Registry tracks reader sets for many files. It is safe for concurrent use.
type Registry struct {
	tick  time.Duration
	ttl   time.Duration
	grace time.Duration
	now   func() time.Time // injectable clock for tests
	mu    sync.Mutex
	files map[string]*fileState
	// nextSweep bounds idle eviction to at most once per tick, amortized over
	// registry activity (no background goroutine: an idle registry grows
	// nothing, so there is nothing to sweep while nothing calls).
	nextSweep time.Time
}

// Options configures a Registry. Zero values select the defaults.
type Options struct {
	// Tick is how often the frozen set is recomputed.
	Tick time.Duration
	// LeaseTTL is the default lease granted to a reader.
	LeaseTTL time.Duration
	// IdleGrace is how long a file with no live leases and no activity is kept
	// before the registry forgets it. The grace absorbs join/leave churn;
	// forgetting is cheap (a later Join simply recreates the entry).
	IdleGrace time.Duration
	// Now overrides the clock (tests).
	Now func() time.Time
}

// NewRegistry creates a Registry.
func NewRegistry(opt Options) *Registry {
	r := &Registry{
		tick:  opt.Tick,
		ttl:   opt.LeaseTTL,
		grace: opt.IdleGrace,
		now:   opt.Now,
		files: make(map[string]*fileState),
	}
	if r.tick <= 0 {
		r.tick = DefaultTick
	}
	if r.ttl <= 0 {
		r.ttl = DefaultLeaseTTL
	}
	if r.grace <= 0 {
		r.grace = DefaultIdleGrace
	}
	if r.now == nil {
		r.now = time.Now
	}
	return r
}

// Join records (or refreshes) node's interest in file and returns the currently
// frozen reader set. The returned readers slice is a copy owned by the caller.
func (r *Registry) Join(file, node string, ttl time.Duration) JoinResponse {
	if ttl <= 0 {
		ttl = r.ttl
	}
	if ttl > MaxLeaseTTL {
		ttl = MaxLeaseTTL // never grant more than the documented bound
	}
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(now)
	fs := r.files[file]
	if fs == nil {
		fs = &fileState{leases: make(map[string]*lease)}
		r.files[file] = fs
	}
	fs.lastActivity = now
	if l := fs.leases[node]; l != nil {
		l.expireAt = now.Add(ttl)
	} else {
		fs.leases[node] = &lease{node: node, expireAt: now.Add(ttl)}
	}
	r.refreshLocked(fs, now)

	return JoinResponse{
		EpochS:  fs.epoch,
		Readers: append([]string(nil), fs.frozen...),
		TTLMs:   ttl.Milliseconds(),
	}
}

// Leave drops node's lease on file immediately (a clean shutdown or a reader
// that finished). The frozen set updates on the next tick.
func (r *Registry) Leave(file, node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fs := r.files[file]; fs != nil {
		delete(fs.leases, node)
		if len(fs.leases) == 0 {
			delete(r.files, file)
		}
	}
}

// Readers returns the frozen reader set for file (a copy) and its epoch.
func (r *Registry) Readers(file string) ([]string, uint64) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(now)
	fs := r.files[file]
	if fs == nil {
		return nil, 0
	}
	fs.lastActivity = now
	r.refreshLocked(fs, now)
	return append([]string(nil), fs.frozen...), fs.epoch
}

// Files returns the number of tracked files (diagnostics).
func (r *Registry) Files() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(r.now())
	return len(r.files)
}

// sweepLocked forgets files that have no live leases and have seen no activity
// for longer than the idle grace, bounding memory for many-file workloads.
// Expired leases are dropped first so a file whose readers simply vanished
// (no Leave) counts as empty. Sweeps run at most once per tick and are driven
// by registry activity; a totally idle tracker is only asked to forget files
// the next time anyone talks to it, which costs one O(files) scan per tick at
// most. Caller must hold r.mu.
func (r *Registry) sweepLocked(now time.Time) {
	if now.Before(r.nextSweep) {
		return
	}
	r.nextSweep = now.Add(r.tick)
	for f, fs := range r.files {
		for node, l := range fs.leases {
			if !l.expireAt.After(now) {
				delete(fs.leases, node)
			}
		}
		if len(fs.leases) == 0 && now.Sub(fs.lastActivity) > r.grace {
			delete(r.files, f)
		}
	}
}

// refreshLocked recomputes the frozen set if the tick is due, dropping expired
// leases. Caller must hold r.mu.
func (r *Registry) refreshLocked(fs *fileState, now time.Time) {
	if fs.nextAt.IsZero() {
		// First observation: publish immediately so the very first reader gets a
		// usable set instead of waiting a whole tick.
		fs.nextAt = now.Add(r.tick)
		fs.recompute(now)
		return
	}
	if now.Before(fs.nextAt) {
		return // frozen: keep the topology stable between ticks
	}
	fs.nextAt = now.Add(r.tick)
	fs.recompute(now)
}

// recompute drops expired leases and rebuilds the frozen set, bumping the epoch
// when it changed.
func (fs *fileState) recompute(now time.Time) {
	live := make([]string, 0, len(fs.leases))
	for node, l := range fs.leases {
		if l.expireAt.After(now) {
			live = append(live, node)
		} else {
			delete(fs.leases, node)
		}
	}
	sort.Strings(live)
	if !equalStrings(fs.frozen, live) {
		fs.frozen = live
		fs.epoch++
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// HTTP wire paths.
const (
	JoinPath  = "/tracker/v1/join"
	LeavePath = "/tracker/v1/leave"
)

// Server exposes a Registry over HTTP.
type Server struct{ R *Registry }

// Handler returns a mux serving the tracker endpoints.
func (s *Server) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(JoinPath, func(w http.ResponseWriter, r *http.Request) {
		var req JoinRequest
		if !decode(w, r, &req) {
			return
		}
		if req.File == "" || req.Node == "" {
			http.Error(w, "file and node are required", http.StatusBadRequest)
			return
		}
		// Clamp before the duration conversion: TTLMs values beyond
		// math.MaxInt64/int64(time.Millisecond) would overflow the multiplication
		// and silently wrap (some wrap *negative* — straight to the default —
		// and some wrap to a positive multi-decade lease).
		ttlMs := req.TTLMs
		if ttlMs > MaxLeaseTTL.Milliseconds() {
			ttlMs = MaxLeaseTTL.Milliseconds()
		}
		resp := s.R.Join(req.File, req.Node, time.Duration(ttlMs)*time.Millisecond)
		writeJSON(w, resp)
	})
	mux.HandleFunc(LeavePath, func(w http.ResponseWriter, r *http.Request) {
		var req JoinRequest
		if !decode(w, r, &req) {
			return
		}
		if req.File == "" || req.Node == "" {
			http.Error(w, "file and node are required", http.StatusBadRequest)
			return
		}
		s.R.Leave(req.File, req.Node)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(v); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}
