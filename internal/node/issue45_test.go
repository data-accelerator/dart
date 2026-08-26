package node

// Regression tests for issue #45: the shutdown admission gate must be a real
// closeable primitive (no Add-after-zero racing Wait), and an abandoned
// handler must never have the store closed under it.

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/store"
)

// TestAdmissionGateRejectsAfterCloseWithoutCounting: once closed, enter()
// fails and the tracked count never moves — no late "admission" can race the
// waiter (the WaitGroup-form version formally allowed Add-after-zero).
func TestAdmissionGateRejectsAfterCloseWithoutCounting(t *testing.T) {
	g := newAdmissionGate()
	if !g.enter() {
		t.Fatal("open gate rejected an entry")
	}
	g.close()
	g.exit() // the admitted handler leaves; count back to 0
	for i := 0; i < 100; i++ {
		if g.enter() {
			t.Fatal("closed gate admitted a handler")
		}
	}
	g.mu.Lock()
	active := g.active
	g.mu.Unlock()
	if active != 0 {
		t.Fatalf("active = %d after rejected entries, want 0", active)
	}
	// wait() on a closed, empty gate returns immediately.
	done := make(chan struct{})
	go func() { g.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait() did not return on a closed, empty gate")
	}
}

// TestAdmissionGateStress races entrants against close+wait under -race: every
// admitted entry is paired with exactly one exit, and wait() returns only
// after the count drains to zero.
func TestAdmissionGateStress(t *testing.T) {
	for round := 0; round < 20; round++ {
		g := newAdmissionGate()
		var wg sync.WaitGroup
		admitted := make(chan struct{}, 256)
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if g.enter() {
					admitted <- struct{}{}
					time.Sleep(time.Millisecond)
					g.exit()
				}
			}()
		}
		time.Sleep(2 * time.Millisecond) // let some enter, some not
		g.close()
		done := make(chan struct{})
		go func() { g.wait(); close(done) }()
		wg.Wait()
		close(admitted)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: wait() never returned though every entrant exited", round)
		}
	}
}

// TestServerSetAbandonsWedgedHandler: a handler that ignores cancellation past
// the drain budget must NOT have the store closed under it — shutdown reports
// it as abandoned, and its gate stays closed (late entries get 503).
func TestServerSetAbandonsWedgedHandler(t *testing.T) {
	wedge := make(chan struct{}) // never closed until test end
	defer close(wedge)
	entered := make(chan struct{})
	ss := newServerSet()
	ss.add("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-wedge // ignore the request context entirely
	}))

	// Serve on an ephemeral port: listen manually to learn the address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ss.servers[0].srv
	go srv.Serve(ln)
	defer srv.Close()

	// Fire the request asynchronously: the wedged handler never answers, so a
	// synchronous Get would block the test itself.
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String() + "/x"); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the wedged handler was never entered")
	}

	// Tiny budgets: drain fails, gate closes, join grace expires → abandoned.
	drainErr, abandoned := ss.shutdown(20*time.Millisecond, 20*time.Millisecond)
	if drainErr == nil {
		t.Fatal("expected a drain error for the wedged handler")
	}
	if abandoned != 1 {
		t.Fatalf("abandoned = %d, want 1 (the wedged handler)", abandoned)
	}

	// Late entries are rejected at the gate (503), never served.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/y", nil)
	ss.servers[0].srv.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-gate entry got %d, want 503", rr.Code)
	}
}

// TestServerSetJoinsCooperativeHandler: a handler that unwinds when its
// connection closes is joined cleanly — nothing abandoned.
func TestServerSetJoinsCooperativeHandler(t *testing.T) {
	entered := make(chan struct{})
	ss := newServerSet()
	ss.add("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done() // cooperative: unwinds on cancellation
	}))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ss.servers[0].srv
	go srv.Serve(ln)
	defer srv.Close()

	go func() { _, _ = http.Get("http://" + ln.Addr().String() + "/x") }()
	<-entered

	drainErr, abandoned := ss.shutdown(20*time.Millisecond, 2*time.Second)
	if drainErr == nil {
		t.Fatal("expected a drain error (handler outlived the tiny drain budget)")
	}
	if abandoned != 0 {
		t.Fatalf("abandoned = %d, want 0 — the cooperative handler must be joined", abandoned)
	}
}

// TestFinishRunLeavesStoreOpenWhenAbandoned pins the lifetime contract of
// issue #45 item 2: with abandoned handlers, the store and cache-dir lock are
// NOT closed (closing them would permit use-after-close); without abandoned
// handlers they are closed normally.
func TestFinishRunLeavesStoreOpenWhenAbandoned(t *testing.T) {
	cfg := config{
		cacheDir: t.TempDir(), cacheSize: 1 << 20, memSize: 1 << 14,
		chunkSize: 64 * 1024, blockSize: 16 * 1024,
	}
	var out syncBuffer

	// Abandon path: lock stays held, store stays open.
	n, err := build(cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	finishRun(n, &out, nil, 1)
	// The dir lock is taken by node.build (not by the store itself), so the
	// observable contract is LockDir: it must fail while the abandoned node's
	// lock fd is still open.
	lk, err := store.LockDir(cfg.cacheDir)
	if err == nil {
		lk.Close()
		t.Fatal("cache dir lock reacquired while an abandoned handler may still run — " +
			"the lock must be left held on the abandon path")
	}
	if !strings.Contains(out.String(), "deliberately left open") {
		t.Fatalf("expected the deliberate-leak diagnostic, got %q", out.String())
	}

	// Clean path: resources are released (lock free, dir reopenable).
	cfg2 := cfg
	cfg2.cacheDir = t.TempDir()
	n2, err := build(cfg2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	finishRun(n2, &out, nil, 0)
	lk2, err := store.LockDir(cfg2.cacheDir)
	if err != nil {
		t.Fatalf("clean shutdown must release the cache-dir lock: %v", err)
	}
	lk2.Close()
}

// syncBuffer is a goroutine-safe io.Writer for capturing the banner.
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// bannerAddr extracts the client= address from the startup banner.
func bannerAddr(banner string) string {
	const prefix = "dart client="
	i := strings.Index(banner, prefix)
	if i < 0 {
		return ""
	}
	rest := banner[i+len(prefix):]
	j := strings.IndexByte(rest, ' ')
	if j < 0 {
		return ""
	}
	return rest[:j]
}
