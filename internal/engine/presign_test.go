package engine

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// methodRecorder is an origin that behaves like a **presigned GET URL**: the
// signature covers the HTTP verb, so anything other than GET is rejected the way
// S3 and Aliyun OSS reject it.
type methodRecorder struct {
	body []byte
	mu   sync.Mutex
	seen []string
}

func (m *methodRecorder) methods() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.seen...)
}

func (m *methodRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.seen = append(m.seen, r.Method)
	m.mu.Unlock()

	if r.Method != http.MethodGet {
		// A presigned URL is signed for one verb. Using another fails signature
		// verification, which the object store reports as 403.
		http.Error(w, "SignatureDoesNotMatch", http.StatusForbidden)
		return
	}
	// Serve Range properly: the signature does not cover the Range header, so
	// ranged GETs are the supported way to probe and to read.
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(m.body))
}

// TestNeverSendsHeadUpstream locks in a constraint that is currently satisfied
// only incidentally.
//
// An upstream may be a presigned object-storage URL, and such a URL is signed for
// a single HTTP verb — in practice GET. Probing it with HEAD, which looks like an
// obvious optimization over fetching one byte, fails signature verification and
// returns 403. Size therefore probes with a ranged GET (bytes=0-0) and reads the
// total from Content-Range, and this test fails if anyone changes that.
func TestNeverSendsHeadUpstream(t *testing.T) {
	origin := &methodRecorder{body: blob(500)}
	srv := httptest.NewServer(origin)
	defer srv.Close()

	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, srv.URL+"/blob?Signature=presigned-for-get")
	front := httptest.NewServer(h)
	defer front.Close()

	// A client HEAD must be answered without ever HEADing the origin.
	resp, err := http.Head(front.URL + "/anything")
	if err != nil {
		t.Fatalf("client HEAD: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("client HEAD status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != int64(len(origin.body)) {
		t.Errorf("Content-Length = %d, want %d", resp.ContentLength, len(origin.body))
	}

	// And a client GET still works.
	resp2, err := http.Get(front.URL + "/anything")
	if err != nil {
		t.Fatalf("client GET: %v", err)
	}
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if !bytes.Equal(body, origin.body) {
		t.Errorf("client GET returned %d bytes, want %d", len(body), len(origin.body))
	}

	for i, m := range origin.methods() {
		if m != http.MethodGet {
			t.Errorf("origin request %d used %s; a presigned URL is signed for GET only", i, m)
		}
	}
	if len(origin.methods()) == 0 {
		t.Error("the origin was never contacted")
	}
}

// TestSizeProbeUsesRangedGet spells out how the size is obtained, so the
// mechanism is visible rather than implied: one ranged GET, whose Content-Range
// carries the total.
func TestSizeProbeUsesRangedGet(t *testing.T) {
	var mu sync.Mutex
	var ranges []string
	body := blob(321)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "SignatureDoesNotMatch", http.StatusForbidden)
			return
		}
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
	}))
	defer srv.Close()

	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := e.Size(context.Background(), srv.URL+"/o?Signature=x")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if got != int64(len(body)) {
		t.Errorf("Size = %d, want %d", got, len(body))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ranges) != 1 {
		t.Fatalf("origin saw %d requests for a size probe, want 1", len(ranges))
	}
	if !strings.HasPrefix(ranges[0], "bytes=0-0") {
		t.Errorf("size probe Range = %q, want a single-byte ranged GET", ranges[0])
	}
}
