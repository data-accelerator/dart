package fetch

// Regression tests for issue #15 items F3/F4. (Item 3, the joiner
// refusal-retry context fix, shipped earlier under issue #4 as
// TestJoinerRetrySkippedWhenCallerGone.)

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestFetchBlockPastEndErrors pins F3: a block wholly past the probed size
// used to clamp end below start, which silently degraded to a no-Range
// full-object GET — a caller bug becoming a 4500-byte transfer. It must be a
// cheap error now, and an inverted range handed to Fetch directly errors too.
func TestFetchBlockPastEndErrors(t *testing.T) {
	var served int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served++
		w.Write(make([]byte, 4500))
	}))
	defer srv.Close()
	f := &HTTPFetcher{}

	// Block index 2 of a 4500-byte object at 4096-byte blocks starts at 8192.
	if _, err := FetchBlock(context.Background(), f, srv.URL, 4096, 2, 4500); err == nil {
		t.Fatal("FetchBlock past end-of-object: want an error, got none")
	}
	if served != 0 {
		t.Fatalf("origin was contacted %d times for an unsatisfiable block", served)
	}

	// The same trap one layer down: an inverted range must not become a
	// whole-object GET.
	if _, err := f.Fetch(context.Background(), srv.URL, 8192, 4499); err == nil {
		t.Fatal("Fetch(8192, 4499): want an error, got none")
	}
	if served != 0 {
		t.Fatalf("origin was contacted %d times for an inverted range", served)
	}

	// Control: the legitimate tail block still fetches (4500 = 4096 + 404).
	r, err := FetchBlock(context.Background(), f, srv.URL, 4096, 1, 4500)
	if err != nil {
		t.Fatalf("legitimate tail block: %v", err)
	}
	_ = r
}

// TestFetchBlockUnknownSizeTailBlock pins F4: with size unknown (size <= 0),
// the final block's natural window runs past the object; an RFC-compliant
// origin clamps it (206, Content-Range end = total-1). That answer used to be
// rejected as a wrong-length error, so the documented unknown-size flow could
// never fetch the tail block. A clamped answer is now accepted and reveals
// Total.
func TestFetchBlockUnknownSizeTailBlock(t *testing.T) {
	content := make([]byte, 5000) // 4096 + 904
	for i := range content {
		content[i] = byte(i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "blob", time.Unix(0, 0), strings.NewReader(string(content)))
	}))
	defer srv.Close()
	f := &HTTPFetcher{}

	r, err := FetchBlock(context.Background(), f, srv.URL, 4096, 1, 0 /* size unknown */)
	if err != nil {
		t.Fatalf("tail block with unknown size: %v", err)
	}
	if int64(len(r.Data)) != 904 {
		t.Fatalf("tail block = %d bytes, want 904 (clamped window)", len(r.Data))
	}
	for i, b := range r.Data {
		if b != content[4096+i] {
			t.Fatalf("byte %d wrong: the clamped window must match the object", i)
		}
	}
	if r.Total != 5000 {
		t.Fatalf("Total = %d, want 5000 revealed via Content-Range", r.Total)
	}
	if r.RangeIgnored {
		t.Fatal("a clamped 206 is not RangeIgnored")
	}

	// Controls: a short body that is NOT an RFC clamp stays a hard error
	// (wrong offset, or end not clamped to total-1).
	short := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 4096-4899/5000") // not total-1
		w.WriteHeader(http.StatusPartialContent)
		w.Write(make([]byte, 804))
	}))
	defer short.Close()
	if _, err := FetchBlock(context.Background(), f, short.URL, 4096, 1, 0); err == nil {
		t.Fatal("a short 206 whose end is not total-1 must stay an error")
	}
}
