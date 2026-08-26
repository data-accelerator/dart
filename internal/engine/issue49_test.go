package engine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestParseRangeSuffixEmptyObject pins the parser half of issue #49: a suffix
// range (bytes=-n) against a zero-length object used to be clamped to
// (start=0, end=-1, ok=true) — an inverted interval that the handler then
// emitted as "206" with "Content-Range: bytes 0--1/0". Any range request
// against an empty representation is unsatisfiable (RFC 7233 §4.4: 416).
func TestParseRangeSuffixEmptyObject(t *testing.T) {
	for _, hdr := range []string{"bytes=-1", "bytes=-10", "bytes=-9223372036854775807"} {
		start, end, isRange, ok := parseRange(hdr, 0)
		if ok {
			t.Errorf("parseRange(%q, 0) = (%d,%d,ok=%v): an empty object has no bytes to suffix into; must be unsatisfiable",
				hdr, start, end, ok)
			continue
		}
		if !isRange {
			t.Errorf("parseRange(%q, 0): isRange=%v, want true (a Range header was present, so the handler answers 416, not 400)",
				hdr, isRange)
		}
		if start > end && ok {
			t.Errorf("parseRange(%q, 0) returned an inverted interval [%d,%d] marked satisfiable", hdr, start, end)
		}
	}

	// The non-empty suffix contract must be unchanged by the empty-object guard.
	if s, e, _, ok := parseRange("bytes=-10", 100); !ok || s != 90 || e != 99 {
		t.Errorf("parseRange(bytes=-10, 100) = (%d,%d,ok=%v), want (90,99,true)", s, e, ok)
	}
	if s, e, _, ok := parseRange("bytes=-999", 100); !ok || s != 0 || e != 99 {
		t.Errorf("parseRange(bytes=-999, 100) = (%d,%d,ok=%v), want (0,99,true) (clamped to whole object)", s, e, ok)
	}
}

// TestEmptyObjectSuffixRangeServes416 pins the handler half of issue #49:
// `Range: bytes=-n` on an empty object must be 416 with
// `Content-Range: bytes */0` — never a 206 carrying an inverted
// `Content-Range: bytes 0--1/0`.
func TestEmptyObjectSuffixRangeServes416(t *testing.T) {
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: emptyFetcher{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := NewStaticHandler(e, "http://origin/empty")

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, hdr := range []string{"bytes=-1", "bytes=-10"} {
			t.Run(fmt.Sprintf("%s %s", method, hdr), func(t *testing.T) {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(method, "/empty", nil)
				req.Header.Set("Range", hdr)
				h.ServeHTTP(rec, req)

				if rec.Code != http.StatusRequestedRangeNotSatisfiable {
					t.Fatalf("status=%d, want 416 (an empty object cannot satisfy any range)", rec.Code)
				}
				if got := rec.Header().Get("Content-Range"); got != "bytes */0" {
					t.Fatalf("Content-Range = %q, want %q (RFC 7233 §4.4)", got, "bytes */0")
				}
			})
		}
	}
}
