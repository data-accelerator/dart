package engine

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// dartServer wires: origin -> engine -> DART Handler -> httptest server.
func dartServer(t *testing.T, content []byte) *httptest.Server {
	t.Helper()
	origin := countingOrigin(t, content, nil)
	e := newEngine(t, testCfg())
	h := NewStaticHandler(e, origin.URL)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// getRange issues a GET (optionally with a Range header) and returns the
// response and body.
func getRange(t *testing.T, url, rangeHdr string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %q: %v", rangeHdr, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body
}

// assertNonChunked verifies the response used Content-Length framing, not
// chunked transfer encoding (a hard requirement for client reads).
func assertNonChunked(t *testing.T, resp *http.Response, wantLen int64) {
	t.Helper()
	if len(resp.TransferEncoding) != 0 {
		t.Errorf("response is chunked (Transfer-Encoding=%v), want Content-Length framing", resp.TransferEncoding)
	}
	if resp.ContentLength != wantLen {
		t.Errorf("ContentLength = %d, want %d", resp.ContentLength, wantLen)
	}
}

func TestHandlerFull(t *testing.T) {
	content := blob(100)
	srv := dartServer(t, content)
	resp, body := getRange(t, srv.URL, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(body, content) {
		t.Errorf("body mismatch (%d bytes)", len(body))
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("missing Accept-Ranges: bytes")
	}
	assertNonChunked(t, resp, 100)
}

func TestHandlerRange(t *testing.T) {
	content := blob(100)
	srv := dartServer(t, content)
	resp, body := getRange(t, srv.URL, "bytes=20-45")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 20-45/100" {
		t.Errorf("Content-Range = %q, want bytes 20-45/100", cr)
	}
	if !bytes.Equal(body, content[20:46]) {
		t.Errorf("range body mismatch")
	}
	assertNonChunked(t, resp, 26)
}

func TestHandlerSuffix(t *testing.T) {
	content := blob(100)
	srv := dartServer(t, content)
	resp, body := getRange(t, srv.URL, "bytes=-10")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, content[90:100]) {
		t.Errorf("suffix body mismatch")
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 90-99/100" {
		t.Errorf("Content-Range = %q, want bytes 90-99/100", cr)
	}
}

func TestHandlerOpenEnded(t *testing.T) {
	content := blob(100)
	srv := dartServer(t, content)
	resp, body := getRange(t, srv.URL, "bytes=50-")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, content[50:100]) {
		t.Errorf("open-ended body mismatch")
	}
}

func TestHandler416(t *testing.T) {
	content := blob(100)
	srv := dartServer(t, content)
	resp, _ := getRange(t, srv.URL, "bytes=200-300")
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", resp.StatusCode)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes */100" {
		t.Errorf("Content-Range = %q, want bytes */100", cr)
	}
}

func TestHandlerHEAD(t *testing.T) {
	content := blob(100)
	srv := dartServer(t, content)
	req, _ := http.NewRequest(http.MethodHead, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("HEAD body = %d bytes, want 0", len(body))
	}
	if resp.ContentLength != 100 {
		t.Errorf("HEAD ContentLength = %d, want 100", resp.ContentLength)
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	content := blob(10)
	srv := dartServer(t, content)
	resp, err := http.Post(srv.URL, "text/plain", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestParseRange(t *testing.T) {
	const size = 100
	cases := []struct {
		hdr                 string
		wantStart, wantEnd  int64
		wantIsRange, wantOK bool
	}{
		{"", 0, 99, false, true},
		{"bytes=0-9", 0, 9, true, true},
		{"bytes=20-45", 20, 45, true, true},
		{"bytes=50-", 50, 99, true, true},
		{"bytes=-10", 90, 99, true, true},
		{"bytes=90-999", 90, 99, true, true},    // end clamped
		{"bytes=200-300", 0, 0, true, false},    // start beyond size
		{"bytes=10-5", 0, 0, true, false},       // inverted
		{"bytes=0-9,20-29", 0, 0, false, false}, // multi-range unsupported
		{"items=0-9", 0, 0, false, false},       // wrong unit
		{"garbage", 0, 0, false, false},
	}
	for _, c := range cases {
		s, e, isR, ok := parseRange(c.hdr, size)
		if ok != c.wantOK || isR != c.wantIsRange || (ok && (s != c.wantStart || e != c.wantEnd)) {
			t.Errorf("parseRange(%q) = (%d,%d,isRange=%v,ok=%v), want (%d,%d,%v,%v)",
				c.hdr, s, e, isR, ok, c.wantStart, c.wantEnd, c.wantIsRange, c.wantOK)
		}
	}
}

func TestParseRangeEdge(t *testing.T) {
	if _, _, _, ok := parseRange("", 0); ok {
		t.Error("empty object full range should be unsatisfiable")
	}
	// Suffix larger than the object clamps to the whole object.
	if s, e, _, ok := parseRange("bytes=-999", 100); !ok || s != 0 || e != 99 {
		t.Errorf("suffix clamp = (%d,%d,ok=%v), want (0,99,true)", s, e, ok)
	}
}

func TestHandlerResolveError(t *testing.T) {
	e := newEngine(t, testCfg())
	h := &Handler{E: e, Resolve: func(*http.Request) (string, error) { return "", errors.New("nope") }}
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, _ := getRange(t, srv.URL, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandlerOriginError(t *testing.T) {
	e := newEngine(t, testCfg())
	// Port 1 is closed: the size probe fails -> 502.
	h := NewStaticHandler(e, "http://127.0.0.1:1/x")
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, _ := getRange(t, srv.URL, "")
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}
