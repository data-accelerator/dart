package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPFetchIntoValidation(t *testing.T) {
	content := []byte("0123456789abcdefghijklmnopqrstuv")
	for _, tc := range []struct {
		name             string
		status           int
		start, end       int64
		body             int
		cr               string
		length           int
		wantErr, ignored bool
	}{
		{"complete", 206, 0, 15, 16, "bytes 0-15/32", 16, false, false},
		{"short", 206, 0, 15, 15, "bytes 0-15/32", 15, true, false},
		{"oversized", 206, 0, 15, 17, "bytes 0-15/32", 17, true, false},
		{"wrong-offset", 206, 0, 15, 16, "bytes 1-16/32", 16, true, false},
		{"clamped-tail", 206, 0, 31, 16, "bytes 0-15/16", 16, false, false},
		{"truncated", 206, 0, 15, 8, "bytes 0-15/32", 16, true, false},
		{"chunked-complete", 206, 0, 15, 16, "bytes 0-15/32", -1, false, false},
		{"chunked-oversized", 206, 0, 15, 17, "bytes 0-15/32", -1, true, false},
		{"ignored", 200, 8, 23, 32, "", 32, false, true},
		{"refused", 403, 0, 15, 0, "", 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.Header.Get("Range") != fmt.Sprintf("bytes=%d-%d", tc.start, tc.end) {
					t.Error("request geometry")
				}
				if tc.cr != "" {
					w.Header().Set("Content-Range", tc.cr)
				}
				if tc.length >= 0 {
					w.Header().Set("Content-Length", fmt.Sprint(tc.length))
				}
				w.WriteHeader(tc.status)
				if tc.length < 0 {
					w.(http.Flusher).Flush()
				}
				w.Write(content[:tc.body])
			}))
			defer server.Close()
			dst := make([]byte, tc.end-tc.start+1)
			result, err := (&HTTPFetcher{}).FetchInto(context.Background(), server.URL, tc.start, tc.end, dst)
			if (err != nil) != tc.wantErr {
				t.Fatalf("result %+v err %v", result, err)
			}
			if err == nil {
				if len(result.Data) > 0 && &result.Data[0] != &dst[0] {
					t.Fatal("did not use provided storage")
				}
				if result.RangeIgnored != tc.ignored {
					t.Fatal("range capability changed")
				}
				want := content[:len(result.Data)]
				if tc.ignored {
					want = content[tc.start : tc.end+1]
				}
				if !bytes.Equal(result.Data, want) {
					t.Fatal("wrong bytes")
				}
			}
		})
	}
}

func TestFetchIntoNestedTimeoutAndFallback(t *testing.T) {
	started, exited := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(exited) }))
	defer server.Close()
	f := &Coalescing{F: &Coalescing{F: &HTTPFetcher{}}, MaxFlight: 100 * time.Millisecond}
	if _, err := f.FetchInto(context.Background(), server.URL, 0, 15, make([]byte, 16)); err == nil {
		t.Fatal("fill escaped timeout")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("backend not called")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("backend not canceled")
	}
	if _, err := (&HTTPFetcher{}).FetchInto(context.Background(), server.URL, -1, 15, make([]byte, 16)); err == nil {
		t.Fatal("whole-object geometry accepted")
	}
}

func TestReadBoundedWindows(t *testing.T) {
	for _, n := range []int{0, 3, 4, 5} {
		dst := make([]byte, 4)
		data, err := ReadBounded(bytes.NewReader(bytes.Repeat([]byte{1}, n)), dst)
		if n > 4 {
			if !errors.Is(err, ErrBodyTooLong) {
				t.Fatal(err)
			}
		} else if err != nil || len(data) != n {
			t.Fatal(n, len(data), err)
		}
	}
	_, err := ReadBounded(errorReader{}, make([]byte, 8))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestFetchIntoCompatibilityOwnership(t *testing.T) {
	source := []byte("immutable")
	original := append([]byte(nil), source...)
	for _, tc := range []struct {
		name     string
		capacity int
		fail     bool
	}{
		{"copy", 16, false}, {"oversize", 4, false}, {"fetch-error", 16, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := errors.New("origin failed")
			f := bufferFetcher(func(context.Context, string, int64, int64) (Range, error) {
				if tc.fail {
					return Range{}, failure
				}
				return Range{Data: source, Total: 99, Coalesced: true}, nil
			})
			dst := make([]byte, tc.capacity)
			r, err := FetchInto(context.Background(), f, "url", 0, 8, dst)
			if tc.fail {
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				return
			}
			if tc.capacity < len(source) {
				if !errors.Is(err, ErrBodyTooLong) {
					t.Fatal(err)
				}
				return
			}
			if err != nil || !r.Coalesced || r.Total != 99 || &r.Data[0] != &dst[0] {
				t.Fatal("metadata/storage mismatch", r, err)
			}
			dst[0] = 'X'
			if !bytes.Equal(source, original) {
				t.Fatal("copied result retained shared mutation")
			}
		})
	}
}

type bufferFetcher func(context.Context, string, int64, int64) (Range, error)

func (f bufferFetcher) Fetch(ctx context.Context, u string, s, e int64) (Range, error) {
	return f(ctx, u, s, e)
}
