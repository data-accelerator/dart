package peer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestServerHopValidation pins the X-DART-Hop domain on both peer server
// implementations (buffered Server and cut-through StreamServer): the header is
// optional (absent = depth 0) and any non-negative decimal is accepted — the
// relay-depth bound itself is enforced by the engine, not the transport — but a
// malformed, negative, or out-of-range value is rejected with 400 before the
// source is ever invoked. A negative hop used to sail through: the engine only
// declines at hop >= maxHop and every relay increments the value, so a negative
// start delayed the loop-safety cutoff (a huge negative one effectively
// forever) exactly when membership skew creates a relay cycle.
func TestServerHopValidation(t *testing.T) {
	blockPath := "/peer/v1/block/abcdef/7"

	servers := map[string]func(record func(hop int)) http.Handler{
		"buffered": func(record func(hop int)) http.Handler {
			return &Server{NodeID: "n", Src: func(_ context.Context, req BlockRequest) ([]byte, bool, error) {
				record(req.Hop)
				return []byte("data"), true, nil
			}}
		},
		"stream": func(record func(hop int)) http.Handler {
			return &StreamServer{NodeID: "n", Src: func(_ context.Context, req BlockRequest, w io.Writer, sizer func(int64)) (int64, bool, error) {
				record(req.Hop)
				sizer(4)
				n, err := w.Write([]byte("data"))
				return int64(n), true, err
			}}
		},
	}

	cases := []struct {
		name       string
		hop        string // header value; set=false means the header is absent
		set        bool
		wantStatus int
		wantHop    int // hop the source must observe (valid cases only)
	}{
		{"absent means depth zero", "", false, http.StatusOK, 0},
		{"zero", "0", true, http.StatusOK, 0},
		{"below relay bound", "63", true, http.StatusOK, 63},
		// The transport accepts any non-negative depth; declining at the relay
		// bound (maxHop) is the engine's job, so 64 and beyond are still valid
		// wire values here.
		{"at relay bound", "64", true, http.StatusOK, 64},
		{"large", "4096", true, http.StatusOK, 4096},
		{"negative one", "-1", true, http.StatusBadRequest, 0},
		{"negative large", "-4096", true, http.StatusBadRequest, 0},
		{"min int64", "-9223372036854775808", true, http.StatusBadRequest, 0},
		{"overflow", "9223372036854775808", true, http.StatusBadRequest, 0},
		{"non-numeric", "abc", true, http.StatusBadRequest, 0},
		{"float", "1.5", true, http.StatusBadRequest, 0},
		{"leading space", " 1", true, http.StatusBadRequest, 0},
		{"grouped digits", "1_000", true, http.StatusBadRequest, 0},
	}

	for srvName, build := range servers {
		for _, tc := range cases {
			t.Run(srvName+"/"+tc.name, func(t *testing.T) {
				var calls int
				var gotHop int
				h := build(func(hop int) { calls++; gotHop = hop })

				r := httptest.NewRequest(http.MethodGet, blockPath, nil)
				if tc.set {
					r.Header.Set(HeaderHop, tc.hop)
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, r)

				if rec.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantStatus, rec.Body.String())
				}
				if tc.wantStatus == http.StatusOK {
					if calls != 1 {
						t.Fatalf("source called %d times, want 1", calls)
					}
					if gotHop != tc.wantHop {
						t.Errorf("source observed hop %d, want %d", gotHop, tc.wantHop)
					}
				} else if calls != 0 {
					t.Errorf("source called %d times on a rejected request, want 0", calls)
				}
			})
		}
	}
}

// TestParseHop exercises the decoder directly, including the values strconv
// accepts that the wire never produces (a leading plus).
func TestParseHop(t *testing.T) {
	cases := []struct {
		in      string
		wantHop int
		wantOK  bool
	}{
		{"", 0, true},
		{"0", 0, true},
		{"7", 7, true},
		{"+5", 5, true}, // strconv.Atoi semantics; harmless and accepted
		{"-1", 0, false},
		{"-0", 0, true}, // Atoi("-0") == 0, which is in domain
		{"abc", 0, false},
		{"9223372036854775808", 0, false},
	}
	for _, tc := range cases {
		hop, ok := parseHop(tc.in)
		if ok != tc.wantOK || (ok && hop != tc.wantHop) {
			t.Errorf("parseHop(%q) = (%d, %v), want (%d, %v)", tc.in, hop, ok, tc.wantHop, tc.wantOK)
		}
	}
}
