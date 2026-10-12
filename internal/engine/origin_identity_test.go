package engine

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServeDistinctEscapedOrigins(t *testing.T) {
	for _, paths := range [][2]string{{"/a%2Fb", "/a/b"}, {"/a%1Fb", "/ab"}} {
		t.Run(paths[0], func(t *testing.T) {
			contents := map[string][]byte{paths[0]: []byte("first"), paths[1]: []byte("other")}
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				content, ok := contents[r.URL.EscapedPath()]
				if !ok {
					http.NotFound(w, r)
					return
				}
				http.ServeContent(w, r, "blob", time.Unix(0, 0), bytes.NewReader(content))
			}))
			defer origin.Close()
			e := newEngine(t, testCfg())
			for _, path := range paths {
				got := serve(t, e, origin.URL+path, 0, 4)
				if !bytes.Equal(got, contents[path]) {
					t.Fatalf("Serve(%s) = %q, want %q", path, got, contents[path])
				}
			}
		})
	}
}
