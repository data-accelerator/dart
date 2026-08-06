package peer

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/data-accelerator/dart/internal/store"
)

func newStore(t *testing.T) *store.DiskStore {
	t.Helper()
	s, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "b.dat"), SlotSize: 4096, Slots: 64})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// addrOf strips the scheme from an httptest server URL, leaving host:port.
func addrOf(t *testing.T, srvURL string) string {
	t.Helper()
	return strings.TrimPrefix(srvURL, "http://")
}

func TestServerClientRoundtrip(t *testing.T) {
	st := newStore(t)
	key := store.BlockKey{Chunk: 0xABCDEF, Block: 7}
	want := bytes.Repeat([]byte{0x5A}, 1000)
	if err := st.Put(key, want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	srv := httptest.NewServer(&Server{NodeID: "node-a", Src: StoreSource(st)})
	defer srv.Close()
	c := NewClient()

	got, held, err := c.Get(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: key})
	if err != nil || !held {
		t.Fatalf("Get held=%v err=%v", held, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("block bytes mismatch (%d vs %d)", len(got), len(want))
	}
}

func TestClientMiss(t *testing.T) {
	st := newStore(t)
	srv := httptest.NewServer(&Server{Src: StoreSource(st)})
	defer srv.Close()
	c := NewClient()
	_, held, err := c.Get(context.Background(), addrOf(t, srv.URL), BlockRequest{Key: store.BlockKey{Chunk: 1, Block: 2}})
	if err != nil || held {
		t.Errorf("miss: held=%v err=%v, want held=false, err=nil", held, err)
	}
}

func TestServerNodeHeader(t *testing.T) {
	st := newStore(t)
	key := store.BlockKey{Chunk: 3, Block: 4}
	_ = st.Put(key, []byte("hello"))
	srv := httptest.NewServer(&Server{NodeID: "node-x", Src: StoreSource(st)})
	defer srv.Close()

	resp, err := http.Get(blockURL(addrOf(t, srv.URL), key))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.Header.Get(HeaderNode) != "node-x" {
		t.Errorf("X-DART-Node = %q, want node-x", resp.Header.Get(HeaderNode))
	}
}

func TestServerBadPathAndMethod(t *testing.T) {
	st := newStore(t)
	srv := httptest.NewServer(&Server{Src: StoreSource(st)})
	defer srv.Close()
	base := srv.URL

	bad := []string{
		base + "/peer/v1/block/",            // no chunk/index
		base + "/peer/v1/block/xyz/1",       // bad hex
		base + "/peer/v1/block/ab/notanint", // bad index
		base + "/wrong/path",                // wrong prefix
	}
	for _, u := range bad {
		resp, err := http.Get(u)
		if err != nil {
			t.Fatalf("GET %s: %v", u, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 400/404", u, resp.StatusCode)
		}
	}

	// Non-GET method.
	resp, err := http.Post(base+"/peer/v1/block/ab/1", "text/plain", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", resp.StatusCode)
	}
}

func TestServerSourceError(t *testing.T) {
	srv := httptest.NewServer(&Server{Src: func(context.Context, BlockRequest) ([]byte, bool, error) {
		return nil, false, errors.New("boom")
	}})
	defer srv.Close()
	resp, err := http.Get(blockURL(addrOf(t, srv.URL), store.BlockKey{Chunk: 1, Block: 1}))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

func TestClientConnError(t *testing.T) {
	c := NewClient()
	_, _, err := c.Get(context.Background(), "127.0.0.1:1", BlockRequest{Key: store.BlockKey{Chunk: 1, Block: 1}})
	if err == nil {
		t.Error("expected connection error to a closed port")
	}
}

func TestParseBlockPath(t *testing.T) {
	ok := map[string]store.BlockKey{
		"/peer/v1/block/0/0":                                   {Chunk: 0, Block: 0},
		"/peer/v1/block/abcdef/7":                              {Chunk: 0xABCDEF, Block: 7},
		"/peer/v1/block/ffffffffffffffff/18446744073709551615": {Chunk: ^uint64(0), Block: ^uint64(0)},
	}
	for p, want := range ok {
		if got, o := parseBlockPath(p); !o || got != want {
			t.Errorf("parseBlockPath(%q) = (%+v, %v), want %+v", p, got, o, want)
		}
	}
	bad := []string{"/peer/v1/block/", "/peer/v1/block/ab", "/peer/v1/block//1", "/x/ab/1", "/peer/v1/block/ab/1/2"}
	for _, p := range bad {
		if _, o := parseBlockPath(p); o {
			t.Errorf("parseBlockPath(%q) = ok, want !ok", p)
		}
	}
}

// TestConcurrent exercises the pooled client and server under load (-race).
func TestConcurrent(t *testing.T) {
	st := newStore(t)
	const n = 20
	for i := 0; i < n; i++ {
		_ = st.Put(store.BlockKey{Chunk: uint64(i), Block: 0}, bytes.Repeat([]byte{byte(i)}, 128))
	}
	srv := httptest.NewServer(&Server{NodeID: "n", Src: StoreSource(st)})
	defer srv.Close()
	addr := addrOf(t, srv.URL)
	c := NewClient()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				got, held, err := c.Get(context.Background(), addr, BlockRequest{Key: store.BlockKey{Chunk: uint64(i), Block: 0}})
				if err != nil || !held || len(got) != 128 {
					t.Errorf("Get(%d): held=%v err=%v len=%d", i, held, err, len(got))
					return
				}
			}
		}()
	}
	wg.Wait()
}
