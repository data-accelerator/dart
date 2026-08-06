package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/store"
)

func get(t *testing.T, h http.Handler, path string) (*http.Response, string) {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(body)
}

func newStore(t *testing.T) *store.DiskStore {
	t.Helper()
	s, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "b.dat"), SlotSize: 16, Slots: 8})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestHealthz(t *testing.T) {
	resp, body := get(t, Handler(Options{}), "/healthz")
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(body) != "ok" {
		t.Errorf("healthz = %d %q", resp.StatusCode, body)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	r := metrics.NewRegistry()
	r.NewCounter("dart_test_total", "test counter").Add(7)
	resp, body := get(t, Handler(Options{Registry: r}), "/metrics")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q", ct)
	}
	if !strings.Contains(body, "dart_test_total 7") {
		t.Errorf("metrics body:\n%s", body)
	}
}

func TestMetricsUnconfigured(t *testing.T) {
	resp, _ := get(t, Handler(Options{}), "/metrics")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestStats(t *testing.T) {
	st := newStore(t)
	_ = st.Put(store.BlockKey{Chunk: 1, Block: 0}, []byte("abc"))
	_ = st.Put(store.BlockKey{Chunk: 2, Block: 0}, []byte("def"))

	resp, body := get(t, Handler(Options{Store: st, SelfID: "N1"}), "/admin/stats")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("json: %v (%s)", err, body)
	}
	if got["self_id"] != "N1" {
		t.Errorf("self_id = %v", got["self_id"])
	}
	if n, _ := got["cached_blocks"].(float64); n != 2 {
		t.Errorf("cached_blocks = %v, want 2", got["cached_blocks"])
	}
}

func TestStatsUnconfigured(t *testing.T) {
	resp, _ := get(t, Handler(Options{}), "/admin/stats")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestMembers(t *testing.T) {
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: "A", Addr: "10.0.0.1:9000", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: "B", Addr: "10.0.0.2:9000", Weight: 2, State: cluster.Suspect},
	)
	resp, body := get(t, Handler(Options{Cluster: prov, SelfID: "A"}), "/admin/members")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got struct {
		SelfID  string `json:"self_id"`
		Epoch   string `json:"epoch"`
		Members []struct {
			ID, Addr, State string
			Weight          float64
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("json: %v (%s)", err, body)
	}
	if got.SelfID != "A" || got.Epoch == "" || len(got.Members) != 2 {
		t.Fatalf("members payload = %+v", got)
	}
	if got.Members[0].ID != "A" || got.Members[0].State != "Ready" {
		t.Errorf("member A = %+v", got.Members[0])
	}
	if got.Members[1].State != "Suspect" || got.Members[1].Weight != 2 {
		t.Errorf("member B = %+v", got.Members[1])
	}
}

func TestMembersUnconfigured(t *testing.T) {
	resp, _ := get(t, Handler(Options{}), "/admin/members")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestRing(t *testing.T) {
	prov := cluster.NewStaticProvider(
		cluster.Member{ID: "A", Addr: "a:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: "B", Addr: "b:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: "C", Addr: "c:1", Weight: 1, State: cluster.Ready},
		cluster.Member{ID: "D", Addr: "d:1", Weight: 1, State: cluster.Leaving}, // excluded
	)
	h := Handler(Options{Cluster: prov})

	resp, body := get(t, h, "/admin/ring?key=42")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d (%s)", resp.StatusCode, body)
	}
	var got struct {
		Key   string `json:"key"`
		Order []struct {
			Rank     int
			ID, Addr string
		} `json:"order"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("json: %v (%s)", err, body)
	}
	if got.Key != "42" {
		t.Errorf("key = %q", got.Key)
	}
	if len(got.Order) != 3 { // only Ready members
		t.Fatalf("order len = %d, want 3 (Ready only): %+v", len(got.Order), got.Order)
	}
	for i, e := range got.Order {
		if e.Rank != i {
			t.Errorf("entry %d rank = %d", i, e.Rank)
		}
		if e.Addr == "" {
			t.Errorf("entry %d missing addr", i)
		}
	}

	// n limits the returned prefix.
	_, body2 := get(t, h, "/admin/ring?key=42&n=1")
	var got2 struct {
		Order []map[string]any `json:"order"`
	}
	_ = json.Unmarshal([]byte(body2), &got2)
	if len(got2.Order) != 1 {
		t.Errorf("n=1 order len = %d", len(got2.Order))
	}

	// Deterministic: same key yields the same order.
	_, body3 := get(t, h, "/admin/ring?key=42")
	if body3 != body {
		t.Error("ring order is not deterministic for the same key")
	}
}

func TestRingBadKey(t *testing.T) {
	prov := cluster.NewStaticProvider(cluster.Member{ID: "A", Addr: "a:1", Weight: 1, State: cluster.Ready})
	h := Handler(Options{Cluster: prov})
	for _, q := range []string{"/admin/ring", "/admin/ring?key=", "/admin/ring?key=abc", "/admin/ring?key=-1"} {
		resp, _ := get(t, h, q)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestRingUnconfigured(t *testing.T) {
	resp, _ := get(t, Handler(Options{}), "/admin/ring?key=1")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}
