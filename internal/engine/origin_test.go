package engine

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
)

func TestNonRootOriginFailoverKeepsRefusalsTerminal(t *testing.T) {
	var calls [2]atomic.Int32
	var members []cluster.Member
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		i := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[i].Add(1)
			w.Header().Set(peer.HeaderOriginResponse, "1")
			http.Error(w, "origin refused", http.StatusForbidden)
		}))
		defer s.Close()
		servers[i] = s
		members = append(members, cluster.Member{ID: []string{"A", "B"}[i], Addr: s.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly})
	}
	prov := cluster.NewStaticProvider(members...)
	forbidden := &forbiddenOrigin{}
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: forbidden, Cluster: prov, Peer: peer.NewClient(), SelfID: "leaf", Role: cluster.NonRoot})
	if err != nil {
		t.Fatal(err)
	}
	const originURL = "http://unreachable-origin/object"
	oid, _ := chunk.ObjectID(originURL)
	order := hashring.Rank(e.fileKey(oid), prov.Current().Roots())
	primary := 0
	if order[0].ID == "B" {
		primary = 1
	}
	backup := 1 - primary
	client := &http.Client{Transport: e.OriginTransport()}
	get := func() {
		t.Helper()
		resp, err := client.Get(originURL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != 403 {
			t.Fatalf("origin status lost: %d", resp.StatusCode)
		}
	}
	get()
	if calls[primary].Load() != 1 || calls[backup].Load() != 0 {
		t.Fatal("definitive refusal triggered backup")
	}
	servers[primary].Close()
	get()
	if calls[backup].Load() != 1 {
		t.Fatal("unreachable root did not fall back")
	}
	prov.Set(nil)
	if resp, err := client.Get(originURL); err == nil {
		resp.Body.Close()
		t.Fatal("no-root operation succeeded")
	}
	if forbidden.calls.Load() != 0 {
		t.Fatal("non-root used its local origin fetcher")
	}
}

func TestNonRootPassthroughAbortsIncompleteResponse(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	defer origin.Close()
	root := httptest.NewServer(&peer.OriginServer{Transport: http.DefaultTransport})
	defer root.Close()
	prov := cluster.NewStaticProvider(cluster.Member{ID: "root", Addr: root.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly})
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: &forbiddenOrigin{}, Cluster: prov, Peer: peer.NewClient(), SelfID: "leaf", Role: cluster.NonRoot})
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := e.ServePassthrough(r.Context(), w, r, origin.URL); err != nil {
			http.Error(w, "upstream unavailable", 502)
		}
	}))
	defer front.Close()
	resp, err := http.Get(front.URL)
	if err == nil {
		defer resp.Body.Close()
		_, err = io.ReadAll(resp.Body)
	}
	if err == nil {
		t.Fatal("incomplete unknown-length response looked complete")
	}
}

func TestNonRootPassthroughCountsPeerBytes(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "hello") }))
	defer origin.Close()
	rootReg := metrics.NewRegistry()
	rootEngine, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(), Role: cluster.RootOnly, Metrics: NewMetrics(rootReg)})
	if err != nil {
		t.Fatal(err)
	}
	root := httptest.NewServer(&peer.OriginServer{Transport: rootEngine.OriginSource(http.DefaultTransport)})
	defer root.Close()
	prov := cluster.NewStaticProvider(cluster.Member{ID: "root", Addr: root.Listener.Addr().String(), Weight: 1, State: cluster.Ready, Role: cluster.RootOnly})
	reg := metrics.NewRegistry()
	e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: &forbiddenOrigin{}, Cluster: prov, Peer: peer.NewClient(), SelfID: "leaf", Role: cluster.NonRoot, Metrics: NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	if e.OriginSource(http.DefaultTransport) != nil {
		t.Fatal("non-root exposed an origin source")
	}
	r := httptest.NewRequest("GET", "http://client/read", nil)
	w := httptest.NewRecorder()
	if err := e.ServePassthrough(context.Background(), w, r, origin.URL); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	if err := reg.Render(&out); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`dart_bytes_total{direction="client"} 5`, `dart_bytes_total{direction="peer_in"} 5`, `dart_bytes_total{direction="origin_in"} 0`} {
		if !strings.Contains(out.String(), expected) {
			t.Fatalf("missing %s in %s", expected, out.String())
		}
	}
	out.Reset()
	if err := rootReg.Render(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `dart_bytes_total{direction="origin_in"} 5`) || !strings.Contains(out.String(), `dart_bytes_total{direction="client"} 0`) {
		t.Fatalf("root proxy ingress miscounted: %s", out.String())
	}
}
