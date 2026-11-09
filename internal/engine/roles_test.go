package engine

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
)

type forbiddenOrigin struct{ calls atomic.Int64 }

func (f *forbiddenOrigin) Fetch(context.Context, string, int64, int64) (fetch.Range, error) {
	f.calls.Add(1)
	return fetch.Range{}, errors.New("origin egress forbidden")
}

func TestRolesColdReadAndRootFailover(t *testing.T) {
	content := blob(10)
	origin := countingOrigin(t, content, nil)
	prov := cluster.NewStaticProvider()
	roots := map[string]*httptest.Server{}
	calls := map[string]*atomic.Int64{}
	var members []cluster.Member
	for _, id := range []string{"A", "B"} {
		e, err := New(Options{Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(), Cluster: prov, Peer: peer.NewClient(), SelfID: id, Role: cluster.RootOnly})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.Handle(peer.MetadataPath, &peer.MetadataServer{Probe: e.ProbeSource()})
		count := &atomic.Int64{}
		calls[id] = count
		blocks := &peer.Server{NodeID: id, Borrow: e.PeerLeaseSource()}
		mux.Handle("/peer/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			if r.Header.Get(peer.HeaderSupply) != "1" {
				t.Error("root called without terminal supply mode")
			}
			blocks.ServeHTTP(w, r)
		}))
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		roots[id] = srv
		members = append(members, cluster.Member{ID: id, Addr: strings.TrimPrefix(srv.URL, "http://"), Weight: 1, Role: cluster.RootOnly, State: cluster.Ready})
	}
	members = append(members, cluster.Member{ID: "C", Addr: "unused:1", Weight: 1, Role: cluster.NonRoot, State: cluster.Ready})
	prov.Set(members)
	forbidden := &forbiddenOrigin{}
	st := openStoreAt(t)
	leaf, err := New(Options{Chunk: testCfg(), Store: st, Fetcher: forbidden, Cluster: prov, Peer: peer.NewClient(), SelfID: "C", Role: cluster.NonRoot})
	if err != nil {
		t.Fatal(err)
	}
	key := blockKeyFor(origin.URL)
	order := hashring.Rank(key.Chunk, prov.Current().Roots())
	read := func() {
		t.Helper()
		var b bytes.Buffer
		if err := leaf.Serve(context.Background(), &b, origin.URL, 0, 9); err != nil || !bytes.Equal(b.Bytes(), content) {
			t.Fatalf("read: %v %x", err, b.Bytes())
		}
	}
	read()
	if calls[order[0].ID].Load() != 1 || calls[order[1].ID].Load() != 0 {
		t.Fatal("owner miss queried another root")
	}
	roots[order[0].ID].Close()
	st.Delete(key)
	read()
	if calls[order[1].ID].Load() != 1 {
		t.Fatal("backup did not supply failed root's block")
	}
	roots[order[1].ID].Close()
	read() // cached metadata and bytes survive loss of all roots
	if forbidden.calls.Load() != 0 {
		t.Fatalf("non-root contacted origin %d times", forbidden.calls.Load())
	}
	st.Delete(key)
	if err := leaf.Serve(context.Background(), &bytes.Buffer{}, origin.URL, 0, 9); err == nil {
		t.Fatal("cold read succeeded with no roots")
	}
	if forbidden.calls.Load() != 0 {
		t.Fatal("failed root triggered non-root origin fallback")
	}
}
