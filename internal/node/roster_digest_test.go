package node

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/peer"
)

func TestConditionalRosterOverHTTP(t *testing.T) {
	srv := httptest.NewUnstartedServer(nil)
	addr := srv.Listener.Addr().String()
	self := cluster.Member{ID: "s", Addr: addr, Weight: 3, Role: cluster.RootOnly}
	caller := cluster.Member{ID: "c", Addr: "c:1", Weight: 1, Role: cluster.NonRoot}
	other := cluster.Member{ID: "third", Addr: "old:1", Weight: 2}
	d := cluster.NewDynamicProvider(cluster.DynamicConfig{Self: self, Seeder: cluster.StaticSeeder{}})
	ctx := context.Background()
	d.LearnMember(caller)
	d.LearnMember(other)
	d.Refresh(ctx)
	inbound := atomic.Int32{}
	srv.Config.Handler = &peer.RosterServer{NodeID: self.ID, Select: func(tag string) peer.Roster { return selectRoster(d, tag) }, LearnMember: func(m peer.RosterMember) {
		if m.ID == caller.ID && m.Role == caller.Role && m.Addr == caller.Addr {
			inbound.Add(1)
		}
	}}
	srv.Start()
	defer srv.Close()
	f := rosterFetcher{c: peer.NewClient(), selfID: caller.ID, selfAddr: caller.Addr, selfRole: caller.Role}
	defer f.c.HTTP.CloseIdleConnections()
	check := func(want int) []cluster.Member {
		ms, id, err := f.FetchRoster(ctx, addr)
		if err != nil || id != self.ID || len(ms) != want {
			t.Fatalf("got %d members, responder %q, error %v; want %d members from %q", len(ms), id, err, want, self.ID)
		}
		return ms
	}
	check(3) // diagnostic/older v2 caller
	same := d.Current()
	f.current = func() *cluster.View { return same }
	if m := check(1)[0]; m.ID != self.ID || m.Role != self.Role || m.Weight != self.Weight || m.Addr != addr {
		t.Fatalf("incomplete direct evidence: %+v", m)
	}
	other.Addr = "new:2"
	d.LearnMember(other)
	d.Refresh(ctx)
	if d.Current().Epoch() != same.Epoch() {
		t.Fatal("test requires same placement epoch")
	}
	ms := check(3)
	if ms[2].ID != other.ID || ms[2].Addr != other.Addr {
		t.Fatalf("missed new routing address: %+v", ms)
	}
	// A restarted observer sends its own smaller current view even if this
	// server's membership did not change since the previous full response.
	f.current = func() *cluster.View { return cluster.NewView([]cluster.Member{caller}) }
	check(3)
	if got := inbound.Load(); got != 4 {
		t.Fatalf("inbound direct contacts = %d, want 4", got)
	}
	legacy := httptest.NewServer(&peer.RosterServer{NodeID: self.ID, Src: func() peer.Roster { return rosterOf(d) }})
	defer legacy.Close()
	f.current = d.Current
	ms, _, err := f.FetchRoster(ctx, legacy.Listener.Addr().String())
	if err != nil || len(ms) != 3 {
		t.Fatalf("v2 server ignoring hint: %v %v", ms, err)
	}
}

type rosterBenchConn struct {
	net.Conn
	in, out *atomic.Int64
}

func (c rosterBenchConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.in.Add(int64(n))
	return n, err
}
func (c rosterBenchConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.out.Add(int64(n))
	return n, err
}

// BenchmarkConvergedRoster measures a pooled HTTP/1.1 exchange including
// serialization, parsing and socket bytes in both directions. In one stable
// cluster round there are N*(N-1) such exchanges.
func BenchmarkConvergedRoster(b *testing.B) {
	for _, size := range []int{8, 64, 256} {
		for _, mode := range []string{"full", "matching"} {
			b.Run(fmt.Sprintf("%s/%d", mode, size), func(b *testing.B) {
				self := cluster.Member{ID: "n0", Addr: "n0:9000", Weight: 1}
				d := cluster.NewDynamicProvider(cluster.DynamicConfig{Self: self, Seeder: cluster.StaticSeeder{}})
				for i := 1; i < size; i++ {
					id := fmt.Sprintf("n%d", i)
					d.LearnMember(cluster.Member{ID: id, Addr: id + ":9000", Weight: 1})
				}
				d.Refresh(context.Background())
				srv := httptest.NewServer(&peer.RosterServer{NodeID: self.ID, Select: func(h string) peer.Roster { return selectRoster(d, h) }})
				defer srv.Close()
				c := peer.NewClient()
				defer c.HTTP.CloseIdleConnections()
				tr := peer.NewTransport(0, 0)
				dial := tr.DialContext
				var in, out atomic.Int64
				tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
					conn, err := dial(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					return rosterBenchConn{conn, &in, &out}, nil
				}
				c.HTTP.Transport = tr
				f := rosterFetcher{c: c, selfID: "n1", selfAddr: "n1:9000"}
				if mode == "matching" {
					f.current = d.Current
				}
				addr := srv.Listener.Addr().String()
				ctx := context.Background()
				if _, _, err := f.FetchRoster(ctx, addr); err != nil {
					b.Fatal(err)
				}
				in.Store(0)
				out.Store(0)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := f.FetchRoster(ctx, addr); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(in.Load()+out.Load())/float64(b.N), "wire-B/op")
			})
		}
	}
}

// The observer only receives outbound replies. Inbound liveness credit from a
// peer would mask a compact reply that omitted the responder's self-report.
func TestCompactRosterKeepsLiveRespondersAndExpiresDepartures(t *testing.T) {
	ctx := context.Background()
	observer := cluster.Member{ID: "observer", Addr: "observer:9000", Weight: 1}
	b := httptest.NewUnstartedServer(nil)
	c := httptest.NewUnstartedServer(nil)
	second := cluster.Member{ID: "b", Addr: b.Listener.Addr().String(), Weight: 1}
	third := cluster.Member{ID: "c", Addr: c.Listener.Addr().String(), Weight: 1}
	start := func(srv *httptest.Server, self, other cluster.Member) *atomic.Int32 {
		sender := cluster.NewDynamicProvider(cluster.DynamicConfig{Self: self, Seeder: cluster.StaticSeeder{}})
		sender.LearnMember(observer)
		sender.LearnMember(other)
		sender.Refresh(ctx)
		var compact atomic.Int32
		srv.Config.Handler = &peer.RosterServer{NodeID: self.ID, Select: func(digest string) peer.Roster {
			r := selectRoster(sender, digest)
			if len(r.Members) == 1 {
				compact.Add(1)
			}
			return r
		}}
		srv.Start()
		t.Cleanup(srv.Close)
		return &compact
	}
	bCompact := start(b, second, third)
	cCompact := start(c, third, second)
	now := time.Unix(100, 0)
	const forget = 3 * time.Second
	f := &rosterFetcher{c: peer.NewClient(), selfID: observer.ID, selfAddr: observer.Addr}
	t.Cleanup(f.c.HTTP.CloseIdleConnections)
	d := cluster.NewDynamicProvider(cluster.DynamicConfig{
		Self: observer, Seeder: cluster.StaticSeeder{second.Addr, third.Addr}, Fetcher: f,
		Now: func() time.Time { return now }, ForgetAfter: forget,
	})
	f.current = d.Current
	if v := d.Refresh(ctx); v.Len() != 3 {
		t.Fatalf("initial view = %v", v.Members())
	}
	for i := 0; i < 8; i++ {
		now = now.Add(time.Second)
		v := d.Refresh(ctx)
		if v.Len() != 3 {
			t.Fatalf("live responder expired at step %d: %v", i, v.Members())
		}
	}
	if bCompact.Load() != 8 || cCompact.Load() != 8 {
		t.Fatalf("expected eight compact replies per peer, got %d and %d", bCompact.Load(), cCompact.Load())
	}
	c.Close()
	now = now.Add(forget + time.Second)
	for i := 0; i < 3; i++ {
		// b continues to advertise c. Once the views differ, its full reply
		// may rediscover c as a candidate, but cannot confirm it.
		v := d.Refresh(ctx)
		if _, ok := v.Get(second.ID); !ok || v.Len() != 2 {
			t.Fatalf("post-departure view = %v", v.Members())
		}
		now = now.Add(time.Second)
	}
}
