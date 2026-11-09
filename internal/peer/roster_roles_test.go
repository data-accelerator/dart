package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
)

func TestRosterRolesBothDirections(t *testing.T) {
	incoming := make(chan RosterMember, 1)
	addr, close := rosterServer(t, &RosterServer{NodeID: "root", Role: cluster.RootOnly,
		Src: func() Roster {
			return Roster{Members: []RosterMember{{ID: "other", Addr: "other:1", Role: cluster.NonRoot}}}
		},
		LearnMember: func(m RosterMember) { incoming <- m },
	})
	defer close()
	r, id, err := NewClient().FetchRosterMember(context.Background(), addr, RosterMember{ID: "leaf", Addr: "leaf:1", Weight: 2, Role: cluster.NonRoot})
	if err != nil {
		t.Fatal(err)
	}
	m := <-incoming
	if m.Role != cluster.NonRoot || m.Weight != 2 || m.ID != "leaf" {
		t.Fatalf("caller report: %+v", m)
	}
	if id != "root" {
		t.Fatal(id)
	}
	var root RosterMember
	for _, m := range r.Members {
		if m.ID == id {
			root = m
		}
	}
	if root.Role != cluster.RootOnly || root.Addr != addr {
		t.Fatalf("missing-body self role lost: %+v", root)
	}
}

func TestRosterRejectsUnknownRoles(t *testing.T) {
	for _, raw := range []string{"3", "-1", "256", "non-root"} {
		t.Run(raw, func(t *testing.T) {
			learned := false
			s := &RosterServer{Src: func() Roster { return Roster{} }, LearnMember: func(RosterMember) { learned = true }}
			req := httptest.NewRequest("GET", RosterPath, nil)
			req.Header.Set(HeaderNode, "caller")
			req.Header.Set(HeaderPeerAddr, "caller:1")
			req.Header.Set(HeaderPeerRole, raw)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest || learned {
				t.Fatalf("unknown caller accepted: %d", w.Code)
			}
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"members":[{"id":"bad","addr":"bad:1","role":3}]}`))
	}))
	defer srv.Close()
	if _, _, err := NewClient().FetchRoster(context.Background(), strings.TrimPrefix(srv.URL, "http://"), "", ""); err == nil {
		t.Fatal("unknown response role accepted")
	}
}
