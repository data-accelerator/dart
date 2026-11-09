package admin

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/data-accelerator/dart/internal/cluster"
)

func TestRoleDiagnostics(t *testing.T) {
	provider := cluster.NewStaticProvider(
		cluster.Member{ID: "A", Addr: "a:1", Weight: 1, Role: cluster.Ordinary, State: cluster.Ready},
		cluster.Member{ID: "B", Addr: "b:1", Weight: 1, Role: cluster.RootOnly, State: cluster.Ready},
		cluster.Member{ID: "C", Addr: "c:1", Weight: 100, Role: cluster.NonRoot, State: cluster.Ready},
		cluster.Member{ID: "D", Addr: "d:1", Weight: 100, Role: cluster.RootOnly, State: cluster.Suspect},
	)
	h := Handler(Options{Store: newStore(t), Cluster: provider, SelfID: "C", Role: cluster.NonRoot})
	_, body := get(t, h, "/admin/stats")
	var stats struct{ Role string }
	if err := json.Unmarshal([]byte(body), &stats); err != nil || stats.Role != "non-root" {
		t.Fatal("local role absent from stats", body, err)
	}
	_, body = get(t, h, "/admin/members")
	var members struct {
		Roots   int `json:"eligible_roots"`
		Readers int `json:"eligible_readers"`
		Members []struct{ ID, Role string }
	}
	if err := json.Unmarshal([]byte(body), &members); err != nil {
		t.Fatal(err)
	}
	if members.Roots != 2 || members.Readers != 2 || len(members.Members) != 4 {
		t.Fatal(body)
	}
	want := map[string]string{"A": "ordinary", "B": "root-only", "C": "non-root", "D": "root-only"}
	for _, m := range members.Members {
		if m.Role != want[m.ID] {
			t.Fatal(body)
		}
	}
	_, body = get(t, h, "/admin/ring?key=42")
	var ring struct{ Order []struct{ ID, Role string } }
	if err := json.Unmarshal([]byte(body), &ring); err != nil {
		t.Fatal(err)
	}
	if len(ring.Order) != 2 {
		t.Fatal("role or readiness ignored", body)
	}
	for _, m := range ring.Order {
		if (m.ID != "A" && m.ID != "B") || m.Role != want[m.ID] {
			t.Fatal(body)
		}
	}
	provider.Set([]cluster.Member{{ID: "C", Addr: "c:1", Role: cluster.NonRoot, State: cluster.Ready}})
	resp, body := get(t, h, "/admin/ring?key=42")
	if err := json.Unmarshal([]byte(body), &ring); err != nil || resp.StatusCode != http.StatusOK || len(ring.Order) != 0 {
		t.Fatal(body, err)
	}
	if resp, _ := get(t, h, "/healthz"); resp.StatusCode != http.StatusOK {
		t.Fatal("liveness depends on supply availability")
	}
}
