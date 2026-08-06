// Package admin exposes DART's observability and introspection endpoints: the
// Prometheus scrape endpoint plus a few read-only JSON views for debugging
// placement and membership.
//
//	GET /metrics         Prometheus text exposition format
//	GET /healthz         liveness ("ok")
//	GET /admin/stats     store/cache counters as JSON
//	GET /admin/members   current membership view as JSON
//	GET /admin/ring?key=<chunkKey>&n=<topN>   HRW placement order for a key
//
// It is intended for a separate, non-public listener (see cmd/dart -admin).
package admin

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/data-accelerator/dart/internal/cluster"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/store"
)

// Options configures the admin handler. All fields are optional: an endpoint
// whose dependency is nil reports 503 instead of failing to start.
type Options struct {
	// Registry backs GET /metrics.
	Registry *metrics.Registry
	// Store backs GET /admin/stats.
	Store store.Store
	// Cluster backs GET /admin/members and /admin/ring.
	Cluster cluster.Provider
	// SelfID is reported in /admin/members to identify this node.
	SelfID string
}

// Handler returns an http.ServeMux with the admin endpoints registered. A
// ServeMux is safe here: none of these paths carry embedded URLs, so path
// cleaning is harmless (unlike the client data path).
func Handler(opt Options) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if opt.Registry == nil {
			http.Error(w, "metrics not configured", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := opt.Registry.Render(w); err != nil {
			http.Error(w, "render error", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/admin/stats", func(w http.ResponseWriter, r *http.Request) {
		if opt.Store == nil {
			http.Error(w, "store not configured", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]any{
			"self_id":       opt.SelfID,
			"cached_blocks": opt.Store.Len(),
		})
	})

	mux.HandleFunc("/admin/members", func(w http.ResponseWriter, r *http.Request) {
		view := currentView(opt.Cluster)
		if view == nil {
			http.Error(w, "cluster not configured", http.StatusServiceUnavailable)
			return
		}
		ms := view.Members()
		out := make([]map[string]any, 0, len(ms))
		for _, m := range ms {
			out = append(out, map[string]any{
				"id": m.ID, "addr": m.Addr, "weight": m.Weight, "state": m.State.String(),
			})
		}
		writeJSON(w, map[string]any{
			"self_id": opt.SelfID,
			"epoch":   strconv.FormatUint(view.Epoch(), 10),
			"members": out,
		})
	})

	mux.HandleFunc("/admin/ring", func(w http.ResponseWriter, r *http.Request) {
		view := currentView(opt.Cluster)
		if view == nil {
			http.Error(w, "cluster not configured", http.StatusServiceUnavailable)
			return
		}
		key, err := strconv.ParseUint(r.URL.Query().Get("key"), 10, 64)
		if err != nil {
			http.Error(w, "key must be a uint64 (decimal chunkKey)", http.StatusBadRequest)
			return
		}
		n := 8
		if s := r.URL.Query().Get("n"); s != "" {
			if v, err := strconv.Atoi(s); err == nil && v > 0 {
				n = v
			}
		}
		ranked := hashring.Rank(key, view.Ready())
		if n < len(ranked) {
			ranked = ranked[:n]
		}
		out := make([]map[string]any, 0, len(ranked))
		for i, nd := range ranked {
			m, _ := view.Get(nd.ID)
			out = append(out, map[string]any{
				"rank": i, "id": nd.ID, "addr": m.Addr, "weight": nd.Weight,
			})
		}
		writeJSON(w, map[string]any{
			"key":   strconv.FormatUint(key, 10),
			"epoch": strconv.FormatUint(view.Epoch(), 10),
			"order": out,
		})
	})

	return mux
}

// currentView returns the provider's view, or nil when unavailable.
func currentView(p cluster.Provider) *cluster.View {
	if p == nil {
		return nil
	}
	return p.Current()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
