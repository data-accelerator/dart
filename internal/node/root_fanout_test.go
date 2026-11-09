package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-accelerator/dart/internal/chunk"
	"github.com/data-accelerator/dart/internal/hashring"
	"github.com/data-accelerator/dart/internal/peer"
)

func TestRootFanoutFlagsAndRouting(t *testing.T) {
	for _, raw := range []string{"0", "-1", "invalid"} {
		if _, err := parseFlags([]string{"-root-fanout=" + raw}, io.Discard, nil); err == nil {
			t.Fatal("invalid root fanout accepted", raw)
		}
	}
	for _, tc := range []struct {
		flag                   string
		failParent             bool
		rootCalls, parentCalls int64
	}{
		{"", false, 0, 1}, {"1", false, 0, 1}, {"2", false, 1, 0}, {"1", true, 1, 1},
	} {
		t.Run(fmt.Sprintf("root=%s/fallback=%v", tc.flag, tc.failParent), func(t *testing.T) {
			const url = "http://origin.invalid/config"
			content := blob(16)
			oid, _ := chunk.ObjectID(url)
			order := hashring.Rank(chunk.ChunkKey("dart", oid, 0), []hashring.Node{{ID: "A", Weight: 1}, {ID: "B", Weight: 1}})
			var roots, parents atomic.Int64
			mux := http.NewServeMux()
			mux.Handle(peer.MetadataPath, &peer.MetadataServer{Probe: func(context.Context, string) (peer.Metadata, error) { return peer.Metadata{Size: 16}, nil }})
			mux.Handle("/peer/", &peer.Server{Src: func(_ context.Context, req peer.BlockRequest) ([]byte, bool, error) {
				roots.Add(1)
				if !req.Supply {
					t.Error("root request is not terminal")
				}
				return content, true, nil
			}})
			root := httptest.NewServer(mux)
			defer root.Close()
			parent := httptest.NewServer(&peer.Server{Src: func(_ context.Context, req peer.BlockRequest) ([]byte, bool, error) {
				parents.Add(1)
				if req.Supply {
					t.Error("reader request marked supply")
				}
				if tc.failParent {
					return nil, false, nil
				}
				return content, true, nil
			}})
			defer parent.Close()
			peers := fmt.Sprintf("R@%s#root-only,%s@%s#non-root,%s@unused:1#non-root", strings.TrimPrefix(root.URL, "http://"), order[0].ID, strings.TrimPrefix(parent.URL, "http://"), order[1].ID)
			args := []string{"-self-id=" + order[1].ID, "-role=non-root", "-peers=" + peers, "-reader-tree=false", "-hedge=false", "-fanout=2", "-cache-mode=memory", "-cache-size=32KiB", "-block-size=16", "-chunk-size=64"}
			if tc.flag != "" {
				args = append(args, "-root-fanout="+tc.flag)
			}
			cfg, err := parseFlags(args, io.Discard, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.flag == "" && cfg.rootFanout != 1 {
				t.Fatal("default root fanout", cfg.rootFanout)
			}
			n, err := build(cfg, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer n.closer.Close()
			rr := httptest.NewRecorder()
			n.client.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/dart/"+url, nil))
			if rr.Code != http.StatusOK || !bytes.Equal(rr.Body.Bytes(), content) {
				t.Fatalf("response: %d %s", rr.Code, rr.Body.String())
			}
			if roots.Load() != tc.rootCalls || parents.Load() != tc.parentCalls {
				t.Fatal("wrong route", roots.Load(), parents.Load())
			}
		})
	}
}
