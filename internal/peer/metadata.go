package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/data-accelerator/dart/internal/fetch"
)

// MetadataPath is the role-aware, terminal object-probe endpoint.
const MetadataPath = "/peer/v2/metadata"

// Metadata describes an origin endpoint, independently of cached digest bytes.
type Metadata struct {
	Size             int64 `json:"size"`
	RangeUnsupported bool  `json:"range_unsupported"`
}

// MetadataServer delegates to a local root probe. Probe must never relay.
type MetadataServer struct {
	Probe func(context.Context, string) (Metadata, error)
}

func (s *MetadataServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "read-only", 405)
		return
	}
	url := r.Header.Get(HeaderOrigin)
	if url == "" {
		http.Error(w, "missing origin", 400)
		return
	}
	if s.Probe == nil {
		http.Error(w, "not a supply root", 503)
		return
	}
	m, err := s.Probe(r.Context(), url)
	if err != nil {
		var se *fetch.StatusError
		if errors.As(err, &se) {
			w.Header().Set(HeaderUpstreamStatus, strconv.Itoa(se.Code))
		}
		http.Error(w, "metadata unavailable", 502)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m)
}

// Probe retrieves endpoint metadata without fetching or publishing a block.
func (c *Client) Probe(ctx context.Context, addr, url string) (Metadata, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+addr+MetadataPath, nil)
	if err != nil {
		return Metadata{}, err
	}
	req.Header.Set(HeaderOrigin, url)
	resp, err := c.http().Do(req)
	if err != nil {
		return Metadata{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if err := upstreamRefusal(addr, resp); err != nil {
			return Metadata{}, err
		}
		return Metadata{}, fmt.Errorf("peer metadata: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil {
		return Metadata{}, err
	}
	if len(body) > 4096 {
		return Metadata{}, errors.New("peer metadata too large")
	}
	var wire struct {
		Size             *int64 `json:"size"`
		RangeUnsupported *bool  `json:"range_unsupported"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return Metadata{}, err
	}
	if wire.Size == nil || wire.RangeUnsupported == nil || *wire.Size < -1 || (*wire.Size < 0 && !*wire.RangeUnsupported) {
		return Metadata{}, errors.New("invalid peer metadata")
	}
	return Metadata{Size: *wire.Size, RangeUnsupported: *wire.RangeUnsupported}, nil
}
