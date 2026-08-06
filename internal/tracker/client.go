package tracker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client talks to a tracker over HTTP.
type Client struct {
	// HTTP is the underlying client; nil uses a short-timeout default. Tracker
	// calls are small control-plane messages, so they should fail fast rather
	// than stall a read.
	HTTP *http.Client
}

// NewClient returns a Client with a control-plane-appropriate timeout.
func NewClient() *Client {
	return &Client{HTTP: &http.Client{Timeout: 2 * time.Second}}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

// Join announces interest in file and returns the frozen reader set.
func (c *Client) Join(ctx context.Context, addr, file, node string, ttl time.Duration) (JoinResponse, error) {
	body, err := json.Marshal(JoinRequest{File: file, Node: node, TTLMs: ttl.Milliseconds()})
	if err != nil {
		return JoinResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+JoinPath, bytes.NewReader(body))
	if err != nil {
		return JoinResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return JoinResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return JoinResponse{}, fmt.Errorf("tracker %s: unexpected status %s", addr, resp.Status)
	}
	var out JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return JoinResponse{}, fmt.Errorf("tracker %s: decode: %w", addr, err)
	}
	return out, nil
}

// Leave drops this node's lease on file.
func (c *Client) Leave(ctx context.Context, addr, file, node string) error {
	body, err := json.Marshal(JoinRequest{File: file, Node: node})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+LeavePath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tracker %s: unexpected status %s", addr, resp.Status)
	}
	return nil
}
