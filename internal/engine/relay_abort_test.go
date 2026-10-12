package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-accelerator/dart/internal/peer"
)

func TestStreamRelayPropagatesParentFailure(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	parent := httptest.NewServer(&peer.StreamServer{Src: func(_ context.Context, _ peer.BlockRequest, w io.Writer, _ func(int64)) (int64, bool, error) {
		n, err := w.Write(content[:8])
		if err != nil {
			return int64(n), true, err
		}
		return int64(n), true, errors.New("origin stream interrupted")
	}})
	defer parent.Close()
	e, _ := tailEngine(t, peer.NewClient(), parent, origin.URL)
	relay := httptest.NewServer(&peer.StreamServer{Src: e.PeerStreamSource()})
	defer relay.Close()
	var body bytes.Buffer
	key := blockKeyFor(origin.URL)
	n, held, err := peer.NewClient().Stream(context.Background(), strings.TrimPrefix(relay.URL, "http://"), peer.BlockRequest{Key: key, URL: origin.URL}, &body)
	if n != 8 || !held || !errors.Is(err, io.ErrUnexpectedEOF) || !bytes.Equal(body.Bytes(), content[:8]) {
		t.Fatalf("relay Stream = %d, %v, %v, body=%x; want aborted partial response", n, held, err, body.Bytes())
	}
	if e.store.Has(key) {
		t.Fatal("interrupted parent stream populated the relay cache")
	}
}

func TestStreamRelayQuarantinesWrongLengthParent(t *testing.T) {
	content := blob(16)
	var parentCalls, originCalls int64
	origin := countingOrigin(t, content, &originCalls)
	parent := httptest.NewServer(&peer.StreamServer{Src: func(_ context.Context, _ peer.BlockRequest, w io.Writer, _ func(int64)) (int64, bool, error) {
		atomic.AddInt64(&parentCalls, 1)
		n, err := w.Write(content[:8])
		return int64(n), true, err // clean HTTP EOF, wrong block geometry
	}})
	defer parent.Close()
	client := peer.NewClient()
	client.Breaker = peer.NewBreaker(peer.BreakerOptions{
		FailureThreshold: 3, Cooldown: time.Hour,
	})
	e, _ := tailEngine(t, client, parent, origin.URL)
	relay := httptest.NewServer(&peer.StreamServer{Src: e.PeerStreamSource()})
	defer relay.Close()
	key := blockKeyFor(origin.URL)
	caller := peer.NewClient()
	addr := strings.TrimPrefix(relay.URL, "http://")
	parentAddr := strings.TrimPrefix(parent.URL, "http://")
	var body bytes.Buffer
	_, _, err := caller.Stream(context.Background(), addr, peer.BlockRequest{Key: key, URL: origin.URL}, &body)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first relay error = %v, want unexpected EOF", err)
	}
	if state := client.Breaker.State(parentAddr); state != peer.BreakerOpen {
		t.Fatalf("invalid parent circuit = %v, want open", state)
	}
	if e.store.Has(key) {
		t.Fatal("invalid parent block entered the cache")
	}
	beforeFallback := atomic.LoadInt64(&originCalls)
	for i := 0; i < 3; i++ {
		body.Reset()
		n, held, err := caller.Stream(context.Background(), addr, peer.BlockRequest{Key: key, URL: origin.URL}, &body)
		if err != nil || !held || n != int64(len(content)) || !bytes.Equal(body.Bytes(), content) {
			t.Fatalf("request %d after quarantine = %d, %v, %v; want correct origin bytes", i, n, held, err)
		}
	}
	if atomic.LoadInt64(&parentCalls) != 1 {
		t.Fatal("repeated requests contacted the quarantined parent")
	}
	if atomic.LoadInt64(&originCalls) != beforeFallback+1 || !e.store.Has(key) {
		t.Fatal("relay did not fetch the valid origin block once and cache it")
	}
}
