package node

// Node-side ingress tests for issue #21 item 2: control-byte member IDs must
// be rejected at -self-id validation and at -peers parsing (the epoch framing
// bytes 0x1F/0x1E could otherwise collide two different memberships).

import (
	"io"
	"strings"
	"testing"
)

func TestSelfIDRejectsControlBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := build(config{
		listen: "127.0.0.1:0", prefix: "dart", cacheDir: dir,
		cacheSize: 64 << 20, blockSize: 1 << 20, chunkSize: 8 << 20,
		selfID:     "bad\x1fid",
		peerListen: "127.0.0.1:0",
		peers:      "bad\x1fid@127.0.0.1:1",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "self-id") {
		t.Fatalf("error = %v, want a -self-id alphabet complaint", err)
	}
}

func TestParsePeersRejectsControlByteIDs(t *testing.T) {
	if _, err := parsePeers("a\x1fb@127.0.0.1:9000"); err == nil {
		t.Fatal("a control-byte peer ID was accepted")
	}
	if _, err := parsePeers("good@127.0.0.1:9000,also-good@127.0.0.1:9001"); err != nil {
		t.Fatalf("legitimate IDs rejected: %v", err)
	}
}
