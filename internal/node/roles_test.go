package node

import (
	"github.com/data-accelerator/dart/internal/cluster"
	"io"
	"testing"
)

func TestRoleConfiguration(t *testing.T) {
	for _, name := range []string{"ordinary", "root-only", "non-root"} {
		cfg, err := parseFlags([]string{"-role", name}, io.Discard, nil)
		if err != nil || cfg.role.String() != name {
			t.Fatalf("%s: %+v %v", name, cfg, err)
		}
	}
	if _, err := parseFlags([]string{"-role", "root"}, io.Discard, nil); err == nil {
		t.Fatal("unknown role accepted")
	}
	ms, err := parsePeers("a@a:1#root-only,b@[::1]:2#non-root,c@c:3")
	if err != nil {
		t.Fatal(err)
	}
	if ms[0].Role != cluster.RootOnly || ms[1].Role != cluster.NonRoot || ms[2].Role != cluster.Ordinary || ms[1].Addr != "[::1]:2" {
		t.Fatal(ms)
	}
}

func TestRootOnlyHasNoClientHandler(t *testing.T) {
	cfg, err := parseFlags([]string{"-role", "root-only", "-self-id", "a", "-peers", "a@a:1#root-only", "-cache-mode", "memory", "-cache-size", "32KiB", "-block-size", "4KiB", "-chunk-size", "16KiB"}, io.Discard, nil)
	if err != nil {
		t.Fatal(err)
	}
	n, err := build(cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer n.closer.Close()
	if n.client != nil || n.peer == nil {
		t.Fatal("root-only listener assembly incorrect")
	}
	cfg.peers = "a@a:1"
	if other, err := build(cfg, io.Discard); err == nil {
		other.closer.Close()
		t.Fatal("inconsistent self role accepted")
	}
	cfg.peers = ""
	if other, err := build(cfg, io.Discard); err == nil {
		other.closer.Close()
		t.Fatal("standalone root-only accepted")
	}
}
