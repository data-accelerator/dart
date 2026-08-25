package engine

// Regression test for issue #13 item C1: the namespace is a ChunkKey field and
// must not contain the 0x1F separator (serialization would not be injective).

import (
	"strings"
	"testing"
)

// TestNamespaceWithSeparatorRejected pins C1: an operator-supplied namespace
// containing the chunk-key separator must fail fast at engine construction
// rather than silently breaking key injectivity.
func TestNamespaceWithSeparatorRejected(t *testing.T) {
	_, err := New(Options{
		Chunk: testCfg(), Store: openStoreAt(t), Fetcher: newFetcher(),
		Namespace: "dar\x1ft",
	})
	if err == nil || !strings.Contains(err.Error(), "separator") {
		t.Fatalf("New with a 0x1F namespace: err=%v, want a rejection naming the separator", err)
	}
}
