package chunk

// Issue #14: cross-check the ChunkKey golden values against the independent
// Python reference (contrib/golden/golden_reference.py). Runs only when
// DART_GOLDEN_REF=1 is set (CI).

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestChunkKeyAgainstPythonReference(t *testing.T) {
	if os.Getenv("DART_GOLDEN_REF") != "1" {
		t.Skip("set DART_GOLDEN_REF=1 to diff against the Python reference (CI)")
	}
	script := filepath.Join("..", "..", "contrib", "golden", "golden_reference.py")
	out, err := exec.Command("python3", script).Output()
	if err != nil {
		t.Fatalf("python3 %s: %v", script, err)
	}
	n := 0
	for _, rec := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(strings.TrimSpace(rec), "|")
		if f[0] != "chunkkey" {
			continue
		}
		if len(f) != 5 {
			t.Fatalf("malformed chunkkey record %q", rec)
		}
		n++
		ci, _ := strconv.ParseInt(f[3], 10, 64)
		want, _ := strconv.ParseUint(f[4], 10, 64)
		if got := ChunkKey(f[1], f[2], ci); got != want {
			t.Errorf("ChunkKey(%q, %q, %d) = %d, python reference says %d", f[1], f[2], ci, got, want)
		}
	}
	if n != 4 {
		t.Fatalf("reference emitted %d chunkkey records, want 4", n)
	}
}
