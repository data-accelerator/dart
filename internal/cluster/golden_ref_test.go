package cluster

// Issue #14: cross-check the epoch golden values against the independent
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

func TestEpochAgainstPythonReference(t *testing.T) {
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
		if f[0] != "epoch" {
			continue
		}
		if len(f) != 3 {
			t.Fatalf("malformed epoch record %q", rec)
		}
		n++
		var members []Member
		if f[1] != "(empty)" {
			for _, m := range strings.Split(f[1], ",") {
				id, ws, ok := strings.Cut(m, "=")
				if !ok {
					t.Fatalf("malformed member %q", m)
				}
				w, err := strconv.ParseFloat(ws, 64)
				if err != nil {
					t.Fatalf("malformed weight in %q: %v", m, err)
				}
				members = append(members, Member{ID: id, Weight: w, State: Ready})
			}
		}
		want, _ := strconv.ParseUint(f[2], 10, 64)
		if got := NewView(members).Epoch(); got != want {
			t.Errorf("epoch(%s) = %d, python reference says %d", f[1], got, want)
		}
	}
	if n != 2 {
		t.Fatalf("reference emitted %d epoch records, want 2", n)
	}
}
