package metrics

// Regression test for issue #18 M1: HELP text must be backslash/newline
// escaped (Prometheus text format), else a help string containing a newline
// splits the exposition into malformed lines.

import (
	"strings"
	"testing"
)

func TestHelpTextEscaped(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("dart_issue18_counter", "line one\nline two with a \\ backslash")

	var b strings.Builder
	if err := r.Render(&b); err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := b.String()
	want := `# HELP dart_issue18_counter line one\nline two with a \\ backslash` + "\n"
	if !strings.Contains(out, want) {
		t.Fatalf("HELP line not escaped; want substring %q in:\n%s", want, out)
	}
	// No physical line inside the exposition may be an orphan HELP fragment.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "line two") && !strings.HasPrefix(line, "# HELP") {
			t.Fatalf("unescaped newline split the HELP text: %q", line)
		}
	}
}

func TestEscapeHelpOnlyEscapesBackslashAndNewline(t *testing.T) {
	// Unlike label values, HELP text needs no quote escaping (spec: backslash
	// and line feed only) — a quote must pass through unadorned.
	if got := escapeHelp(`say "hi"`); got != `say "hi"` {
		t.Errorf("escapeHelp mangled quotes: %q", got)
	}
}
