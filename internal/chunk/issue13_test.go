package chunk

// Regression tests for issue #13 items C1/C2 (separator exclusion, fallback
// query stripping). H1/H2 are documentation/comment fixes per the issue.

import (
	"strings"
	"testing"
)

// TestObjectIDFallbackStripsQueryAndFragment pins C2: the raw fallback used to
// return the input verbatim, query and fragment included — so a presigned
// signature churned cache identity and landed in keys, the exact harm §3.5
// excludes.
func TestObjectIDFallbackStripsQueryAndFragment(t *testing.T) {
	in := "/v2/x/blobs/latest?token=abc#frag"
	id, ca := ObjectID(in)
	if ca {
		t.Fatal("non-digest path must not be content-addressed")
	}
	if strings.ContainsAny(id, "?#") {
		t.Fatalf("fallback identity %q still contains query/fragment", id)
	}
	if id != "/v2/x/blobs/latest" {
		t.Fatalf("fallback identity = %q, want the path alone", id)
	}

	// The load-bearing property: signatures never move identity.
	a, _ := ObjectID("/v2/x/blobs/latest?sig=1")
	b, _ := ObjectID("/v2/x/blobs/latest?sig=2")
	if a != b {
		t.Fatalf("?sig=1 vs ?sig=2 produced different identities: %q vs %q", a, b)
	}
}

// TestObjectIDNeverContainsSeparator pins C1: a percent-encoded %1F delivers a
// literal 0x1F into the decoded path; the derived identity must never contain
// the ChunkKey separator, or serialization is not injective.
func TestObjectIDNeverContainsSeparator(t *testing.T) {
	for _, in := range []string{
		"http://example.com/rel%1F1",
		"/rel%1F1",
		"http://example.com/a%1Fb/c?x=%1F",
	} {
		id, _ := ObjectID(in)
		if strings.IndexByte(id, UnitSep) >= 0 {
			t.Fatalf("ObjectID(%q) = %q contains the 0x1F separator", in, id)
		}
	}

	// The injectivity hazard itself: without the exclusion these two pairs
	// hash identically. Both raw shapes are now unreachable (namespace
	// rejected at engine construction, objectID stripped), which is what the
	// guards above pin.
	if ChunkKey("a", "b\x1fc", 0) != ChunkKey("a\x1fb", "c", 0) {
		t.Fatal("expected the raw collision to demonstrate why the exclusion exists")
	}
}
