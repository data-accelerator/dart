package chunk

import "testing"

// These identities are literal URL encodings, independent of ChunkKey's hash.
func TestObjectIDPreservesEscapedPath(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"HTTP://Example.COM/a%2Fb", "http://example.com/a%2Fb"},
		{"http://example.com/a/b", "http://example.com/a/b"},
		{"http://example.com/a%1Fb", "http://example.com/a%1Fb"},
		{"http://example.com/ab", "http://example.com/ab"},
		{"http://example.com/a%251Fb", "http://example.com/a%251Fb"},
		{"/a%1Fb?Signature=x#fragment", "/a%1Fb"},
		{"/ab?Signature=x", "/ab"},
		{"http://example.com/sha256:" + sha256 + "%2Fmetadata", "http://example.com/sha256:" + sha256 + "%2Fmetadata"},
		{"bad%escape/a\x1fb", "bad%25escape/a%1Fb"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, ca := ObjectID(tc.raw)
			if ca || got != tc.want {
				t.Fatalf("ObjectID(%q) = %q, %v; want %q, false", tc.raw, got, ca, tc.want)
			}
		})
	}
}
