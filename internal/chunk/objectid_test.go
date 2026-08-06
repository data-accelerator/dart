package chunk

import (
	"strings"
	"testing"
)

const (
	// sha256 is a well-formed 64-char hex digest used across these tests.
	sha256 = "ab3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a" // 64 hex
)

func init() {
	if len(sha256) != 64 {
		panic("test fixture: sha256 must be 64 hex chars")
	}
}

// TestObjectIDPresignedDedupAcrossEndpoints is the point of the Distribution
// layout support: the same layer reached through different buckets, hosts or
// signatures must resolve to one identity, so it is cached once.
func TestObjectIDPresignedDedupAcrossEndpoints(t *testing.T) {
	base := "/docker/registry/v2/blobs/sha256/" + sha256[:2] + "/" + sha256 + "/data"
	urls := []string{
		"https://bkt.oss-cn-hangzhou.aliyuncs.com" + base + "?OSSAccessKeyId=k1&Expires=1&Signature=aaa",
		"https://bkt.oss-cn-hangzhou.aliyuncs.com" + base + "?OSSAccessKeyId=k1&Expires=2&Signature=bbb",
		"https://other-bkt.s3.us-east-1.amazonaws.com" + base + "?X-Amz-Signature=ccc",
		"https://internal.mirror.example" + base,
		// The OCI form for the same content must also agree.
		"https://registry.example.com/v2/lib/nginx/blobs/sha256:" + sha256,
	}
	want := "sha256:" + sha256
	for _, u := range urls {
		got, ca := ObjectID(u)
		if got != want {
			t.Errorf("ObjectID(%.60s...) = %q, want %q", u, got, want)
		}
		if !ca {
			t.Errorf("ObjectID(%.60s...) contentAddressed=false, want true", u)
		}
	}
}

// TestObjectIDRejectsNonLayoutPaths guards the property that makes defaulting the
// Distribution layout ON acceptable: a false positive would map two *different*
// objects onto one key, which is a correctness bug rather than a slow cache.
func TestObjectIDRejectsNonLayoutPaths(t *testing.T) {
	notRecognized := []string{
		// The intermediate segment must equal the hex's own first two chars.
		// This self-consistency check is the strong one.
		"https://h/blobs/sha256/zz/" + sha256 + "/data",
		"https://h/blobs/sha256/00/" + sha256 + "/data",
		// Wrong hex length for the algorithm.
		"https://h/blobs/sha256/ab/abcdef/data",
		"https://h/blobs/sha256/" + sha256[:2] + "/" + sha256 + "cd/data",
		// Unknown algorithm: we cannot verify its length, so we refuse.
		"https://h/blobs/md5/ab/" + sha256 + "/data",
		"https://h/blobs/sha1/ab/" + sha256 + "/data",
		// Not hex.
		"https://h/blobs/sha256/gg/" + strings.Repeat("g", 64) + "/data",
		// Missing the "blobs" anchor.
		"https://h/objects/sha256/" + sha256[:2] + "/" + sha256 + "/data",
		// Prefix segment of the wrong width.
		"https://h/blobs/sha256/a/" + sha256 + "/data",
		"https://h/blobs/sha256/abc/" + sha256 + "/data",
		// Truncated: nothing after the algorithm.
		"https://h/blobs/sha256",
		"https://h/blobs/sha256/ab",
	}
	for _, u := range notRecognized {
		got, ca := ObjectID(u)
		if ca {
			t.Errorf("ObjectID(%q) = %q, contentAddressed=true; want the canonical-URL fallback", u, got)
		}
		if strings.HasPrefix(got, "sha256:") || strings.HasPrefix(got, "md5:") || strings.HasPrefix(got, "sha1:") {
			t.Errorf("ObjectID(%q) = %q; a digest must not be fabricated", u, got)
		}
	}
}

// TestObjectIDSha512Layout: the recognizer is not hardcoded to sha256.
func TestObjectIDSha512Layout(t *testing.T) {
	h := strings.Repeat("9f", 64) // 128 hex chars
	u := "https://h/docker/registry/v2/blobs/sha512/" + h[:2] + "/" + h + "/data"
	got, ca := ObjectID(u)
	if !ca || got != "sha512:"+h {
		t.Errorf("ObjectID = %q (ca=%v), want sha512 digest", got, ca)
	}
}

// TestObjectIDLayoutSwitch: the recognizer can be turned off, and then the same
// URL falls back to the canonical form.
func TestObjectIDLayoutSwitch(t *testing.T) {
	u := "https://bkt.example.com/docker/registry/v2/blobs/sha256/" +
		sha256[:2] + "/" + sha256 + "/data?Signature=xyz"

	got, ca := ObjectIDLayout(u, LayoutDistribution)
	if !ca || got != "sha256:"+sha256 {
		t.Errorf("Distribution layout: got %q (ca=%v)", got, ca)
	}

	got, ca = ObjectIDLayout(u, LayoutOCIOnly)
	if ca {
		t.Errorf("OCIOnly: contentAddressed=true, want false")
	}
	// The fallback is the canonical URL, and it must still exclude the signature.
	if strings.Contains(got, "Signature") {
		t.Errorf("OCIOnly fallback %q leaks the signature", got)
	}
	if got != "https://bkt.example.com/docker/registry/v2/blobs/sha256/"+
		sha256[:2]+"/"+sha256+"/data" {
		t.Errorf("OCIOnly fallback = %q", got)
	}

	// An OCI blob path is recognized under either layout.
	oci := "https://r.example.com/v2/x/blobs/sha256:" + sha256
	for _, l := range []DigestLayout{LayoutDistribution, LayoutOCIOnly} {
		if got, ca := ObjectIDLayout(oci, l); !ca || got != "sha256:"+sha256 {
			t.Errorf("layout %v on an OCI path: got %q (ca=%v)", l, got, ca)
		}
	}
}

// TestObjectIDQueryNeverInIdentity: a presigned signature changes on every
// request, so including the query would give one object a new identity each time.
func TestObjectIDQueryNeverInIdentity(t *testing.T) {
	for _, u := range []string{
		"https://h/some/opaque/path?Signature=aaa",
		"https://h/some/opaque/path?Signature=bbb",
	} {
		got, _ := ObjectID(u)
		if strings.Contains(got, "Signature") || strings.Contains(got, "?") {
			t.Errorf("ObjectID(%q) = %q, must exclude the query", u, got)
		}
	}
	a, _ := ObjectID("https://h/p?Signature=aaa")
	b, _ := ObjectID("https://h/p?Signature=bbb")
	if a != b {
		t.Errorf("different signatures gave different identities: %q vs %q", a, b)
	}
}

func TestObjectIDCaseNormalization(t *testing.T) {
	upper := strings.ToUpper(sha256)
	u := "HTTPS://BKT.Example.COM/blobs/SHA256/" + upper[:2] + "/" + upper + "/data"
	got, ca := ObjectID(u)
	if !ca || got != "sha256:"+sha256 {
		t.Errorf("ObjectID = %q (ca=%v), want the lower-cased digest", got, ca)
	}
}
