package main

import "testing"

// TestSchemes guards the wiring contract of this variant: the dependency-free
// schemes plus the EndpointSlice scheme, and nothing else.
func TestSchemes(t *testing.T) {
	want := []string{"dns", "static", "k8s"}
	if len(schemes) != len(want) {
		t.Fatalf("schemes = %v, want names %v", schemes, want)
	}
	for i, name := range want {
		if schemes[i].Name != name {
			t.Errorf("schemes[%d].Name = %q, want %q", i, schemes[i].Name, name)
		}
		if schemes[i].New == nil {
			t.Errorf("schemes[%d] (%s) has no constructor", i, name)
		}
	}
}
