package hashring

import (
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"testing"
)

type rootFanoutGolden struct {
	N, K     int
	RootK    int `json:"root_k"`
	Parents  []int
	Children [][]int
	Depths   []int
}

func TestRootFanoutGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/root_fanout.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []rootFanoutGolden
	if err := json.Unmarshal(data, &cases); err != nil || len(cases) != 10 {
		t.Fatal("invalid golden fixture", err, len(cases))
	}
	for _, tc := range cases {
		for i := 0; i < tc.N; i++ {
			p := ParentWithRootFanout(i, tc.N, tc.K, tc.RootK)
			d := DepthWithRootFanout(i, tc.N, tc.K, tc.RootK)
			children := ChildrenWithRootFanout(i, tc.N, tc.K, tc.RootK)
			if p != tc.Parents[i] || d != tc.Depths[i] || !equalInts(children, tc.Children[i]) {
				t.Fatalf("n=%d k=%d root=%d i=%d: parent=%d depth=%d children=%v", tc.N, tc.K, tc.RootK, i, p, d, children)
			}
		}
	}
	if os.Getenv("DART_GOLDEN_REF") == "1" {
		out, err := exec.Command("python3", "../../contrib/golden/root_fanout_reference.py").Output()
		if err != nil || !reflect.DeepEqual(data, out) {
			t.Fatal("fixture differs from independent Python reference", err)
		}
	}
}

func TestRootFanoutProperties(t *testing.T) {
	for n := 1; n <= 128; n++ {
		for _, k := range []int{1, 2, 4, 17} {
			for _, rootK := range []int{1, 2, 3, 16} {
				for i := 0; i < n; i++ {
					p := ParentWithRootFanout(i, n, k, rootK)
					d := DepthWithRootFanout(i, n, k, rootK)
					cs := ChildrenWithRootFanout(i, n, k, rootK)
					limit := k
					if i == 0 {
						limit = rootK
						if p != -1 || d != 0 {
							t.Fatal("invalid root")
						}
					} else if p < 0 || p >= i || d != 1+DepthWithRootFanout(p, n, k, rootK) {
						t.Fatalf("invalid ordered edge: %d -> %d", i, p)
					}
					if len(cs) > limit {
						t.Fatal("degree exceeded")
					}
					for _, c := range cs {
						if ParentWithRootFanout(c, n, k, rootK) != i {
							t.Fatal("child/parent mismatch")
						}
					}
					if i > 0 {
						found := false
						for _, c := range ChildrenWithRootFanout(p, n, k, rootK) {
							found = found || c == i
						}
						if !found {
							t.Fatal("parent/child mismatch")
						}
					}
					if rootK == k && (p != Parent(i, n, k) || d != Depth(i, n, k) || !equalInts(cs, Children(i, n, k))) {
						t.Fatal("uniform tree changed")
					}
				}
			}
		}
	}
}

func TestRootFanoutBoundsAndAllocations(t *testing.T) {
	for _, tc := range [][4]int{{-1, 8, 2, 1}, {8, 8, 2, 1}, {0, 0, 2, 1}, {1, 8, 0, 1}, {1, 8, 2, 0}, {1, 8, 2, -1}} {
		i, n, k, rootK := tc[0], tc[1], tc[2], tc[3]
		if ParentWithRootFanout(i, n, k, rootK) != -1 || DepthWithRootFanout(i, n, k, rootK) != -1 || ChildrenWithRootFanout(i, n, k, rootK) != nil {
			t.Fatal("invalid arguments accepted", tc)
		}
	}
	// Very large configured fanout must only allocate for existing children.
	if got := ChildrenWithRootFanout(0, 3, 2, int(^uint(0)>>1)); !equalInts(got, []int{1, 2}) {
		t.Fatal(got)
	}
	if got := testing.AllocsPerRun(100, func() {
		if ParentWithRootFanout(99999, 100000, 4, 1) < 0 || DepthWithRootFanout(99999, 100000, 4, 1) < 1 {
			panic("missing node")
		}
	}); got != 0 {
		t.Fatal("navigation allocated", got)
	}
}
