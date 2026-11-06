package store

// fixedIndex is a bounded open-addressed index. Values are descriptor indices
// plus one (zero is empty). Backward-shift deletion avoids tombstones and
// allocations during unlimited churn.
type indexCell struct {
	a, b  uint64
	value int
}
type fixedIndex []indexCell

func makeIndex(n int) fixedIndex {
	size := 4
	for size < n*2 {
		size *= 2
	}
	return make(fixedIndex, size)
}
func (t fixedIndex) home(a, b uint64) int {
	return int(fmix64(a^fmix64(b+0x9e3779b97f4a7c15)) & uint64(len(t)-1))
}
func (t fixedIndex) get(a, b uint64) (int, bool) {
	for i := t.home(a, b); ; i = (i + 1) & (len(t) - 1) {
		c := t[i]
		if c.value == 0 {
			return 0, false
		}
		if c.a == a && c.b == b {
			return c.value - 1, true
		}
	}
}
func (t fixedIndex) put(a, b uint64, value int) {
	for i := t.home(a, b); ; i = (i + 1) & (len(t) - 1) {
		if t[i].value == 0 || (t[i].a == a && t[i].b == b) {
			t[i] = indexCell{a: a, b: b, value: value + 1}
			return
		}
	}
}
func (t fixedIndex) remove(a, b uint64) {
	mask := len(t) - 1
	i := t.home(a, b)
	for t[i].value != 0 && (t[i].a != a || t[i].b != b) {
		i = (i + 1) & mask
	}
	if t[i].value == 0 {
		return
	}
	for j := (i + 1) & mask; t[j].value != 0; j = (j + 1) & mask {
		home := t.home(t[j].a, t[j].b)
		if (j-home)&mask >= (j-i)&mask {
			t[i] = t[j]
			i = j
		}
	}
	t[i] = indexCell{}
}
