package metrics

import (
	"math"
	"strings"
	"sync"
	"testing"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b strings.Builder
	if err := r.Render(&b); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return b.String()
}

func TestCounter(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("dart_things_total", "things done")
	c.Inc()
	c.Add(5)
	c.Add(-3) // ignored
	if c.Value() != 6 {
		t.Errorf("Value = %d, want 6", c.Value())
	}
	out := render(t, r)
	for _, want := range []string{
		"# HELP dart_things_total things done",
		"# TYPE dart_things_total counter",
		"dart_things_total 6",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestGauge(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("dart_bytes", "bytes in use")
	g.Set(1234.5)
	if g.Value() != 1234.5 {
		t.Errorf("Value = %v", g.Value())
	}
	out := render(t, r)
	if !strings.Contains(out, "# TYPE dart_bytes gauge") || !strings.Contains(out, "dart_bytes 1234.5") {
		t.Errorf("gauge output:\n%s", out)
	}
}

func TestLabels(t *testing.T) {
	r := NewRegistry()
	hit := r.NewCounter("dart_cache_total", "cache ops", LabelPair{"result", "hit"})
	miss := r.NewCounter("dart_cache_total", "cache ops", LabelPair{"result", "miss"})
	hit.Add(3)
	miss.Add(1)
	out := render(t, r)
	if !strings.Contains(out, `dart_cache_total{result="hit"} 3`) {
		t.Errorf("missing hit series:\n%s", out)
	}
	if !strings.Contains(out, `dart_cache_total{result="miss"} 1`) {
		t.Errorf("missing miss series:\n%s", out)
	}
	// HELP/TYPE must appear exactly once for a repeated metric name.
	if n := strings.Count(out, "# TYPE dart_cache_total"); n != 1 {
		t.Errorf("TYPE emitted %d times, want 1:\n%s", n, out)
	}
}

// TestHistogramBuckets checks cumulative le buckets, sum and count.
func TestHistogramBuckets(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("dart_latency_seconds", "latency", []float64{0.1, 1, 10})
	for _, v := range []float64{0.05, 0.1, 0.5, 5, 100} {
		h.Observe(v)
	}
	out := render(t, r)
	// 0.05,0.1 <= 0.1 => 2; +0.5 <= 1 => 3; +5 <= 10 => 4; 100 only in +Inf => 5
	for _, want := range []string{
		`dart_latency_seconds_bucket{le="0.1"} 2`,
		`dart_latency_seconds_bucket{le="1"} 3`,
		`dart_latency_seconds_bucket{le="10"} 4`,
		`dart_latency_seconds_bucket{le="+Inf"} 5`,
		`dart_latency_seconds_count 5`,
		`dart_latency_seconds_sum 105.65`,
		"# TYPE dart_latency_seconds histogram",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
}

func TestHistogramBoundsSortedAndDeduped(t *testing.T) {
	h := NewHistogram(10, 1, 1, 5)
	if len(h.bounds) != 3 || h.bounds[0] != 1 || h.bounds[1] != 5 || h.bounds[2] != 10 {
		t.Errorf("bounds = %v, want [1 5 10]", h.bounds)
	}
}

func TestInvalidNamePanics(t *testing.T) {
	r := NewRegistry()
	for _, bad := range []string{"", "1bad", "has-dash", "has space"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewCounter(%q) should panic", bad)
				}
			}()
			r.NewCounter(bad, "help")
		}()
	}
	// Invalid label name also panics.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("invalid label name should panic")
			}
		}()
		r.NewCounter("ok_total", "help", LabelPair{"bad-label", "v"})
	}()
}

func TestLabelValueEscaping(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("dart_x_total", "x", LabelPair{"path", `a"b\c` + "\n" + "d"}).Inc()
	out := render(t, r)
	if !strings.Contains(out, `path="a\"b\\c\nd"`) {
		t.Errorf("label not escaped:\n%s", out)
	}
}

func TestFormatFloatSpecials(t *testing.T) {
	cases := map[float64]string{
		math.Inf(1):  "+Inf",
		math.Inf(-1): "-Inf",
		1.5:          "1.5",
		0:            "0",
	}
	for in, want := range cases {
		if got := formatFloat(in); got != want {
			t.Errorf("formatFloat(%v) = %q, want %q", in, got, want)
		}
	}
	if got := formatFloat(math.NaN()); got != "NaN" {
		t.Errorf("formatFloat(NaN) = %q", got)
	}
}

// TestGaugeFuncSampledAtRender: a func metric reflects the current value on every
// scrape, which is what lets state owned elsewhere be exported without pushing
// updates.
func TestGaugeFuncSampledAtRender(t *testing.T) {
	r := NewRegistry()
	var live float64
	r.NewGaugeFunc("dart_live_blocks", "blocks right now", func() float64 { return live })

	live = 3
	if out := render(t, r); !strings.Contains(out, "dart_live_blocks 3") {
		t.Errorf("first scrape:\n%s", out)
	}
	live = 42
	out := render(t, r)
	if !strings.Contains(out, "dart_live_blocks 42") {
		t.Errorf("second scrape did not resample:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE dart_live_blocks gauge") {
		t.Errorf("wrong TYPE:\n%s", out)
	}
}

func TestCounterFuncTypedAsCounter(t *testing.T) {
	r := NewRegistry()
	var n float64 = 7
	r.NewCounterFunc("dart_sampled_total", "sampled counter", func() float64 { return n })
	out := render(t, r)
	if !strings.Contains(out, "# TYPE dart_sampled_total counter") {
		t.Errorf("wrong TYPE:\n%s", out)
	}
	if !strings.Contains(out, "dart_sampled_total 7") {
		t.Errorf("wrong value:\n%s", out)
	}
}

func TestFuncMetricsWithLabels(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeFunc("dart_tier_blocks", "blocks per tier", func() float64 { return 1 },
		LabelPair{"tier", "owned"})
	r.NewGaugeFunc("dart_tier_blocks", "blocks per tier", func() float64 { return 2 },
		LabelPair{"tier", "borrowed"})
	out := render(t, r)
	if !strings.Contains(out, `dart_tier_blocks{tier="owned"} 1`) ||
		!strings.Contains(out, `dart_tier_blocks{tier="borrowed"} 2`) {
		t.Errorf("label series wrong:\n%s", out)
	}
	if n := strings.Count(out, "# TYPE dart_tier_blocks"); n != 1 {
		t.Errorf("TYPE emitted %d times, want 1", n)
	}
}

func TestFuncMetricsRejectNilAndBadNames(t *testing.T) {
	r := NewRegistry()
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"nil gauge fn", func() { r.NewGaugeFunc("ok_gauge", "h", nil) }},
		{"nil counter fn", func() { r.NewCounterFunc("ok_total", "h", nil) }},
		{"bad gauge name", func() { r.NewGaugeFunc("1bad", "h", func() float64 { return 0 }) }},
		{"bad counter name", func() { r.NewCounterFunc("has-dash", "h", func() float64 { return 0 }) }},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s should panic", tc.name)
				}
			}()
			tc.call()
		}()
	}
}

func TestEmptyRegistry(t *testing.T) {
	if out := render(t, NewRegistry()); out != "" {
		t.Errorf("empty registry rendered %q", out)
	}
}

// TestConcurrent exercises all metric types plus rendering under load (-race).
func TestConcurrent(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("dart_c_total", "c")
	g := r.NewGauge("dart_g", "g")
	h := r.NewHistogram("dart_h_seconds", "h", []float64{0.5, 1})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Inc()
				g.Set(float64(j))
				h.Observe(float64(j%3) * 0.4)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			_ = render(t, r)
		}
	}()
	wg.Wait()

	if c.Value() != 8000 {
		t.Errorf("counter = %d, want 8000", c.Value())
	}
	_, _, total := h.snapshot()
	if total != 8000 {
		t.Errorf("histogram count = %d, want 8000", total)
	}
}
