// Package metrics is DART's dependency-free Prometheus exporter: a small set of
// concurrency-safe metric types (Counter, Gauge, Histogram) and a Registry that
// renders them in the Prometheus text exposition format.
//
// It avoids pulling in the official client library so the binary stays small and
// the data path has no third-party allocation behavior. The rendered output is
// standard text format (version 0.0.4), so any Prometheus scraper accepts it.
//
// Metric and label names must be valid Prometheus identifiers; the Registry
// validates them at registration time.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

// Inc adds 1.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n (n must be >= 0; negative values are ignored).
func (c *Counter) Add(n int64) {
	if n > 0 {
		c.v.Add(uint64(n))
	}
}

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that can go up and down.
type Gauge struct{ bits atomic.Uint64 }

// Set sets the gauge to v.
func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }

// Value returns the current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Histogram observes a distribution over fixed upper bounds (cumulative
// buckets, as Prometheus expects), plus a sum and count.
type Histogram struct {
	bounds []float64 // sorted, exclusive of +Inf

	mu     sync.Mutex
	counts []uint64 // len(bounds)+1; last is the +Inf overflow bucket
	sum    float64
	total  uint64
}

// NewHistogram creates a Histogram with the given upper bounds (they are sorted
// and de-duplicated; +Inf is implicit).
func NewHistogram(bounds ...float64) *Histogram {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	// de-duplicate
	out := b[:0]
	for i, v := range b {
		if i == 0 || v != b[i-1] {
			out = append(out, v)
		}
	}
	return &Histogram{bounds: out, counts: make([]uint64, len(out)+1)}
}

// Observe records one sample. Prometheus buckets are upper-inclusive (le), so
// the sample belongs to the first bucket whose bound is >= v; samples above all
// bounds land in the implicit +Inf bucket (index len(bounds)).
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v)
	h.mu.Lock()
	h.counts[i]++
	h.sum += v
	h.total++
	h.mu.Unlock()
}

// snapshot returns cumulative bucket counts, the sum, and the total count.
func (h *Histogram) snapshot() (cum []uint64, sum float64, total uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cum = make([]uint64, len(h.counts))
	var running uint64
	for i, c := range h.counts {
		running += c
		cum[i] = running
	}
	return cum, h.sum, h.total
}

// metric is a registered metric with its metadata.
type metric struct {
	name   string
	help   string
	kind   string // "counter", "gauge", "histogram"
	labels []string
	values []string
	c      *Counter
	g      *Gauge
	h      *Histogram
	// fn, when non-nil, is sampled at render time instead of reading c/g. It is
	// how state owned elsewhere (cache occupancy, circuit states) is exported
	// without that state having to push updates on every change.
	fn func() float64
}

// Registry holds metrics and renders them in the Prometheus text format. It is
// safe for concurrent use.
type Registry struct {
	mu sync.Mutex
	ms []*metric
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry { return &Registry{} }

// ErrInvalidName is returned when a metric or label name is not a valid
// Prometheus identifier.
type ErrInvalidName struct{ Name string }

func (e ErrInvalidName) Error() string { return "metrics: invalid name " + strconv.Quote(e.Name) }

// validName reports whether s is a valid Prometheus metric/label name:
// [a-zA-Z_][a-zA-Z0-9_]*
func validName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// register validates and appends a metric.
func (r *Registry) register(m *metric) error {
	if !validName(m.name) {
		return ErrInvalidName{m.name}
	}
	for _, l := range m.labels {
		if !validName(l) {
			return ErrInvalidName{l}
		}
	}
	if len(m.labels) != len(m.values) {
		return fmt.Errorf("metrics: %s has %d labels but %d values", m.name, len(m.labels), len(m.values))
	}
	r.mu.Lock()
	r.ms = append(r.ms, m)
	r.mu.Unlock()
	return nil
}

// LabelPair is one label name/value.
type LabelPair struct{ Name, Value string }

func split(pairs []LabelPair) (names, values []string) {
	for _, p := range pairs {
		names = append(names, p.Name)
		values = append(values, p.Value)
	}
	return names, values
}

// NewCounter registers and returns a Counter. It panics on an invalid name
// (a programming error, checked at startup).
func (r *Registry) NewCounter(name, help string, labels ...LabelPair) *Counter {
	c := &Counter{}
	n, v := split(labels)
	if err := r.register(&metric{name: name, help: help, kind: "counter", labels: n, values: v, c: c}); err != nil {
		panic(err)
	}
	return c
}

// NewGauge registers and returns a Gauge. It panics on an invalid name.
func (r *Registry) NewGauge(name, help string, labels ...LabelPair) *Gauge {
	g := &Gauge{}
	n, v := split(labels)
	if err := r.register(&metric{name: name, help: help, kind: "gauge", labels: n, values: v, g: g}); err != nil {
		panic(err)
	}
	return g
}

// NewHistogram registers and returns a Histogram. It panics on an invalid name.
func (r *Registry) NewHistogram(name, help string, bounds []float64, labels ...LabelPair) *Histogram {
	h := NewHistogram(bounds...)
	n, v := split(labels)
	if err := r.register(&metric{name: name, help: help, kind: "histogram", labels: n, values: v, h: h}); err != nil {
		panic(err)
	}
	return h
}

// NewGaugeFunc registers a gauge whose value is sampled by fn at render time.
//
// Use this for state that lives elsewhere and is naturally read as a snapshot —
// cache occupancy, open circuits — so the owning component does not have to
// push an update on every mutation. fn must be cheap and safe for concurrent
// use, because it runs on the scrape path. It panics on an invalid name.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64, labels ...LabelPair) {
	if fn == nil {
		panic("metrics: NewGaugeFunc requires a non-nil fn")
	}
	n, v := split(labels)
	if err := r.register(&metric{name: name, help: help, kind: "gauge", labels: n, values: v, fn: fn}); err != nil {
		panic(err)
	}
}

// NewCounterFunc is NewGaugeFunc for a value that only increases, so it is typed
// as a counter. fn must return a monotonically non-decreasing value.
func (r *Registry) NewCounterFunc(name, help string, fn func() float64, labels ...LabelPair) {
	if fn == nil {
		panic("metrics: NewCounterFunc requires a non-nil fn")
	}
	n, v := split(labels)
	if err := r.register(&metric{name: name, help: help, kind: "counter", labels: n, values: v, fn: fn}); err != nil {
		panic(err)
	}
}

// escapeLabelValue escapes a label value per the text format.
// escapeHelp escapes a HELP string per the Prometheus text format: backslash
// and line feed only (quotes need no escaping outside label values).
func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

// labelString renders {a="1",b="2"} or "" when there are no labels, optionally
// appending one extra pair (used for histogram le=).
func labelString(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(names[i])
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(values[i]))
		b.WriteByte('"')
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraName)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(extraValue))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// formatFloat renders a float in a Prometheus-acceptable form.
func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// Render writes all metrics in the Prometheus text exposition format. HELP and
// TYPE lines are emitted once per metric name (the first occurrence).
func (r *Registry) Render(w io.Writer) error {
	r.mu.Lock()
	ms := append([]*metric(nil), r.ms...)
	r.mu.Unlock()

	var b strings.Builder
	seen := map[string]bool{}
	for _, m := range ms {
		if !seen[m.name] {
			seen[m.name] = true
			if m.help != "" {
				// HELP must be escaped like a label value: a literal newline
				// would split the exposition line, a bare backslash is a
				// format error (Prometheus text format, escaping rules).
				fmt.Fprintf(&b, "# HELP %s %s\n", m.name, escapeHelp(m.help))
			}
			fmt.Fprintf(&b, "# TYPE %s %s\n", m.name, m.kind)
		}
		ls := labelString(m.labels, m.values, "", "")
		switch m.kind {
		case "counter":
			if m.fn != nil {
				fmt.Fprintf(&b, "%s%s %s\n", m.name, ls, formatFloat(m.fn()))
			} else {
				fmt.Fprintf(&b, "%s%s %d\n", m.name, ls, m.c.Value())
			}
		case "gauge":
			v := m.fn
			if v == nil {
				v = m.g.Value
			}
			fmt.Fprintf(&b, "%s%s %s\n", m.name, ls, formatFloat(v()))
		case "histogram":
			cum, sum, total := m.h.snapshot()
			for i, bound := range m.h.bounds {
				fmt.Fprintf(&b, "%s_bucket%s %d\n", m.name,
					labelString(m.labels, m.values, "le", formatFloat(bound)), cum[i])
			}
			fmt.Fprintf(&b, "%s_bucket%s %d\n", m.name,
				labelString(m.labels, m.values, "le", "+Inf"), total)
			fmt.Fprintf(&b, "%s_sum%s %s\n", m.name, ls, formatFloat(sum))
			fmt.Fprintf(&b, "%s_count%s %d\n", m.name, ls, total)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}
