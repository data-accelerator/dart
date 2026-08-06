package engine

import (
	"time"

	"github.com/data-accelerator/dart/internal/metrics"
	"github.com/data-accelerator/dart/internal/peer"
	"github.com/data-accelerator/dart/internal/store"
)

// Metrics holds the engine's instrumentation. Build one with NewMetrics and pass
// it in Options; a nil *Metrics disables instrumentation (all record* helpers
// become no-ops), so tests and embedders need not wire a registry.
type Metrics struct {
	// Where a served block came from.
	cacheHits *metrics.Counter
	peerHits  *metrics.Counter
	originGet *metrics.Counter

	// Bytes served to local clients and pulled from each source.
	clientBytes *metrics.Counter
	peerBytes   *metrics.Counter
	originBytes *metrics.Counter

	// Relay activity (this node fetching on behalf of a peer).
	relayServed *metrics.Counter
	relayMiss   *metrics.Counter

	// Hedging: how often a duplicate was raced, and who won.
	hedgeFired     *metrics.Counter
	hedgeWonPrim   *metrics.Counter
	hedgeWonBackup *metrics.Counter
	// Failover: a definite peer failure escalated to the next ancestor. Counted
	// separately from hedging because it is reactive rather than speculative and
	// is deliberately not rate limited.
	failover *metrics.Counter

	// Latency of a block fetch, by source.
	peerLatency   *metrics.Histogram
	originLatency *metrics.Histogram
}

// blockLatencyBounds are the default histogram bounds (seconds) for a block
// fetch: sub-ms cache-ish hits through multi-second origin stalls.
var blockLatencyBounds = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10}

// NewMetrics registers the engine's metrics on r and returns the handle.
func NewMetrics(r *metrics.Registry) *Metrics {
	return &Metrics{
		cacheHits: r.NewCounter("dart_block_source_total",
			"blocks served, by source", metrics.LabelPair{Name: "source", Value: "cache"}),
		peerHits: r.NewCounter("dart_block_source_total",
			"blocks served, by source", metrics.LabelPair{Name: "source", Value: "peer"}),
		originGet: r.NewCounter("dart_block_source_total",
			"blocks served, by source", metrics.LabelPair{Name: "source", Value: "origin"}),

		clientBytes: r.NewCounter("dart_bytes_total",
			"bytes transferred, by direction", metrics.LabelPair{Name: "direction", Value: "client"}),
		peerBytes: r.NewCounter("dart_bytes_total",
			"bytes transferred, by direction", metrics.LabelPair{Name: "direction", Value: "peer_in"}),
		originBytes: r.NewCounter("dart_bytes_total",
			"bytes transferred, by direction", metrics.LabelPair{Name: "direction", Value: "origin_in"}),

		relayServed: r.NewCounter("dart_relay_total",
			"relay requests handled for peers", metrics.LabelPair{Name: "result", Value: "served"}),
		relayMiss: r.NewCounter("dart_relay_total",
			"relay requests handled for peers", metrics.LabelPair{Name: "result", Value: "declined"}),

		hedgeFired: r.NewCounter("dart_hedge_total",
			"hedged peer fetches", metrics.LabelPair{Name: "event", Value: "fired"}),
		hedgeWonPrim: r.NewCounter("dart_hedge_total",
			"hedged peer fetches", metrics.LabelPair{Name: "event", Value: "primary_won"}),
		hedgeWonBackup: r.NewCounter("dart_hedge_total",
			"hedged peer fetches", metrics.LabelPair{Name: "event", Value: "backup_won"}),

		failover: r.NewCounter("dart_peer_failover_total",
			"definite peer failures escalated to the next ancestor"),

		peerLatency: r.NewHistogram("dart_block_fetch_seconds",
			"block fetch latency by source", blockLatencyBounds,
			metrics.LabelPair{Name: "source", Value: "peer"}),
		originLatency: r.NewHistogram("dart_block_fetch_seconds",
			"block fetch latency by source", blockLatencyBounds,
			metrics.LabelPair{Name: "source", Value: "origin"}),
	}
}

func (m *Metrics) recordCacheHit() {
	if m == nil {
		return
	}
	m.cacheHits.Inc()
}

func (m *Metrics) recordPeerHit(n int, d time.Duration) {
	if m == nil {
		return
	}
	m.peerHits.Inc()
	m.peerBytes.Add(int64(n))
	m.peerLatency.Observe(d.Seconds())
}

// recordOrigin records a block read satisfied from upstream.
//
// coalesced means this caller rode along on a fetch another caller had already
// started. The read still counts — it is a real miss and belongs in the hit ratio,
// and the latency is what this caller genuinely waited — but the bytes must not,
// because they crossed the network once, not once per waiter. Counting them per
// caller would report singleflight's savings as extra traffic.
func (m *Metrics) recordOrigin(n int, d time.Duration, coalesced bool) {
	if m == nil {
		return
	}
	m.originGet.Inc()
	if !coalesced {
		m.originBytes.Add(int64(n))
	}
	m.originLatency.Observe(d.Seconds())
}

func (m *Metrics) recordClientBytes(n int) {
	if m == nil {
		return
	}
	m.clientBytes.Add(int64(n))
}

func (m *Metrics) recordRelay(served bool) {
	if m == nil {
		return
	}
	if served {
		m.relayServed.Inc()
	} else {
		m.relayMiss.Inc()
	}
}

// recordHedge counts a duplicate request actually being launched.
func (m *Metrics) recordHedge() {
	if m == nil {
		return
	}
	m.hedgeFired.Inc()
}

// recordHedgeWin counts which contender served the block. Comparing
// backup_won against fired shows whether hedging is paying for itself.
func (m *Metrics) recordHedgeWin(primary bool) {
	if m == nil {
		return
	}
	if primary {
		m.hedgeWonPrim.Inc()
	} else {
		m.hedgeWonBackup.Inc()
	}
}

// recordFailover counts a reactive escalation after a definite peer failure.
func (m *Metrics) recordFailover() {
	if m == nil {
		return
	}
	m.failover.Inc()
}

// RegisterStoreMetrics exports a tiered store's occupancy on r.
//
// These are sampled at scrape time (NewGaugeFunc) rather than pushed, because
// occupancy is naturally a snapshot and the store should not have to notify a
// metrics registry on every insert and eviction on the hot path.
func RegisterStoreMetrics(r *metrics.Registry, s store.ClassStore) {
	if r == nil || s == nil {
		return
	}
	const (
		blocksHelp = "cached blocks by cache class"
		slotsHelp  = "capacity in blocks by cache class"
	)
	class := func(v string) metrics.LabelPair { return metrics.LabelPair{Name: "class", Value: v} }

	r.NewGaugeFunc("dart_store_blocks", blocksHelp,
		func() float64 { return float64(s.Stats().OwnedBlocks) }, class("owned"))
	r.NewGaugeFunc("dart_store_blocks", blocksHelp,
		func() float64 { return float64(s.Stats().BorrowedBlocks) }, class("borrowed"))
	r.NewGaugeFunc("dart_store_blocks", blocksHelp,
		func() float64 { return float64(s.Stats().MemBlocks) }, class("mem"))

	r.NewGaugeFunc("dart_store_slots", slotsHelp,
		func() float64 { return float64(s.Stats().OwnedSlots) }, class("owned"))
	r.NewGaugeFunc("dart_store_slots", slotsHelp,
		func() float64 { return float64(s.Stats().BorrowedSlots) }, class("borrowed"))
	r.NewGaugeFunc("dart_store_slots", slotsHelp,
		func() float64 { return float64(s.Stats().MemSlots) }, class("mem"))

	// A rising rejection count against a full borrowed budget is the signal that
	// admission is doing its job (or that the budget is too small).
	r.NewCounterFunc("dart_store_admit_rejected_total",
		"borrowed candidates refused by TinyLFU admission",
		func() float64 { return float64(s.Stats().AdmitRejected) })
}

// RegisterPeerMetrics exports peer circuit-breaker state on r.
func RegisterPeerMetrics(r *metrics.Registry, b *peer.Breaker) {
	if r == nil || b == nil {
		return
	}
	r.NewGaugeFunc("dart_peer_circuits_open",
		"peers whose circuit is currently open",
		func() float64 { return float64(b.OpenCount()) })
}
