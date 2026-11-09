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
	temporaryBuffers, temporaryBytes *metrics.Counter
	receiveLoads, receiveBytes       *metrics.Gauge
	receiveRejected                  *metrics.Counter
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
	failover                                          *metrics.Counter
	peerFetchRoot, peerFetchParent, peerFetchAncestor *metrics.Counter
	readerSnapshotSize                                *metrics.Histogram

	// Passthrough: requests proxied verbatim because the origin ignores Range.
	passthrough *metrics.Counter

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
		peerFetchRoot:      r.NewCounter("dart_peer_fetch_total", "peer block HTTP attempts dispatched by topology target", metrics.LabelPair{Name: "target", Value: "root"}),
		peerFetchParent:    r.NewCounter("dart_peer_fetch_total", "peer block HTTP attempts dispatched by topology target", metrics.LabelPair{Name: "target", Value: "parent"}),
		peerFetchAncestor:  r.NewCounter("dart_peer_fetch_total", "peer block HTTP attempts dispatched by topology target", metrics.LabelPair{Name: "target", Value: "ancestor"}),
		readerSnapshotSize: r.NewHistogram("dart_reader_snapshot_size", "published reader count in successful tracker JOIN responses, excluding cache hits and coalesced waiters", []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}),
		temporaryBuffers:   r.NewCounter("dart_temporary_buffers_total", "temporary block receive buffers"),
		temporaryBytes:     r.NewCounter("dart_temporary_buffer_bytes_total", "bytes allocated for temporary block receive buffers"),
		receiveLoads:       r.NewGauge("dart_receive_buffers", "live temporary block receive buffers including retained results"),
		receiveBytes:       r.NewGauge("dart_receive_bytes", "live temporary block receive payload bytes including retained results"),
		receiveRejected:    r.NewCounter("dart_receive_budget_rejected_total", "temporary receive admissions declined, including skipped speculation"),
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

		passthrough: r.NewCounter("dart_passthrough_total",
			"requests proxied verbatim without block caching",
			metrics.LabelPair{Name: "reason", Value: "range_unsupported"}),

		peerLatency: r.NewHistogram("dart_block_fetch_seconds",
			"block fetch latency by source", blockLatencyBounds,
			metrics.LabelPair{Name: "source", Value: "peer"}),
		originLatency: r.NewHistogram("dart_block_fetch_seconds",
			"block fetch latency by source", blockLatencyBounds,
			metrics.LabelPair{Name: "source", Value: "origin"}),
	}
}

func (m *Metrics) recordPeerFetch(root, parent bool) {
	if m == nil {
		return
	}
	switch {
	case root:
		m.peerFetchRoot.Inc()
	case parent:
		m.peerFetchParent.Inc()
	default:
		m.peerFetchAncestor.Inc()
	}
}

func (m *Metrics) recordReaderSnapshot(n int) {
	if m != nil {
		m.readerSnapshotSize.Observe(float64(n))
	}
}

func (m *Metrics) recordCacheHit() {
	if m == nil {
		return
	}
	m.cacheHits.Inc()
}

// Record one successful serving read, independently of who executed the load.
// The duration includes waiting, retries and publication. Ingress bytes are
// recorded by the validated producer before a caller can abandon its result.
func (m *Metrics) recordBlockRead(r blockRead, d time.Duration) {
	switch r.source {
	case blockFromPeer:
		m.recordPeerHit(0, d)
	case blockFromOrigin:
		m.recordOrigin(0, d, false)
	default:
		m.recordCacheHit()
	}
}

// Count fully received, validated payloads, not partial/invalid wire traffic.
// Both completed hedge contenders count; a canceled result handoff loses none.
func (m *Metrics) recordIngress(source blockSource, n int) {
	if m == nil {
		return
	}
	switch source {
	case blockFromPeer:
		m.peerBytes.Add(int64(n))
	case blockFromOrigin:
		m.originBytes.Add(int64(n))
	}
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

// recordPassthrough counts a verbatim-proxied request and its bytes. The
// bytes count as client-served and either origin-pulled or peer-pulled for
// non-root delegation. They do not count as block sources: no block was fetched.
func (m *Metrics) recordPassthrough(n int64, delegated bool) {
	if m == nil {
		return
	}
	m.passthrough.Inc()
	m.clientBytes.Add(n)
	if delegated {
		m.peerBytes.Add(n)
	} else {
		m.originBytes.Add(n)
	}
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
	r.NewCounterFunc("dart_store_slot_wait_total", "memory slot reservation waits", func() float64 { return float64(s.Stats().SlotWaits) })
	r.NewCounterFunc("dart_store_write_bypass_total", "memory fills bypassed by pressure or admission", func() float64 { return float64(s.Stats().WriteBypasses) })
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

func (m *Metrics) recordTemporary(n int64) {
	if m != nil {
		m.temporaryBuffers.Inc()
		m.temporaryBytes.Add(n)
	}
}
