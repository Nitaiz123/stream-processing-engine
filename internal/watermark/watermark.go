// Package watermark implements watermark generation and tracking.
//
// Watermarks are the mechanism by which a stream processing system tracks
// progress in event time and decides when windows are complete.
//
// The key challenge: events can arrive out of order. A sensor reading from
// 14:00:00 might arrive at the pipeline at 14:00:05 because of network delays.
// If we close a window at 14:00:00 processing time, we'd miss that event.
//
// Watermarks solve this by saying: "I've seen all events up to time T - delay."
// The delay (maxLag) is a configurable bound on how late events can arrive.
//
// Watermark strategies:
//   - BoundedOutOfOrder: watermark = max_event_time - max_lag
//   - MonotonousTimestamps: watermark = max_event_time (no lag)
//   - PeriodicWatermark: advance watermark at fixed wall-clock intervals
//
// References:
//   - Streaming Systems (Tyler Akidau et al., 2018)
//   - Apache Flink watermark documentation
package watermark

import (
	"sync"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// Generator generates watermarks from a stream of events.
type Generator interface {
	// OnEvent is called for each event to potentially advance the watermark.
	OnEvent(e *event.Event) *event.Watermark
	// OnPeriodicEmit is called periodically to emit watermarks.
	OnPeriodicEmit() *event.Watermark
	// CurrentWatermark returns the current watermark timestamp.
	CurrentWatermark() time.Time
}

// BoundedOutOfOrderGenerator generates watermarks for streams with bounded
// out-of-order events.
//
// Watermark = max(event_time) - maxLag
//
// This is the most common strategy in production systems. It allows events
// to arrive up to maxLag late while still producing correct results.
type BoundedOutOfOrderGenerator struct {
	mu             sync.Mutex
	maxLag         time.Duration
	maxEventTime   time.Time
	lastWatermark  time.Time
	emitInterval   time.Duration
	lastEmitTime   time.Time
}

// NewBoundedOutOfOrder creates a watermark generator that tolerates events
// arriving up to maxLag late.
func NewBoundedOutOfOrder(maxLag, emitInterval time.Duration) *BoundedOutOfOrderGenerator {
	return &BoundedOutOfOrderGenerator{
		maxLag:       maxLag,
		emitInterval: emitInterval,
	}
}

func (g *BoundedOutOfOrderGenerator) OnEvent(e *event.Event) *event.Watermark {
	g.mu.Lock()
	defer g.mu.Unlock()

	if e.EventTime.After(g.maxEventTime) {
		g.maxEventTime = e.EventTime
	}
	return nil // Watermarks are emitted periodically, not per-event
}

func (g *BoundedOutOfOrderGenerator) OnPeriodicEmit() *event.Watermark {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	if now.Sub(g.lastEmitTime) < g.emitInterval {
		return nil
	}
	g.lastEmitTime = now

	if g.maxEventTime.IsZero() {
		return nil
	}

	wm := g.maxEventTime.Add(-g.maxLag)
	if wm.After(g.lastWatermark) {
		g.lastWatermark = wm
		return &event.Watermark{Timestamp: wm}
	}
	return nil
}

func (g *BoundedOutOfOrderGenerator) CurrentWatermark() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lastWatermark
}

// MonotonousGenerator assumes events arrive in order (no late data).
// Watermark = max(event_time)
type MonotonousGenerator struct {
	mu           sync.Mutex
	maxEventTime time.Time
	emitInterval time.Duration
	lastEmitTime time.Time
}

func NewMonotonous(emitInterval time.Duration) *MonotonousGenerator {
	return &MonotonousGenerator{emitInterval: emitInterval}
}

func (g *MonotonousGenerator) OnEvent(e *event.Event) *event.Watermark {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e.EventTime.After(g.maxEventTime) {
		g.maxEventTime = e.EventTime
	}
	return nil
}

func (g *MonotonousGenerator) OnPeriodicEmit() *event.Watermark {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if now.Sub(g.lastEmitTime) < g.emitInterval {
		return nil
	}
	g.lastEmitTime = now
	if g.maxEventTime.IsZero() {
		return nil
	}
	return &event.Watermark{Timestamp: g.maxEventTime}
}

func (g *MonotonousGenerator) CurrentWatermark() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxEventTime
}

// Tracker tracks the minimum watermark across multiple parallel sources.
//
// In a parallel pipeline, each source partition generates its own watermark.
// The global watermark is the minimum of all partition watermarks — we can
// only advance the global watermark when ALL partitions have advanced.
//
// This is the same approach used by Apache Flink's watermark propagation.
type Tracker struct {
	mu         sync.RWMutex
	watermarks map[int]time.Time // partition -> watermark
	global     time.Time
}

// NewTracker creates a watermark tracker for the given number of partitions.
func NewTracker(numPartitions int) *Tracker {
	t := &Tracker{
		watermarks: make(map[int]time.Time, numPartitions),
	}
	return t
}

// Update updates the watermark for a partition and returns the new global watermark.
func (t *Tracker) Update(partition int, wm time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.watermarks[partition] = wm

	// Global watermark = min of all partition watermarks
	var minWm time.Time
	for _, w := range t.watermarks {
		if minWm.IsZero() || w.Before(minWm) {
			minWm = w
		}
	}

	if minWm.After(t.global) {
		t.global = minWm
	}

	return t.global
}

// Global returns the current global watermark.
func (t *Tracker) Global() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.global
}

// IsLate returns true if an event's time is before the current watermark.
func (t *Tracker) IsLate(eventTime time.Time) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return !t.global.IsZero() && eventTime.Before(t.global)
}
