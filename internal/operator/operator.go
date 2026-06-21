// Package operator defines the streaming operator interfaces and built-in implementations.
//
// Operators are the processing nodes in the pipeline DAG. Each operator:
//   - Receives events from upstream operators via input channels
//   - Applies a transformation or aggregation
//   - Emits results to downstream operators via output channels
//
// Built-in operators:
//   - Map:       Transform each event (1-to-1)
//   - Filter:    Drop events that don't match a predicate
//   - FlatMap:   Transform each event to 0 or more events (1-to-N)
//   - KeyBy:     Partition events by key for stateful processing
//   - Reduce:    Stateful aggregation per key
//   - Window:    Group events into time windows and aggregate
//   - Join:      Join two streams on a key within a time window
//
// The Volcano/iterator model is used: each operator pulls from upstream
// when downstream asks for the next event. This is the same model used
// by Apache Flink and Spark Streaming.
package operator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/internal/window"
	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// Operator is the base interface for all stream processing operators.
type Operator interface {
	// Name returns the operator's name for logging and metrics.
	Name() string
	// Process processes an event and emits 0 or more output events.
	Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error
	// OnWatermark is called when a watermark advances.
	OnWatermark(ctx context.Context, wm *event.Watermark, emit func(*event.Event)) error
}

// ---- Map Operator ----

// MapFn is a function that transforms one event to another.
type MapFn func(*event.Event) (*event.Event, error)

// MapOperator applies a transformation to each event.
type MapOperator struct {
	name string
	fn   MapFn
	// Metrics
	processed int64
	errors    int64
}

// NewMap creates a map operator.
func NewMap(name string, fn MapFn) *MapOperator {
	return &MapOperator{name: name, fn: fn}
}

func (o *MapOperator) Name() string { return o.name }

func (o *MapOperator) Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error {
	result, err := o.fn(e)
	if err != nil {
		atomic.AddInt64(&o.errors, 1)
		return err
	}
	if result != nil {
		emit(result)
	}
	atomic.AddInt64(&o.processed, 1)
	return nil
}

func (o *MapOperator) OnWatermark(_ context.Context, wm *event.Watermark, emit func(*event.Event)) error {
	// Map operators pass watermarks through unchanged
	emit(&event.Event{Value: wm, EventTime: wm.Timestamp})
	return nil
}

// ---- Filter Operator ----

// FilterFn returns true if the event should be kept.
type FilterFn func(*event.Event) bool

// FilterOperator drops events that don't match the predicate.
type FilterOperator struct {
	name    string
	fn      FilterFn
	kept    int64
	dropped int64
}

// NewFilter creates a filter operator.
func NewFilter(name string, fn FilterFn) *FilterOperator {
	return &FilterOperator{name: name, fn: fn}
}

func (o *FilterOperator) Name() string { return o.name }

func (o *FilterOperator) Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error {
	if o.fn(e) {
		emit(e)
		atomic.AddInt64(&o.kept, 1)
	} else {
		atomic.AddInt64(&o.dropped, 1)
	}
	return nil
}

func (o *FilterOperator) OnWatermark(_ context.Context, wm *event.Watermark, emit func(*event.Event)) error {
	emit(&event.Event{Value: wm, EventTime: wm.Timestamp})
	return nil
}

// ---- FlatMap Operator ----

// FlatMapFn transforms one event to zero or more events.
type FlatMapFn func(*event.Event) ([]*event.Event, error)

// FlatMapOperator applies a 1-to-N transformation.
type FlatMapOperator struct {
	name string
	fn   FlatMapFn
}

// NewFlatMap creates a flatmap operator.
func NewFlatMap(name string, fn FlatMapFn) *FlatMapOperator {
	return &FlatMapOperator{name: name, fn: fn}
}

func (o *FlatMapOperator) Name() string { return o.name }

func (o *FlatMapOperator) Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error {
	results, err := o.fn(e)
	if err != nil {
		return err
	}
	for _, r := range results {
		if r != nil {
			emit(r)
		}
	}
	return nil
}

func (o *FlatMapOperator) OnWatermark(_ context.Context, wm *event.Watermark, emit func(*event.Event)) error {
	emit(&event.Event{Value: wm, EventTime: wm.Timestamp})
	return nil
}

// ---- Reduce Operator ----

// ReduceFn combines two events into one (stateful aggregation per key).
type ReduceFn func(accumulator, event *event.Event) (*event.Event, error)

// ReduceOperator maintains per-key state and reduces events.
type ReduceOperator struct {
	mu    sync.RWMutex
	name  string
	fn    ReduceFn
	state map[string]*event.Event // key -> current accumulator
}

// NewReduce creates a reduce operator.
func NewReduce(name string, fn ReduceFn) *ReduceOperator {
	return &ReduceOperator{
		name:  name,
		fn:    fn,
		state: make(map[string]*event.Event),
	}
}

func (o *ReduceOperator) Name() string { return o.name }

func (o *ReduceOperator) Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	acc, exists := o.state[e.Key]
	if !exists {
		o.state[e.Key] = e
		emit(e)
		return nil
	}

	result, err := o.fn(acc, e)
	if err != nil {
		return err
	}

	if result != nil {
		o.state[e.Key] = result
		emit(result)
	}

	return nil
}

func (o *ReduceOperator) OnWatermark(_ context.Context, wm *event.Watermark, emit func(*event.Event)) error {
	emit(&event.Event{Value: wm, EventTime: wm.Timestamp})
	return nil
}

// GetState returns a copy of the current state (for checkpointing).
func (o *ReduceOperator) GetState() map[string]*event.Event {
	o.mu.RLock()
	defer o.mu.RUnlock()
	state := make(map[string]*event.Event, len(o.state))
	for k, v := range o.state {
		state[k] = v.Clone()
	}
	return state
}

// ---- Window Operator ----

// WindowAggFn aggregates events in a window into a result event.
type WindowAggFn func(result *window.WindowResult) (*event.Event, error)

// WindowOperator groups events into time windows and aggregates them.
type WindowOperator struct {
	name     string
	assigner window.Assigner
	aggFn    WindowAggFn
	buffer   *window.WindowBuffer
	mu       sync.Mutex
}

// NewWindowOp creates a window operator.
func NewWindowOp(name string, assigner window.Assigner, aggFn WindowAggFn) *WindowOperator {
	return &WindowOperator{
		name:     name,
		assigner: assigner,
		aggFn:    aggFn,
		buffer:   window.NewWindowBuffer(),
	}
}

func (o *WindowOperator) Name() string { return o.name }

func (o *WindowOperator) Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error {
	windows := o.assigner.AssignWindows(e)
	o.buffer.Add(e, windows)
	return nil
}

func (o *WindowOperator) OnWatermark(ctx context.Context, wm *event.Watermark, emit func(*event.Event)) error {
	// Trigger all windows that have expired
	results := o.buffer.TriggerExpired(wm.Timestamp)
	for _, result := range results {
		out, err := o.aggFn(result)
		if err != nil {
			return err
		}
		if out != nil {
			emit(out)
		}
	}
	// Pass watermark downstream
	emit(&event.Event{Value: wm, EventTime: wm.Timestamp})
	return nil
}

// ---- Deduplication Operator ----

// DeduplicateOperator removes duplicate events based on a key within a time window.
type DeduplicateOperator struct {
	mu      sync.Mutex
	name    string
	seen    map[string]time.Time // key -> first seen time
	ttl     time.Duration
	dedups  int64
}

// NewDeduplicate creates a deduplication operator.
func NewDeduplicate(name string, ttl time.Duration) *DeduplicateOperator {
	return &DeduplicateOperator{
		name: name,
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}
}

func (o *DeduplicateOperator) Name() string { return o.name }

func (o *DeduplicateOperator) Process(ctx context.Context, e *event.Event, emit func(*event.Event)) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Clean up expired entries
	now := time.Now()
	for k, t := range o.seen {
		if now.Sub(t) > o.ttl {
			delete(o.seen, k)
		}
	}

	if _, seen := o.seen[e.Key]; seen {
		atomic.AddInt64(&o.dedups, 1)
		return nil // Duplicate — drop it
	}

	o.seen[e.Key] = now
	emit(e)
	return nil
}

func (o *DeduplicateOperator) OnWatermark(_ context.Context, wm *event.Watermark, emit func(*event.Event)) error {
	emit(&event.Event{Value: wm, EventTime: wm.Timestamp})
	return nil
}

// Stats returns operator statistics.
func (o *DeduplicateOperator) Stats() map[string]int64 {
	return map[string]int64{
		"deduplicated": atomic.LoadInt64(&o.dedups),
	}
}
