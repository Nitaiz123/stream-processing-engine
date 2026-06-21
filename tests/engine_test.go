package tests

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/internal/checkpoint"
	"github.com/Nitaiz123/stream-processing-engine/internal/operator"
	"github.com/Nitaiz123/stream-processing-engine/internal/pipeline"
	"github.com/Nitaiz123/stream-processing-engine/internal/sink"
	"github.com/Nitaiz123/stream-processing-engine/internal/source"
	"github.com/Nitaiz123/stream-processing-engine/internal/watermark"
	"github.com/Nitaiz123/stream-processing-engine/internal/window"
	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// ---- Event Tests ----

func TestEventCreation(t *testing.T) {
	e := event.New("user-1", 42)
	if e.Key != "user-1" {
		t.Errorf("expected key user-1, got %s", e.Key)
	}
	if e.Value != 42 {
		t.Errorf("expected value 42, got %v", e.Value)
	}
	if e.EventTime.IsZero() {
		t.Error("expected non-zero event time")
	}
}

func TestEventWithTime(t *testing.T) {
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	e := event.NewWithTime("sensor-1", 99.5, ts)
	if !e.EventTime.Equal(ts) {
		t.Errorf("expected event time %v, got %v", ts, e.EventTime)
	}
}

func TestEventClone(t *testing.T) {
	e := event.New("key", "value")
	e.Headers = map[string]string{"x-trace": "abc123"}
	clone := e.Clone()

	if clone.Key != e.Key {
		t.Error("clone key mismatch")
	}
	// Modifying clone headers should not affect original
	clone.Headers["new-header"] = "new-value"
	if _, ok := e.Headers["new-header"]; ok {
		t.Error("modifying clone headers affected original")
	}
}

// ---- Window Tests ----

func TestTumblingWindowAssignment(t *testing.T) {
	assigner := window.NewTumbling(time.Minute)

	t0 := time.Date(2024, 1, 1, 12, 0, 30, 0, time.UTC)
	e := event.NewWithTime("k", 1, t0)
	windows := assigner.AssignWindows(e)

	if len(windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(windows))
	}

	expected := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	if !windows[0].Start.Equal(expected) {
		t.Errorf("expected window start %v, got %v", expected, windows[0].Start)
	}
	if !windows[0].End.Equal(expected.Add(time.Minute)) {
		t.Errorf("expected window end %v, got %v", expected.Add(time.Minute), windows[0].End)
	}
}

func TestSlidingWindowAssignment(t *testing.T) {
	// 5-minute window, 1-minute slide
	assigner := window.NewSliding(5*time.Minute, time.Minute)

	t0 := time.Date(2024, 1, 1, 12, 3, 0, 0, time.UTC)
	e := event.NewWithTime("k", 1, t0)
	windows := assigner.AssignWindows(e)

	// An event at 12:03 should be in windows starting at 11:59, 12:00, 12:01, 12:02, 12:03
	if len(windows) < 4 {
		t.Errorf("expected at least 4 windows for sliding(5m, 1m), got %d", len(windows))
	}
}

func TestSessionWindowMerge(t *testing.T) {
	assigner := window.NewSession(10 * time.Minute)

	// Create overlapping windows that should merge
	windows := []window.Window{
		{Start: time.Unix(0, 0), End: time.Unix(600, 0)},   // 0-10min
		{Start: time.Unix(300, 0), End: time.Unix(900, 0)}, // 5-15min (overlaps)
		{Start: time.Unix(1200, 0), End: time.Unix(1800, 0)}, // 20-30min (separate)
	}

	merged := assigner.MergeWindows(windows)
	if len(merged) != 2 {
		t.Errorf("expected 2 merged windows, got %d", len(merged))
	}
	// First merged window should span 0-15min
	if !merged[0].End.Equal(time.Unix(900, 0)) {
		t.Errorf("expected merged end at 900s, got %v", merged[0].End)
	}
}

func TestWindowExpiry(t *testing.T) {
	w := window.Window{
		Start: time.Unix(0, 0),
		End:   time.Unix(60, 0),
	}

	// Watermark before end — not expired
	if w.IsExpired(time.Unix(59, 0)) {
		t.Error("window should not be expired when watermark < end")
	}
	// Watermark at end — expired
	if !w.IsExpired(time.Unix(60, 0)) {
		t.Error("window should be expired when watermark >= end")
	}
}

func TestWindowBuffer(t *testing.T) {
	buf := window.NewWindowBuffer()
	assigner := window.NewTumbling(time.Minute)

	t0 := time.Date(2024, 1, 1, 12, 0, 30, 0, time.UTC)
	for i := 0; i < 5; i++ {
		e := event.NewWithTime(fmt.Sprintf("k%d", i), i, t0)
		windows := assigner.AssignWindows(e)
		buf.Add(e, windows)
	}

	// Watermark before window end — no results
	results := buf.TriggerExpired(t0.Add(-time.Second))
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}

	// Watermark after window end — should trigger
	results = buf.TriggerExpired(t0.Add(time.Minute))
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

// ---- Watermark Tests ----

func TestBoundedOutOfOrderWatermark(t *testing.T) {
	gen := watermark.NewBoundedOutOfOrder(5*time.Second, time.Millisecond)

	t0 := time.Now()
	gen.OnEvent(event.NewWithTime("k", 1, t0))
	gen.OnEvent(event.NewWithTime("k", 2, t0.Add(3*time.Second)))

	// Wait for emit interval
	time.Sleep(5 * time.Millisecond)
	wm := gen.OnPeriodicEmit()
	if wm == nil {
		t.Fatal("expected watermark to be emitted")
	}

	// Watermark should be max_event_time - lag = (t0+3s) - 5s = t0-2s
	expected := t0.Add(3*time.Second - 5*time.Second)
	diff := wm.Timestamp.Sub(expected)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("watermark %v too far from expected %v", wm.Timestamp, expected)
	}
}

func TestWatermarkTracker(t *testing.T) {
	tracker := watermark.NewTracker(3)

	t0 := time.Unix(100, 0)
	t1 := time.Unix(200, 0)
	t2 := time.Unix(150, 0)

	tracker.Update(0, t0)
	tracker.Update(1, t1)
	tracker.Update(2, t2)

	// Global watermark = min(t0, t1, t2) = t0 = 100
	global := tracker.Global()
	if !global.Equal(t0) {
		t.Errorf("expected global watermark %v, got %v", t0, global)
	}

	// Advance partition 0 — global should advance to min(150, 200, 150) = 150
	tracker.Update(0, t2)
	global = tracker.Global()
	if !global.Equal(t2) {
		t.Errorf("expected global watermark %v, got %v", t2, global)
	}
}

func TestWatermarkLateDetection(t *testing.T) {
	tracker := watermark.NewTracker(1)
	t0 := time.Unix(100, 0)
	tracker.Update(0, t0)

	// Event before watermark is late
	if !tracker.IsLate(time.Unix(50, 0)) {
		t.Error("event before watermark should be late")
	}
	// Event after watermark is not late
	if tracker.IsLate(time.Unix(150, 0)) {
		t.Error("event after watermark should not be late")
	}
}

// ---- Operator Tests ----

func TestMapOperator(t *testing.T) {
	op := operator.NewMap("double", func(e *event.Event) (*event.Event, error) {
		val := e.Value.(int)
		return event.New(e.Key, val*2), nil
	})

	var results []*event.Event
	emit := func(e *event.Event) { results = append(results, e) }

	ctx := context.Background()
	op.Process(ctx, event.New("k", 5), emit)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Value != 10 {
		t.Errorf("expected value 10, got %v", results[0].Value)
	}
}

func TestFilterOperator(t *testing.T) {
	op := operator.NewFilter("evens-only", func(e *event.Event) bool {
		return e.Value.(int)%2 == 0
	})

	ctx := context.Background()
	var results []*event.Event
	emit := func(e *event.Event) { results = append(results, e) }

	for i := 0; i < 10; i++ {
		op.Process(ctx, event.New("k", i), emit)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 even results, got %d", len(results))
	}
	for _, r := range results {
		if r.Value.(int)%2 != 0 {
			t.Errorf("expected even value, got %v", r.Value)
		}
	}
}

func TestFlatMapOperator(t *testing.T) {
	op := operator.NewFlatMap("expand", func(e *event.Event) ([]*event.Event, error) {
		n := e.Value.(int)
		var out []*event.Event
		for i := 0; i < n; i++ {
			out = append(out, event.New(e.Key, i))
		}
		return out, nil
	})

	ctx := context.Background()
	var results []*event.Event
	emit := func(e *event.Event) { results = append(results, e) }

	op.Process(ctx, event.New("k", 3), emit)

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestReduceOperator(t *testing.T) {
	op := operator.NewReduce("sum", func(acc, e *event.Event) (*event.Event, error) {
		return event.New(acc.Key, acc.Value.(int)+e.Value.(int)), nil
	})

	ctx := context.Background()
	var lastResult *event.Event
	emit := func(e *event.Event) { lastResult = e }

	for i := 1; i <= 5; i++ {
		op.Process(ctx, event.New("total", i), emit)
	}

	// 1+2+3+4+5 = 15
	if lastResult.Value != 15 {
		t.Errorf("expected sum 15, got %v", lastResult.Value)
	}
}

func TestDeduplicateOperator(t *testing.T) {
	op := operator.NewDeduplicate("dedup", time.Minute)

	ctx := context.Background()
	var results []*event.Event
	emit := func(e *event.Event) { results = append(results, e) }

	// Send same key 5 times
	for i := 0; i < 5; i++ {
		op.Process(ctx, event.New("dup-key", i), emit)
	}
	// Send different key
	op.Process(ctx, event.New("unique-key", 99), emit)

	if len(results) != 2 {
		t.Errorf("expected 2 unique results, got %d", len(results))
	}
}

// ---- Checkpoint Tests ----

func TestCheckpointCoordinator(t *testing.T) {
	dir := t.TempDir()
	coord := checkpoint.NewCoordinator(dir, time.Second, 2)

	// Trigger a checkpoint
	barrier := coord.Trigger(map[int]int64{0: 100, 1: 200})
	if barrier == nil {
		t.Fatal("expected non-nil barrier")
	}
	if barrier.CheckpointID <= 0 {
		t.Error("expected positive checkpoint ID")
	}

	// Acknowledge from both operators
	op1 := checkpoint.NewOperatorCheckpointer("op1", coord)
	op2 := checkpoint.NewOperatorCheckpointer("op2", coord)

	if err := op1.SaveState(barrier.CheckpointID, map[string]int{"count": 42}); err != nil {
		t.Fatalf("op1 save state: %v", err)
	}
	if err := op2.SaveState(barrier.CheckpointID, map[string]int{"sum": 100}); err != nil {
		t.Fatalf("op2 save state: %v", err)
	}

	// Checkpoint should be completed
	cp := coord.LastCompleted()
	if cp == nil {
		t.Fatal("expected completed checkpoint")
	}
	if cp.ID != barrier.CheckpointID {
		t.Errorf("wrong checkpoint ID: %d", cp.ID)
	}
	if len(cp.OperatorStates) != 2 {
		t.Errorf("expected 2 operator states, got %d", len(cp.OperatorStates))
	}
}

func TestCheckpointRestore(t *testing.T) {
	dir := t.TempDir()
	coord := checkpoint.NewCoordinator(dir, time.Second, 1)

	// Create a checkpoint
	barrier := coord.Trigger(map[int]int64{0: 500})
	op := checkpoint.NewOperatorCheckpointer("op1", coord)
	op.SaveState(barrier.CheckpointID, "my-state")

	// Create a new coordinator and restore
	coord2 := checkpoint.NewCoordinator(dir, time.Second, 1)
	cp, err := coord2.Restore()
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if cp == nil {
		t.Fatal("expected restored checkpoint")
	}
	if cp.SourceOffsets[0] != 500 {
		t.Errorf("expected offset 500, got %d", cp.SourceOffsets[0])
	}
}

// ---- Pipeline Integration Tests ----

func TestPipelineMapFilter(t *testing.T) {
	events := []*event.Event{
		event.New("k", 1),
		event.New("k", 2),
		event.New("k", 3),
		event.New("k", 4),
		event.New("k", 5),
	}

	src := source.NewSliceSource("test-source", events)
	snk := sink.NewSliceSink("test-sink")

	mapOp := operator.NewMap("double", func(e *event.Event) (*event.Event, error) {
		return event.New(e.Key, e.Value.(int)*2), nil
	})
	filterOp := operator.NewFilter("gt5", func(e *event.Event) bool {
		return e.Value.(int) > 5
	})

	p := pipeline.New("test-pipeline").
		WithSource(src).
		WithOperator(mapOp).
		WithOperator(filterOp).
		WithSink(snk)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	results := snk.Events()
	// Doubled: 2,4,6,8,10 → filtered > 5: 6,8,10 → 3 results
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Value.(int) <= 5 {
			t.Errorf("expected value > 5, got %v", r.Value)
		}
	}
}

func TestPipelineReduce(t *testing.T) {
	events := []*event.Event{
		event.New("a", 10),
		event.New("b", 20),
		event.New("a", 5),
		event.New("b", 15),
		event.New("a", 3),
	}

	src := source.NewSliceSource("src", events)
	snk := sink.NewSliceSink("snk")

	reduceOp := operator.NewReduce("sum-by-key", func(acc, e *event.Event) (*event.Event, error) {
		return event.New(acc.Key, acc.Value.(int)+e.Value.(int)), nil
	})

	p := pipeline.New("reduce-test").
		WithSource(src).
		WithOperator(reduceOp).
		WithSink(snk)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	// Check final state: a=18, b=35
	state := reduceOp.GetState()
	if state["a"].Value.(int) != 18 {
		t.Errorf("expected a=18, got %v", state["a"].Value)
	}
	if state["b"].Value.(int) != 35 {
		t.Errorf("expected b=35, got %v", state["b"].Value)
	}
}

func TestPipelineThroughput(t *testing.T) {
	const numEvents = 10000
	var processed int64

	events := make([]*event.Event, numEvents)
	for i := range events {
		events[i] = event.New("k", i)
	}

	src := source.NewSliceSource("src", events)
	snk := sink.NewSliceSink("snk")

	mapOp := operator.NewMap("count", func(e *event.Event) (*event.Event, error) {
		atomic.AddInt64(&processed, 1)
		return e, nil
	})

	p := pipeline.New("throughput-test").
		WithSource(src).
		WithOperator(mapOp).
		WithSink(snk)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	elapsed := time.Since(start)
	throughput := float64(numEvents) / elapsed.Seconds()
	t.Logf("Throughput: %.0f events/sec (%d events in %v)", throughput, numEvents, elapsed)

	if snk.Count() != numEvents {
		t.Errorf("expected %d events in sink, got %d", numEvents, snk.Count())
	}
}

func TestPipelineWindowAggregation(t *testing.T) {
	// Create events across two 1-minute windows
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	events := []*event.Event{
		// Window 1: 12:00-12:01
		event.NewWithTime("sensor-1", 10, baseTime.Add(10*time.Second)),
		event.NewWithTime("sensor-1", 20, baseTime.Add(30*time.Second)),
		event.NewWithTime("sensor-1", 30, baseTime.Add(50*time.Second)),
		// Window 2: 12:01-12:02
		event.NewWithTime("sensor-1", 40, baseTime.Add(70*time.Second)),
		event.NewWithTime("sensor-1", 50, baseTime.Add(90*time.Second)),
		// Watermark trigger event (far in the future)
		event.NewWithTime("sensor-1", 0, baseTime.Add(10*time.Minute)),
	}

	src := source.NewSliceSource("src", events)
	snk := sink.NewSliceSink("snk")

	windowOp := operator.NewWindowOp("1min-sum",
		window.NewTumbling(time.Minute),
		func(result *window.WindowResult) (*event.Event, error) {
			sum := result.Sum(func(e *event.Event) float64 {
				return float64(e.Value.(int))
			})
			return event.NewWithTime(
				result.Window.Key,
				sum,
				result.Window.Start,
			), nil
		},
	)

	gen := watermark.NewBoundedOutOfOrder(0, time.Millisecond)
	p := pipeline.New("window-test").
		WithSource(src).
		WithOperator(windowOp).
		WithSink(snk).
		WithWatermarkGenerator(gen).
		WithWatermarkInterval(time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	results := snk.Events()
	if len(results) < 2 {
		t.Fatalf("expected at least 2 window results, got %d", len(results))
	}

	// Sort by event time
	sort.Slice(results, func(i, j int) bool {
		return results[i].EventTime.Before(results[j].EventTime)
	})

	// Window 1 sum: 10+20+30 = 60
	if results[0].Value.(float64) != 60 {
		t.Errorf("expected window 1 sum=60, got %v", results[0].Value)
	}
	// Window 2 sum: 40+50 = 90
	if results[1].Value.(float64) != 90 {
		t.Errorf("expected window 2 sum=90, got %v", results[1].Value)
	}
}

func TestSourceGenerator(t *testing.T) {
	var count int64
	src := source.NewGeneratorSource("gen", 1000, 100, func(seq int64) *event.Event {
		return event.New(fmt.Sprintf("key-%d", seq%5), seq)
	})

	snk := sink.NewCountSink("count-sink")
	mapOp := operator.NewMap("count", func(e *event.Event) (*event.Event, error) {
		atomic.AddInt64(&count, 1)
		return e, nil
	})

	p := pipeline.New("gen-test").
		WithSource(src).
		WithOperator(mapOp).
		WithSink(snk)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	if snk.Total() != 100 {
		t.Errorf("expected 100 events, got %d", snk.Total())
	}
}
