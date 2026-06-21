// Package window implements time-based windowing for stream aggregations.
//
// Windows are the mechanism by which a streaming system groups events into
// finite buckets for aggregation. Without windows, you'd have to aggregate
// over the entire infinite stream.
//
// Window types:
//
//   Tumbling windows: Fixed-size, non-overlapping windows.
//     |--w1--|--w2--|--w3--|
//     Events belong to exactly one window.
//     Example: "Count events per 1-minute window"
//
//   Sliding windows: Fixed-size, overlapping windows.
//     |--w1--|
//       |--w2--|
//         |--w3--|
//     Events can belong to multiple windows.
//     Example: "5-minute window sliding every 1 minute" (moving average)
//
//   Session windows: Dynamic-size windows separated by gaps of inactivity.
//     |--session1--|  gap  |--session2--|
//     Window closes after a configurable period of inactivity.
//     Example: "Group user activity into sessions with 30-min timeout"
//
// All window types use event time (not processing time) for correctness.
//
// References:
//   - The Dataflow Model (Akidau et al., 2015)
//   - Flink window documentation: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/
package window

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// Window represents a time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
	Key   string // For keyed windows
}

// String returns a human-readable representation.
func (w Window) String() string {
	return fmt.Sprintf("[%s, %s)", w.Start.Format("15:04:05"), w.End.Format("15:04:05"))
}

// Contains returns true if the event time falls within this window.
func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// IsExpired returns true if the watermark has passed the window's end.
func (w Window) IsExpired(watermark time.Time) bool {
	return !watermark.Before(w.End)
}

// Assigner assigns events to one or more windows.
type Assigner interface {
	// AssignWindows returns the windows an event belongs to.
	AssignWindows(e *event.Event) []Window
	// MergeWindows merges overlapping windows (for session windows).
	MergeWindows(windows []Window) []Window
}

// ---- Tumbling Window ----

// TumblingAssigner assigns events to fixed-size, non-overlapping windows.
type TumblingAssigner struct {
	Size time.Duration
}

// NewTumbling creates a tumbling window assigner.
func NewTumbling(size time.Duration) *TumblingAssigner {
	return &TumblingAssigner{Size: size}
}

func (a *TumblingAssigner) AssignWindows(e *event.Event) []Window {
	start := e.EventTime.Truncate(a.Size)
	return []Window{{Start: start, End: start.Add(a.Size), Key: e.Key}}
}

func (a *TumblingAssigner) MergeWindows(windows []Window) []Window {
	return windows // Tumbling windows never merge
}

// ---- Sliding Window ----

// SlidingAssigner assigns events to multiple overlapping windows.
type SlidingAssigner struct {
	Size  time.Duration
	Slide time.Duration
}

// NewSliding creates a sliding window assigner.
// Size is the window duration, Slide is how often a new window starts.
func NewSliding(size, slide time.Duration) *SlidingAssigner {
	return &SlidingAssigner{Size: size, Slide: slide}
}

func (a *SlidingAssigner) AssignWindows(e *event.Event) []Window {
	var windows []Window
	t := e.EventTime

	// Find the last window start that contains t
	lastStart := t.Truncate(a.Slide)

	// Go back far enough to cover all windows that contain t
	numWindows := int(a.Size/a.Slide) + 1
	for i := 0; i < numWindows; i++ {
		start := lastStart.Add(-time.Duration(i) * a.Slide)
		end := start.Add(a.Size)
		if !t.Before(start) && t.Before(end) {
			windows = append(windows, Window{Start: start, End: end, Key: e.Key})
		}
	}

	return windows
}

func (a *SlidingAssigner) MergeWindows(windows []Window) []Window {
	return windows // Sliding windows never merge
}

// ---- Session Window ----

// SessionAssigner assigns events to session windows.
// A session window is extended as long as events keep arriving within the gap.
type SessionAssigner struct {
	Gap time.Duration
}

// NewSession creates a session window assigner.
func NewSession(gap time.Duration) *SessionAssigner {
	return &SessionAssigner{Gap: gap}
}

func (a *SessionAssigner) AssignWindows(e *event.Event) []Window {
	// Each event initially gets its own window [t, t+gap)
	return []Window{{
		Start: e.EventTime,
		End:   e.EventTime.Add(a.Gap),
		Key:   e.Key,
	}}
}

// MergeWindows merges overlapping session windows.
func (a *SessionAssigner) MergeWindows(windows []Window) []Window {
	if len(windows) == 0 {
		return windows
	}

	// Sort by start time
	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Start.Before(windows[j].Start)
	})

	merged := []Window{windows[0]}
	for _, w := range windows[1:] {
		last := &merged[len(merged)-1]
		if !w.Start.After(last.End) {
			// Overlapping: extend the current window
			if w.End.After(last.End) {
				last.End = w.End
			}
		} else {
			merged = append(merged, w)
		}
	}

	return merged
}

// ---- Window State ----

// WindowBuffer holds events for a window until it's ready to be emitted.
type WindowBuffer struct {
	mu      sync.Mutex
	windows map[string]*windowState // key+windowID -> state
}

type windowState struct {
	Window Window
	Events []*event.Event
}

func windowID(w Window) string {
	return fmt.Sprintf("%s_%s", w.Start.UnixNano(), w.End.UnixNano())
}

// NewWindowBuffer creates a new window buffer.
func NewWindowBuffer() *WindowBuffer {
	return &WindowBuffer{
		windows: make(map[string]*windowState),
	}
}

// Add adds an event to all its assigned windows.
func (b *WindowBuffer) Add(e *event.Event, windows []Window) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, w := range windows {
		id := e.Key + "_" + windowID(w)
		if _, ok := b.windows[id]; !ok {
			b.windows[id] = &windowState{Window: w}
		}
		b.windows[id].Events = append(b.windows[id].Events, e)
	}
}

// TriggerExpired returns and removes all windows that have expired
// (watermark has passed their end time).
func (b *WindowBuffer) TriggerExpired(watermark time.Time) []*WindowResult {
	b.mu.Lock()
	defer b.mu.Unlock()

	var results []*WindowResult
	for id, state := range b.windows {
		if state.Window.IsExpired(watermark) {
			results = append(results, &WindowResult{
				Window: state.Window,
				Events: state.Events,
			})
			delete(b.windows, id)
		}
	}
	return results
}

// WindowResult contains the events in a triggered window.
type WindowResult struct {
	Window Window
	Events []*event.Event
}

// Count returns the number of events in this window.
func (r *WindowResult) Count() int {
	return len(r.Events)
}

// Sum returns the sum of a numeric field extracted by fn.
func (r *WindowResult) Sum(fn func(*event.Event) float64) float64 {
	var sum float64
	for _, e := range r.Events {
		sum += fn(e)
	}
	return sum
}

// Avg returns the average of a numeric field extracted by fn.
func (r *WindowResult) Avg(fn func(*event.Event) float64) float64 {
	if len(r.Events) == 0 {
		return 0
	}
	return r.Sum(fn) / float64(len(r.Events))
}

// Max returns the maximum of a numeric field extracted by fn.
func (r *WindowResult) Max(fn func(*event.Event) float64) float64 {
	if len(r.Events) == 0 {
		return 0
	}
	max := fn(r.Events[0])
	for _, e := range r.Events[1:] {
		if v := fn(e); v > max {
			max = v
		}
	}
	return max
}

// Min returns the minimum of a numeric field extracted by fn.
func (r *WindowResult) Min(fn func(*event.Event) float64) float64 {
	if len(r.Events) == 0 {
		return 0
	}
	min := fn(r.Events[0])
	for _, e := range r.Events[1:] {
		if v := fn(e); v < min {
			min = v
		}
	}
	return min
}
