// Package event defines the core data types for the stream processing engine.
//
// Every piece of data flowing through the pipeline is an Event. Events have:
//   - A key for partitioning and grouping
//   - A value (arbitrary payload)
//   - An event time timestamp (when the event occurred in the real world)
//   - A processing time timestamp (when the event entered the pipeline)
//
// The distinction between event time and processing time is fundamental to
// stream processing:
//
//   Event time:      When the event actually occurred (e.g., user clicked at 14:00:00)
//   Processing time: When the pipeline processed it (e.g., arrived at 14:00:05 due to lag)
//
// Processing based on event time is more correct but requires handling out-of-order
// events. Processing based on processing time is simpler but can give wrong results
// when events arrive late.
//
// This engine uses event time with watermarks to handle late data.
//
// References:
//   - The Dataflow Model (Akidau et al., 2015): https://research.google/pubs/pub43864/
//   - Apache Flink event time: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/
package event

import (
	"fmt"
	"time"
)

// Event is the fundamental unit of data in the stream processing engine.
type Event struct {
	// Key is used for partitioning and grouping (e.g., user ID, sensor ID)
	Key string
	// Value is the event payload
	Value interface{}
	// EventTime is when the event occurred in the real world
	EventTime time.Time
	// ProcessingTime is when the event entered the pipeline
	ProcessingTime time.Time
	// Headers are optional metadata key-value pairs
	Headers map[string]string
	// Offset is the position in the source stream (for exactly-once semantics)
	Offset int64
	// Partition is the source partition (for parallel processing)
	Partition int
}

// New creates a new event with the current time as both event time and processing time.
func New(key string, value interface{}) *Event {
	now := time.Now()
	return &Event{
		Key:            key,
		Value:          value,
		EventTime:      now,
		ProcessingTime: now,
	}
}

// NewWithTime creates a new event with an explicit event time.
func NewWithTime(key string, value interface{}, eventTime time.Time) *Event {
	return &Event{
		Key:            key,
		Value:          value,
		EventTime:      eventTime,
		ProcessingTime: time.Now(),
	}
}

// WithHeader adds a header to the event.
func (e *Event) WithHeader(key, value string) *Event {
	if e.Headers == nil {
		e.Headers = make(map[string]string)
	}
	e.Headers[key] = value
	return e
}

// WithOffset sets the source offset for exactly-once tracking.
func (e *Event) WithOffset(partition int, offset int64) *Event {
	e.Partition = partition
	e.Offset = offset
	return e
}

// Clone creates a deep copy of the event.
func (e *Event) Clone() *Event {
	clone := *e
	if e.Headers != nil {
		clone.Headers = make(map[string]string, len(e.Headers))
		for k, v := range e.Headers {
			clone.Headers[k] = v
		}
	}
	return &clone
}

// String returns a human-readable representation of the event.
func (e *Event) String() string {
	return fmt.Sprintf("Event{key=%s, value=%v, eventTime=%s}",
		e.Key, e.Value, e.EventTime.Format(time.RFC3339))
}

// Watermark is a special event that signals progress in event time.
//
// A watermark with timestamp T means: "all events with event time < T have
// been observed." Any event arriving after the watermark with event time < T
// is considered "late."
//
// Watermarks are the mechanism by which a streaming system knows when it's
// safe to produce results for a time window.
type Watermark struct {
	// Timestamp is the watermark time — all events before this have been seen
	Timestamp time.Time
	// Source identifies which source emitted this watermark
	Source string
}

// IsWatermark returns true if the event is a watermark (sentinel value).
func IsWatermark(v interface{}) bool {
	_, ok := v.(*Watermark)
	return ok
}

// SourceOffset tracks the committed offset for a source partition.
// Used for exactly-once semantics.
type SourceOffset struct {
	// Partition is the source partition identifier
	Partition int
	// Offset is the last committed offset in this partition
	Offset int64
}

// CheckpointBarrier is injected into the stream to trigger checkpointing.
// All operators must process the barrier before the checkpoint is considered complete.
type CheckpointBarrier struct {
	// CheckpointID uniquely identifies this checkpoint
	CheckpointID int64
	// Timestamp is when the checkpoint was triggered
	Timestamp time.Time
}
