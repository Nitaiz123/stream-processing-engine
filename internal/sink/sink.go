// Package sink provides built-in event sinks.
package sink

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// ChannelSink writes events to a Go channel.
type ChannelSink struct {
	name   string
	output chan<- *event.Event
}

// NewChannelSink creates a sink that writes to a channel.
func NewChannelSink(name string, output chan<- *event.Event) *ChannelSink {
	return &ChannelSink{name: name, output: output}
}

func (s *ChannelSink) Name() string { return s.name }

func (s *ChannelSink) Write(ctx context.Context, e *event.Event) error {
	select {
	case s.output <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChannelSink) Flush() error { return nil }

// SliceSink collects events into a slice. Useful for testing.
type SliceSink struct {
	mu     sync.Mutex
	name   string
	events []*event.Event
}

// NewSliceSink creates a sink that collects events into a slice.
func NewSliceSink(name string) *SliceSink {
	return &SliceSink{name: name}
}

func (s *SliceSink) Name() string { return s.name }

func (s *SliceSink) Write(_ context.Context, e *event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *SliceSink) Flush() error { return nil }

// Events returns all collected events.
func (s *SliceSink) Events() []*event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*event.Event, len(s.events))
	copy(result, s.events)
	return result
}

// Count returns the number of collected events.
func (s *SliceSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// PrintSink prints events to stdout or a writer.
type PrintSink struct {
	name   string
	writer io.Writer
	format func(*event.Event) string
}

// NewPrintSink creates a sink that prints events.
func NewPrintSink(name string) *PrintSink {
	return &PrintSink{
		name:   name,
		writer: os.Stdout,
		format: func(e *event.Event) string {
			return fmt.Sprintf("[%s] key=%s value=%v\n",
				e.EventTime.Format("15:04:05.000"), e.Key, e.Value)
		},
	}
}

func (s *PrintSink) Name() string { return s.name }

func (s *PrintSink) Write(_ context.Context, e *event.Event) error {
	_, err := fmt.Fprint(s.writer, s.format(e))
	return err
}

func (s *PrintSink) Flush() error { return nil }

// CountSink counts events by key.
type CountSink struct {
	mu     sync.Mutex
	name   string
	counts map[string]int64
	total  int64
}

// NewCountSink creates a counting sink.
func NewCountSink(name string) *CountSink {
	return &CountSink{
		name:   name,
		counts: make(map[string]int64),
	}
}

func (s *CountSink) Name() string { return s.name }

func (s *CountSink) Write(_ context.Context, e *event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[e.Key]++
	s.total++
	return nil
}

func (s *CountSink) Flush() error { return nil }

// Total returns the total number of events received.
func (s *CountSink) Total() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// CountForKey returns the count for a specific key.
func (s *CountSink) CountForKey(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

// Counts returns a copy of all counts.
func (s *CountSink) Counts() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]int64, len(s.counts))
	for k, v := range s.counts {
		result[k] = v
	}
	return result
}
