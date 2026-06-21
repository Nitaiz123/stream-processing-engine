// Package source provides built-in event sources.
package source

import (
	"context"
	"sync"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// ChannelSource reads events from a Go channel.
// Useful for testing and in-process event injection.
type ChannelSource struct {
	name   string
	input  <-chan *event.Event
	mu     sync.Mutex
	offset int64
}

// NewChannelSource creates a source that reads from a channel.
func NewChannelSource(name string, input <-chan *event.Event) *ChannelSource {
	return &ChannelSource{name: name, input: input}
}

func (s *ChannelSource) Name() string { return s.name }

func (s *ChannelSource) Start(ctx context.Context, out chan<- *event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-s.input:
			if !ok {
				return nil
			}
			s.mu.Lock()
			s.offset++
			e.Offset = s.offset
			s.mu.Unlock()
			select {
			case out <- e:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (s *ChannelSource) Offsets() map[int]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[int]int64{0: s.offset}
}

// SliceSource reads events from a slice. Useful for testing.
type SliceSource struct {
	name   string
	events []*event.Event
}

// NewSliceSource creates a source that emits events from a slice.
func NewSliceSource(name string, events []*event.Event) *SliceSource {
	return &SliceSource{name: name, events: events}
}

func (s *SliceSource) Name() string { return s.name }

func (s *SliceSource) Start(ctx context.Context, out chan<- *event.Event) error {
	for i, e := range s.events {
		e.Offset = int64(i)
		select {
		case out <- e:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (s *SliceSource) Offsets() map[int]int64 {
	return map[int]int64{0: int64(len(s.events))}
}

// GeneratorSource generates synthetic events at a given rate.
// Useful for load testing and benchmarking.
type GeneratorSource struct {
	name      string
	rate      int // events per second
	total     int // 0 = infinite
	genFn     func(seq int64) *event.Event
	mu        sync.Mutex
	emitted   int64
}

// NewGeneratorSource creates a synthetic event generator.
func NewGeneratorSource(name string, ratePerSec, total int, genFn func(seq int64) *event.Event) *GeneratorSource {
	return &GeneratorSource{
		name:  name,
		rate:  ratePerSec,
		total: total,
		genFn: genFn,
	}
}

func (s *GeneratorSource) Name() string { return s.name }

func (s *GeneratorSource) Start(ctx context.Context, out chan<- *event.Event) error {
	interval := time.Second / time.Duration(s.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var seq int64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e := s.genFn(seq)
			e.Offset = seq
			seq++

			s.mu.Lock()
			s.emitted++
			s.mu.Unlock()

			select {
			case out <- e:
			case <-ctx.Done():
				return nil
			}

			if s.total > 0 && int(seq) >= s.total {
				return nil
			}
		}
	}
}

func (s *GeneratorSource) Offsets() map[int]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[int]int64{0: s.emitted}
}
