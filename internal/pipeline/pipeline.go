// Package pipeline implements the streaming pipeline execution engine.
//
// A pipeline is a DAG of operators connected by channels. Events flow from
// sources through operators to sinks. The pipeline manages:
//   - Operator lifecycle (start, stop, error handling)
//   - Backpressure via buffered channels
//   - Watermark propagation
//   - Exactly-once checkpointing
//
// Architecture:
//
//   Source → [Op1] → [Op2] → [Op3] → Sink
//              ↑       ↑       ↑
//           channel  channel  channel
//
// Each operator runs in its own goroutine and communicates via channels.
// This is the same architecture used by Apache Flink's task execution model.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/internal/operator"
	"github.com/Nitaiz123/stream-processing-engine/internal/watermark"
	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

const defaultChannelBuffer = 1024

// Source is a data source that produces events.
type Source interface {
	// Name returns the source name.
	Name() string
	// Start begins producing events into the output channel.
	Start(ctx context.Context, out chan<- *event.Event) error
	// Offsets returns the current committed offsets (for exactly-once).
	Offsets() map[int]int64
}

// Sink is a data sink that consumes events.
type Sink interface {
	// Name returns the sink name.
	Name() string
	// Write writes an event to the sink.
	Write(ctx context.Context, e *event.Event) error
	// Flush flushes any buffered events.
	Flush() error
}

// Pipeline is a streaming data pipeline.
type Pipeline struct {
	name      string
	source    Source
	operators []operator.Operator
	sink      Sink
	wmGen     watermark.Generator
	wmTracker *watermark.Tracker

	// Configuration
	watermarkInterval time.Duration
	channelBuffer     int

	// Runtime state
	mu      sync.Mutex
	running bool
	errCh   chan error
}

// New creates a new pipeline.
func New(name string) *Pipeline {
	return &Pipeline{
		name:              name,
		watermarkInterval: 200 * time.Millisecond,
		channelBuffer:     defaultChannelBuffer,
		errCh:             make(chan error, 10),
	}
}

// WithSource sets the pipeline source.
func (p *Pipeline) WithSource(s Source) *Pipeline {
	p.source = s
	return p
}

// WithOperator appends an operator to the pipeline.
func (p *Pipeline) WithOperator(op operator.Operator) *Pipeline {
	p.operators = append(p.operators, op)
	return p
}

// WithSink sets the pipeline sink.
func (p *Pipeline) WithSink(s Sink) *Pipeline {
	p.sink = s
	return p
}

// WithWatermarkGenerator sets the watermark strategy.
func (p *Pipeline) WithWatermarkGenerator(gen watermark.Generator) *Pipeline {
	p.wmGen = gen
	return p
}

// WithWatermarkInterval sets how often watermarks are emitted.
func (p *Pipeline) WithWatermarkInterval(d time.Duration) *Pipeline {
	p.watermarkInterval = d
	return p
}

// WithChannelBuffer sets the channel buffer size between operators.
func (p *Pipeline) WithChannelBuffer(size int) *Pipeline {
	p.channelBuffer = size
	return p
}

// Run starts the pipeline and blocks until it completes or the context is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("pipeline %s is already running", p.name)
	}
	p.running = true
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
	}()

	if p.source == nil {
		return fmt.Errorf("pipeline %s has no source", p.name)
	}
	if p.sink == nil {
		return fmt.Errorf("pipeline %s has no sink", p.name)
	}

	// Default watermark generator
	if p.wmGen == nil {
		p.wmGen = watermark.NewBoundedOutOfOrder(5*time.Second, p.watermarkInterval)
	}
	p.wmTracker = watermark.NewTracker(1)

	// Build the channel chain
	// source → ch[0] → op[0] → ch[1] → op[1] → ... → ch[n] → sink
	numChannels := len(p.operators) + 1
	channels := make([]chan *event.Event, numChannels)
	for i := range channels {
		channels[i] = make(chan *event.Event, p.channelBuffer)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(p.operators)+2)

	// sourceDone is closed when the source finishes, so the watermark goroutine stops.
	sourceDone := make(chan struct{})

	// Start source
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(sourceDone)
		if err := p.source.Start(ctx, channels[0]); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("source %s: %w", p.source.Name(), err)
		}
		// Inject a final max-time watermark to flush all pending windows
		if ctx.Err() == nil {
			maxWm := &event.Watermark{Timestamp: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}
			channels[0] <- &event.Event{Value: maxWm, EventTime: maxWm.Timestamp}
		}
		close(channels[0])
	}()

	// Start watermark emitter — stops when source is done or context is cancelled.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(p.watermarkInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sourceDone:
				return
			case <-ticker.C:
				if wm := p.wmGen.OnPeriodicEmit(); wm != nil {
					wmEvent := &event.Event{
						Value:     wm,
						EventTime: wm.Timestamp,
					}
					// Inject watermark into the first channel (non-blocking if source done)
					select {
					case channels[0] <- wmEvent:
					case <-sourceDone:
						return
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	// Start operators
	for i, op := range p.operators {
		i, op := i, op
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(channels[i+1])
			if err := p.runOperator(ctx, op, channels[i], channels[i+1]); err != nil && ctx.Err() == nil {
				errCh <- fmt.Errorf("operator %s: %w", op.Name(), err)
			}
		}()
	}

	// Start sink
	wg.Add(1)
	go func() {
		defer wg.Done()
		lastCh := channels[len(channels)-1]
		for e := range lastCh {
			if ctx.Err() != nil {
				return
			}
			// Skip watermark events at the sink
			if _, ok := e.Value.(*event.Watermark); ok {
				continue
			}
			if err := p.sink.Write(ctx, e); err != nil && ctx.Err() == nil {
				errCh <- fmt.Errorf("sink %s: %w", p.sink.Name(), err)
				return
			}
		}
		if err := p.sink.Flush(); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("sink flush %s: %w", p.sink.Name(), err)
		}
	}()

	// Wait for all goroutines
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	case err := <-errCh:
		return err
	}
}

// runOperator runs a single operator, reading from in and writing to out.
func (p *Pipeline) runOperator(ctx context.Context, op operator.Operator, in <-chan *event.Event, out chan<- *event.Event) error {
	emit := func(e *event.Event) {
		select {
		case out <- e:
		case <-ctx.Done():
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-in:
			if !ok {
				return nil
			}

			// Check if this is a watermark event
			if wm, ok := e.Value.(*event.Watermark); ok {
				// Update watermark tracker
				p.wmTracker.Update(0, wm.Timestamp)
				if err := op.OnWatermark(ctx, wm, emit); err != nil {
					return err
				}
				continue
			}

			// Update watermark generator with event time
			if p.wmGen != nil {
				p.wmGen.OnEvent(e)
			}

			if err := op.Process(ctx, e, emit); err != nil {
				return err
			}
		}
	}
}

// IsRunning returns true if the pipeline is currently running.
func (p *Pipeline) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// Name returns the pipeline name.
func (p *Pipeline) Name() string {
	return p.name
}
