# stream-processing-engine

A **production-grade stream processing engine** built from scratch in Go, implementing the same core concepts as Apache Flink — event time processing, watermarks, windowing, exactly-once semantics via distributed checkpointing, and a composable operator DAG.

[![CI](https://github.com/Nitaiz123/stream-processing-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Nitaiz123/stream-processing-engine/actions)
[![Go](https://img.shields.io/badge/go-1.22+-00ADD8)](https://go.dev/)
[![Throughput](https://img.shields.io/badge/throughput-1.5M%20events%2Fsec-brightgreen)](benchmarks/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Pipeline DAG                                   │
│                                                                         │
│  Source ──→ [Op1: Map] ──→ [Op2: Filter] ──→ [Op3: Window] ──→ Sink    │
│    │            │               │                  │                    │
│    │         channel          channel            channel                │
│    │                                                                    │
│    └──→ Watermark Emitter ──→ injected into stream ──→ triggers windows │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     Core Subsystems                                     │
│                                                                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │  Event Time      │  │  Windowing       │  │  Exactly-Once        │  │
│  │                  │  │                  │  │                      │  │
│  │  EventTime vs    │  │  Tumbling        │  │  Chandy-Lamport      │  │
│  │  ProcessingTime  │  │  Sliding         │  │  checkpoint barriers │  │
│  │                  │  │  Session         │  │  Source offset track │  │
│  │  Watermarks:     │  │                  │  │  Atomic state commit │  │
│  │  BoundedOutOfOrd │  │  WindowBuffer    │  │  Recovery from disk  │  │
│  │  Monotonous      │  │  Aggregations:   │  │                      │  │
│  │  PerPartition    │  │  Sum/Avg/Min/Max │  │                      │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Key Concepts

### Event Time vs Processing Time

The engine processes events based on **event time** (when the event occurred) rather than processing time (when it arrived). This is critical for correctness when events arrive out of order due to network delays.

```
Real world:  event at 14:00:00 ─────────────────────────────────────────▶
                                              network delay (5s)
Pipeline:                        event arrives at 14:00:05 ─────────────▶
```

### Watermarks

Watermarks are the mechanism by which the engine tracks progress in event time:

```
Events:   [t=10] [t=15] [t=12] [t=20] [t=8-LATE] [t=25]
                                  ↑
                           Watermark = max(event_time) - lag
                           = 20 - 5 = 15
                           → Windows ending before t=15 are now complete
```

Three watermark strategies are implemented:
- **BoundedOutOfOrder**: `watermark = max_event_time - max_lag` (most common)
- **Monotonous**: `watermark = max_event_time` (no late data)
- **PerPartition**: minimum watermark across all source partitions

### Windowing

| Window Type | Description | Use Case |
|-------------|-------------|----------|
| **Tumbling** | Fixed-size, non-overlapping | "Count per minute" |
| **Sliding** | Fixed-size, overlapping | "5-min moving average" |
| **Session** | Gap-based, dynamic size | "User session grouping" |

### Exactly-Once Semantics (Chandy-Lamport Checkpointing)

```
Coordinator injects barrier ──→ [Source] ──→ [Op1] ──→ [Op2] ──→ [Sink]
                                   │            │         │
                                   ▼            ▼         ▼
                               save offset  save state  save state
                                   │            │         │
                                   └────────────┴─────────┘
                                                │
                                         Checkpoint complete
                                         (atomic commit to disk)
```

On failure, the pipeline restores from the last completed checkpoint and replays from the committed source offsets.

---

## Built-in Operators

| Operator | Description | Complexity |
|----------|-------------|------------|
| `Map` | 1-to-1 transformation | O(1) per event |
| `Filter` | Drop events by predicate | O(1) per event |
| `FlatMap` | 1-to-N transformation | O(k) per event |
| `Reduce` | Stateful per-key aggregation | O(1) amortized |
| `Window` | Time-window aggregation | O(log n) per event |
| `Deduplicate` | Remove duplicate keys within TTL | O(1) amortized |

---

## Getting Started

### Build

```bash
git clone https://github.com/Nitaiz123/stream-processing-engine.git
cd stream-processing-engine
go build ./...
```

### Run Tests

```bash
go test ./tests/... -v
```

### Example: Word Count with Tumbling Windows

```go
package main

import (
    "context"
    "strings"
    "time"

    "github.com/Nitaiz123/stream-processing-engine/internal/operator"
    "github.com/Nitaiz123/stream-processing-engine/internal/pipeline"
    "github.com/Nitaiz123/stream-processing-engine/internal/sink"
    "github.com/Nitaiz123/stream-processing-engine/internal/source"
    "github.com/Nitaiz123/stream-processing-engine/internal/window"
    "github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

func main() {
    sentences := []*event.Event{
        event.NewWithTime("doc", "hello world foo", time.Now()),
        event.NewWithTime("doc", "hello bar baz", time.Now().Add(10*time.Second)),
        event.NewWithTime("doc", "world qux", time.Now().Add(20*time.Second)),
    }

    src := source.NewSliceSource("sentences", sentences)
    snk := sink.NewPrintSink("output")

    // Split sentences into words
    splitOp := operator.NewFlatMap("split", func(e *event.Event) ([]*event.Event, error) {
        words := strings.Fields(e.Value.(string))
        var events []*event.Event
        for _, w := range words {
            events = append(events, event.NewWithTime(w, 1, e.EventTime))
        }
        return events, nil
    })

    // Count words in 1-minute tumbling windows
    windowOp := operator.NewWindowOp("count-words",
        window.NewTumbling(time.Minute),
        func(result *window.WindowResult) (*event.Event, error) {
            return event.NewWithTime(result.Window.Key, result.Count(), result.Window.Start), nil
        },
    )

    p := pipeline.New("word-count").
        WithSource(src).
        WithOperator(splitOp).
        WithOperator(windowOp).
        WithSink(snk)

    p.Run(context.Background())
}
```

### Example: Sensor Aggregation with Late Data Handling

```go
// Tolerate events arriving up to 30 seconds late
gen := watermark.NewBoundedOutOfOrder(30*time.Second, 100*time.Millisecond)

p := pipeline.New("sensor-agg").
    WithSource(sensorSource).
    WithOperator(operator.NewWindowOp("5min-avg",
        window.NewTumbling(5*time.Minute),
        func(r *window.WindowResult) (*event.Event, error) {
            avg := r.Avg(func(e *event.Event) float64 {
                return e.Value.(float64)
            })
            return event.NewWithTime(r.Window.Key, avg, r.Window.Start), nil
        },
    )).
    WithSink(metricsSink).
    WithWatermarkGenerator(gen)
```

---

## Performance

Benchmarked on a single core (no parallelism):

| Scenario | Throughput |
|----------|-----------|
| Map + Filter pipeline | **~1.5M events/sec** |
| Reduce (stateful, per-key) | ~800K events/sec |
| Window aggregation | ~500K events/sec |

---

## Project Structure

```
stream-processing-engine/
├── pkg/event/
│   └── event.go              # Event, Watermark, CheckpointBarrier types
├── internal/
│   ├── pipeline/
│   │   └── pipeline.go       # Pipeline DAG execution engine
│   ├── operator/
│   │   └── operator.go       # Map, Filter, FlatMap, Reduce, Window, Deduplicate
│   ├── window/
│   │   └── window.go         # Tumbling, Sliding, Session windows + WindowBuffer
│   ├── watermark/
│   │   └── watermark.go      # BoundedOutOfOrder, Monotonous generators + Tracker
│   ├── checkpoint/
│   │   └── checkpoint.go     # Chandy-Lamport distributed checkpointing
│   ├── source/
│   │   └── source.go         # Channel, Slice, Generator sources
│   └── sink/
│       └── sink.go           # Channel, Slice, Print, Count sinks
└── tests/
    └── engine_test.go        # 24 unit + integration tests
```

---

## Comparison with Apache Flink

| Feature | This Engine | Apache Flink |
|---------|-------------|--------------|
| Language | Go | Java/Scala |
| Event time | ✅ | ✅ |
| Watermarks | ✅ (3 strategies) | ✅ |
| Tumbling windows | ✅ | ✅ |
| Sliding windows | ✅ | ✅ |
| Session windows | ✅ | ✅ |
| Exactly-once | ✅ (Chandy-Lamport) | ✅ |
| Parallel execution | ❌ (single-node) | ✅ (distributed) |
| State backends | File (JSON) | RocksDB, heap, S3 |
| Throughput | ~1.5M/sec | ~10M+/sec |

---

## References

- [The Dataflow Model (Akidau et al., 2015)](https://research.google/pubs/pub43864/)
- [Streaming Systems (Tyler Akidau et al., 2018)](https://www.oreilly.com/library/view/streaming-systems/9781491983867/)
- [Chandy-Lamport Algorithm (1985)](https://lamport.azurewebsites.net/pubs/chandy.pdf)
- [Apache Flink Architecture](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/)
- [Lightweight Asynchronous Snapshots (Carbone et al., 2015)](https://arxiv.org/abs/1506.08603)

---

## License

MIT — see [LICENSE](LICENSE)
