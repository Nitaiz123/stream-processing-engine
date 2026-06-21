// Package checkpoint implements distributed checkpointing for exactly-once semantics.
//
// Exactly-once processing guarantees that each event is processed exactly once,
// even in the presence of failures. This is achieved through:
//
//  1. Chandy-Lamport distributed snapshots (adapted for streaming):
//     - The coordinator injects checkpoint barriers into the stream
//     - Each operator saves its state when it receives a barrier
//     - The checkpoint is complete when all operators have saved their state
//
//  2. Source offset tracking:
//     - Sources track the offset of the last event included in a checkpoint
//     - On recovery, sources replay from the last committed offset
//
//  3. Atomic state commit:
//     - All operator states are committed atomically
//     - If any operator fails to commit, the entire checkpoint is aborted
//
// This is the same approach used by Apache Flink's checkpointing mechanism.
//
// References:
//   - Chandy-Lamport algorithm (1985): https://lamport.azurewebsites.net/pubs/chandy.pdf
//   - Flink checkpointing: https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/stream_checkpointing/
//   - Lightweight Asynchronous Snapshots (Carbone et al., 2015)
package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Nitaiz123/stream-processing-engine/pkg/event"
)

// OperatorState holds the serialized state of an operator at a checkpoint.
type OperatorState struct {
	// OperatorID identifies the operator
	OperatorID string
	// State is the serialized operator state (JSON)
	State json.RawMessage
}

// Checkpoint represents a complete snapshot of the pipeline state.
type Checkpoint struct {
	// ID uniquely identifies this checkpoint
	ID int64
	// Timestamp is when the checkpoint was triggered
	Timestamp time.Time
	// SourceOffsets maps partition -> last committed offset
	SourceOffsets map[int]int64
	// OperatorStates holds the state of each operator
	OperatorStates []OperatorState
	// Status is the checkpoint status
	Status CheckpointStatus
}

// CheckpointStatus represents the lifecycle of a checkpoint.
type CheckpointStatus string

const (
	CheckpointPending   CheckpointStatus = "pending"
	CheckpointCompleted CheckpointStatus = "completed"
	CheckpointAborted   CheckpointStatus = "aborted"
)

// Coordinator manages the checkpoint lifecycle.
//
// The coordinator:
//  1. Triggers checkpoints at regular intervals
//  2. Tracks which operators have acknowledged the checkpoint
//  3. Persists completed checkpoints to durable storage
//  4. Provides the latest completed checkpoint for recovery
type Coordinator struct {
	mu              sync.Mutex
	checkpointDir   string
	interval        time.Duration
	nextID          int64
	pending         map[int64]*pendingCheckpoint
	lastCompleted   *Checkpoint
	numOperators    int
}

type pendingCheckpoint struct {
	checkpoint *Checkpoint
	acked      map[string]bool // operatorID -> acked
}

// NewCoordinator creates a checkpoint coordinator.
func NewCoordinator(checkpointDir string, interval time.Duration, numOperators int) *Coordinator {
	return &Coordinator{
		checkpointDir: checkpointDir,
		interval:      interval,
		numOperators:  numOperators,
		pending:       make(map[int64]*pendingCheckpoint),
		nextID:        1,
	}
}

// Trigger initiates a new checkpoint and returns a barrier to inject into the stream.
func (c *Coordinator) Trigger(sourceOffsets map[int]int64) *event.CheckpointBarrier {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := c.nextID
	c.nextID++

	cp := &Checkpoint{
		ID:            id,
		Timestamp:     time.Now(),
		SourceOffsets: sourceOffsets,
		Status:        CheckpointPending,
	}

	c.pending[id] = &pendingCheckpoint{
		checkpoint: cp,
		acked:      make(map[string]bool),
	}

	return &event.CheckpointBarrier{
		CheckpointID: id,
		Timestamp:    cp.Timestamp,
	}
}

// Acknowledge records that an operator has completed its state snapshot.
func (c *Coordinator) Acknowledge(checkpointID int64, operatorID string, state json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending, ok := c.pending[checkpointID]
	if !ok {
		return fmt.Errorf("unknown checkpoint ID: %d", checkpointID)
	}

	pending.acked[operatorID] = true
	pending.checkpoint.OperatorStates = append(pending.checkpoint.OperatorStates, OperatorState{
		OperatorID: operatorID,
		State:      state,
	})

	// Check if all operators have acknowledged
	if len(pending.acked) >= c.numOperators {
		return c.complete(checkpointID)
	}

	return nil
}

// complete finalizes a checkpoint and persists it to disk.
func (c *Coordinator) complete(checkpointID int64) error {
	pending := c.pending[checkpointID]
	pending.checkpoint.Status = CheckpointCompleted

	// Persist to disk
	if err := c.persist(pending.checkpoint); err != nil {
		pending.checkpoint.Status = CheckpointAborted
		delete(c.pending, checkpointID)
		return fmt.Errorf("persisting checkpoint %d: %w", checkpointID, err)
	}

	c.lastCompleted = pending.checkpoint
	delete(c.pending, checkpointID)

	// Clean up old checkpoints (keep last 3)
	c.cleanupOld()

	return nil
}

// persist writes a checkpoint to disk atomically.
func (c *Coordinator) persist(cp *Checkpoint) error {
	if c.checkpointDir == "" {
		return nil // In-memory only
	}

	if err := os.MkdirAll(c.checkpointDir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}

	// Write to a temp file first, then rename (atomic on most filesystems)
	tmpPath := filepath.Join(c.checkpointDir, fmt.Sprintf("cp-%d.tmp", cp.ID))
	finalPath := filepath.Join(c.checkpointDir, fmt.Sprintf("cp-%d.json", cp.ID))

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, finalPath)
}

// cleanupOld removes old checkpoint files, keeping only the last 3.
func (c *Coordinator) cleanupOld() {
	if c.checkpointDir == "" || c.lastCompleted == nil {
		return
	}

	keepFrom := c.lastCompleted.ID - 3
	if keepFrom <= 0 {
		return
	}

	for i := int64(1); i <= keepFrom; i++ {
		path := filepath.Join(c.checkpointDir, fmt.Sprintf("cp-%d.json", i))
		_ = os.Remove(path)
	}
}

// LastCompleted returns the most recently completed checkpoint.
func (c *Coordinator) LastCompleted() *Checkpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastCompleted
}

// Restore loads the latest completed checkpoint from disk.
func (c *Coordinator) Restore() (*Checkpoint, error) {
	if c.checkpointDir == "" {
		return nil, nil
	}

	entries, err := os.ReadDir(c.checkpointDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Find the latest completed checkpoint
	var latestID int64
	for _, entry := range entries {
		var id int64
		if _, err := fmt.Sscanf(entry.Name(), "cp-%d.json", &id); err == nil {
			if id > latestID {
				latestID = id
			}
		}
	}

	if latestID == 0 {
		return nil, nil
	}

	path := filepath.Join(c.checkpointDir, fmt.Sprintf("cp-%d.json", latestID))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}

	return &cp, nil
}

// OperatorCheckpointer is used by operators to save and restore their state.
type OperatorCheckpointer struct {
	operatorID  string
	coordinator *Coordinator
}

// NewOperatorCheckpointer creates a checkpointer for an operator.
func NewOperatorCheckpointer(operatorID string, coordinator *Coordinator) *OperatorCheckpointer {
	return &OperatorCheckpointer{
		operatorID:  operatorID,
		coordinator: coordinator,
	}
}

// SaveState saves the operator's state for a checkpoint.
func (oc *OperatorCheckpointer) SaveState(checkpointID int64, state interface{}) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling operator state: %w", err)
	}
	return oc.coordinator.Acknowledge(checkpointID, oc.operatorID, data)
}
