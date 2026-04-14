package engine

import "time"

// Messages sent from the worker pool to the TUI program.

// MsgChunkStart signals a worker started a new chunk.
type MsgChunkStart struct {
	WorkerID int
	Chunk    ChunkRange
}

// MsgStepStart signals a worker started a step within a chunk.
type MsgStepStart struct {
	WorkerID  int
	StepIndex int
	StepName  string
}

// MsgStepDone signals a step within a chunk completed.
type MsgStepDone struct {
	WorkerID  int
	StepIndex int
	StepName  string
	Rows      int64
	Duration  time.Duration
	SQL       string
}

// MsgChunkDone signals a chunk completed all steps.
type MsgChunkDone struct {
	WorkerID        int
	Chunk           ChunkRange
	Rows            int64
	Duration        time.Duration
	NextWidth       time.Duration
	TotalLoaded     int64
	EstimatedChunks int
	Queries         []string
}

// MsgChunkFailed signals a chunk failed.
type MsgChunkFailed struct {
	WorkerID int
	Chunk    ChunkRange
	Err      error
	Retrying bool
}

// MsgHookStart signals a before/after hook is about to execute.
type MsgHookStart struct {
	Phase string // "before" or "after"
	Name  string
	Index int
	Total int
}

// MsgHookDone signals a before/after hook finished executing.
type MsgHookDone struct {
	Phase    string // "before" or "after"
	Name     string
	Index    int
	Total    int
	Duration time.Duration
	Err      error
	Skipped  bool
}

// MsgJobDone signals the entire job is complete.
type MsgJobDone struct {
	Err error
}
