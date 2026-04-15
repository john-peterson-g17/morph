package progress

import "time"

// ChunkRange represents a time-bounded chunk of work.
type ChunkRange struct {
	Start time.Time
	End   time.Time
}

// TableResult records the outcome of a single step within a chunk.
type TableResult struct {
	RowsInserted int64         `json:"rows_inserted"`
	Duration     time.Duration `json:"duration"`
}

// ChunkState tracks the state of a single time-bounded chunk.
type ChunkState struct {
	Start       time.Time              `json:"start"`
	End         time.Time              `json:"end"`
	Status      string                 `json:"status"` // pending | running | complete | failed
	WorkerID    int                    `json:"worker_id,omitempty"`
	Retries     int                    `json:"retries,omitempty"`
	Tables      map[string]TableResult `json:"tables,omitempty"`
	Error       string                 `json:"error,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
}

// ProgressData is the top-level progress structure persisted to JSON.
type ProgressData struct {
	JobName        string        `json:"job_name"`
	JobVersion     string        `json:"job_version"`
	LoadStart      time.Time     `json:"load_start"`
	LoadEnd        time.Time     `json:"load_end"`
	StartedAt      time.Time     `json:"started_at"`
	LastUpdated    time.Time     `json:"last_updated"`
	LastChunkWidth time.Duration `json:"last_chunk_width"`
	Chunks         []ChunkState  `json:"chunks"`
}

// Store defines the interface for persisting chunk progress.
// Implementations must be safe for concurrent use.
type Store interface {
	Init(jobName, jobVersion string, loadStart, loadEnd time.Time)
	HasData() bool
	Reset() error
	MarkChunkRunning(c ChunkRange, workerID int)
	MarkChunkStepDone(c ChunkRange, stepName string, rows int64, d time.Duration)
	MarkChunkComplete(c ChunkRange, chunkWidth time.Duration)
	MarkChunkFailed(c ChunkRange, stepName string, err error)
	IsChunkComplete(c ChunkRange) bool
	GetResumePoint() (nextStart time.Time, lastWidth time.Duration, completedCount int)
	TotalRows() int64
	Summary() (completedChunks, failedChunks int, rowsByStep map[string]int64, avgRuntime time.Duration)
}
