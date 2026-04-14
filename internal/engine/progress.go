package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

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

// ProgressStore provides goroutine-safe access to the progress file.
type ProgressStore struct {
	mu   sync.Mutex
	data ProgressData
	path string
}

// LoadProgressStore reads an existing progress file or returns an empty store.
func LoadProgressStore(path string) (*ProgressStore, error) {
	s := &ProgressStore{path: path}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("read progress file: %w", err)
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return nil, fmt.Errorf("parse progress file: %w", err)
	}
	return s, nil
}

// Save atomically writes the progress file.
func (s *ProgressStore) Save() error {
	s.data.LastUpdated = time.Now()
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Init sets the initial progress metadata for a new run.
func (s *ProgressStore) Init(jobName, jobVersion string, loadStart, loadEnd time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.JobName = jobName
	s.data.JobVersion = jobVersion
	s.data.LoadStart = loadStart
	s.data.LoadEnd = loadEnd
	s.data.StartedAt = time.Now()
	_ = s.Save()
}

// HasData returns true if the progress file had existing chunk data.
func (s *ProgressStore) HasData() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data.Chunks) > 0
}

// Reset clears all progress state and removes the file.
func (s *ProgressStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = ProgressData{}
	return os.Remove(s.path)
}

func (s *ProgressStore) findChunk(c ChunkRange) int {
	for i := range s.data.Chunks {
		if s.data.Chunks[i].Start.Equal(c.Start) && s.data.Chunks[i].End.Equal(c.End) {
			return i
		}
	}
	return -1
}

// MarkChunkRunning records that a worker has started processing a chunk.
func (s *ProgressStore) MarkChunkRunning(c ChunkRange, workerID int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		s.data.Chunks = append(s.data.Chunks, ChunkState{
			Start:    c.Start,
			End:      c.End,
			Status:   "running",
			WorkerID: workerID,
			Tables:   make(map[string]TableResult),
		})
	} else {
		s.data.Chunks[idx].Status = "running"
		s.data.Chunks[idx].WorkerID = workerID
		if s.data.Chunks[idx].Tables == nil {
			s.data.Chunks[idx].Tables = make(map[string]TableResult)
		}
	}
	_ = s.Save()
}

// MarkChunkStepDone records that a step within a chunk has finished.
func (s *ProgressStore) MarkChunkStepDone(c ChunkRange, stepName string, rows int64, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return
	}
	if s.data.Chunks[idx].Tables == nil {
		s.data.Chunks[idx].Tables = make(map[string]TableResult)
	}
	s.data.Chunks[idx].Tables[stepName] = TableResult{
		RowsInserted: rows,
		Duration:     d,
	}
	_ = s.Save()
}

// MarkChunkComplete records a chunk as successfully finished.
func (s *ProgressStore) MarkChunkComplete(c ChunkRange, chunkWidth time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return
	}
	now := time.Now()
	s.data.Chunks[idx].Status = "complete"
	s.data.Chunks[idx].CompletedAt = &now
	s.data.LastChunkWidth = chunkWidth
	_ = s.Save()
}

// MarkChunkFailed records a chunk as failed with an error message.
func (s *ProgressStore) MarkChunkFailed(c ChunkRange, stepName string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return
	}
	s.data.Chunks[idx].Status = "failed"
	s.data.Chunks[idx].Error = fmt.Sprintf("%s: %v", stepName, err)
	s.data.Chunks[idx].Retries++
	_ = s.Save()
}

// IsChunkComplete returns true if the given chunk range is already complete.
func (s *ProgressStore) IsChunkComplete(c ChunkRange) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return false
	}
	return s.data.Chunks[idx].Status == "complete"
}

// GetResumePoint returns the start time for resuming and the last chunk width.
func (s *ProgressStore) GetResumePoint() (nextStart time.Time, lastWidth time.Duration, completedCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data.Chunks) == 0 {
		return s.data.LoadStart, s.data.LastChunkWidth, 0
	}

	sorted := make([]ChunkState, len(s.data.Chunks))
	copy(sorted, s.data.Chunks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Start.Before(sorted[j].Start)
	})

	cursor := s.data.LoadStart
	completed := 0
	for _, c := range sorted {
		if c.Status == "complete" && !c.Start.After(cursor) {
			if c.End.After(cursor) {
				cursor = c.End
			}
			completed++
		}
	}

	return cursor, s.data.LastChunkWidth, completed
}

// TotalRows returns the sum of all rows inserted across completed chunks.
func (s *ProgressStore) TotalRows() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, c := range s.data.Chunks {
		if c.Status == "complete" {
			for _, result := range c.Tables {
				total += result.RowsInserted
			}
		}
	}
	return total
}

// Summary returns aggregate stats from all recorded chunks.
func (s *ProgressStore) Summary() (completedChunks, failedChunks int, rowsByStep map[string]int64, avgRuntime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rowsByStep = make(map[string]int64)
	var totalRuntime time.Duration

	for _, c := range s.data.Chunks {
		switch c.Status {
		case "complete":
			completedChunks++
			var chunkDuration time.Duration
			for step, result := range c.Tables {
				rowsByStep[step] += result.RowsInserted
				chunkDuration += result.Duration
			}
			totalRuntime += chunkDuration
		case "failed":
			failedChunks++
		}
	}

	if completedChunks > 0 {
		avgRuntime = totalRuntime / time.Duration(completedChunks)
	}
	return
}
