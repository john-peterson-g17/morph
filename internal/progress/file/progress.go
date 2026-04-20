package file

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/john-peterson-g17/morph/internal/progress"
)

// Compile-time check that Store implements progress.Store.
var _ progress.Store = (*Store)(nil)

// Store provides goroutine-safe progress tracking backed by a local JSON file.
type Store struct {
	mu   sync.Mutex
	data progress.ProgressData
	path string
}

// New reads an existing progress file or returns an empty store.
func New(path string) (*Store, error) {
	s := &Store{path: path}

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

func (s *Store) save() error {
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

func (s *Store) Init(jobName, jobVersion string, loadStart, loadEnd time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.JobName = jobName
	s.data.JobVersion = jobVersion
	s.data.LoadStart = loadStart
	s.data.LoadEnd = loadEnd
	s.data.StartedAt = time.Now()
	_ = s.save()
}

func (s *Store) HasData() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data.Chunks) > 0
}

func (s *Store) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = progress.ProgressData{}
	return os.Remove(s.path)
}

func (s *Store) findChunk(c progress.ChunkRange) int {
	for i := range s.data.Chunks {
		if s.data.Chunks[i].Start.Equal(c.Start) && s.data.Chunks[i].End.Equal(c.End) {
			return i
		}
	}
	return -1
}

func (s *Store) MarkChunkRunning(c progress.ChunkRange, workerID int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		s.data.Chunks = append(s.data.Chunks, progress.ChunkState{
			Start:    c.Start,
			End:      c.End,
			Status:   "running",
			WorkerID: workerID,
			Tables:   make(map[string]progress.TableResult),
		})
	} else {
		s.data.Chunks[idx].Status = "running"
		s.data.Chunks[idx].WorkerID = workerID
		if s.data.Chunks[idx].Tables == nil {
			s.data.Chunks[idx].Tables = make(map[string]progress.TableResult)
		}
	}
	_ = s.save()
}

func (s *Store) MarkChunkStepDone(c progress.ChunkRange, stepName string, rows int64, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return
	}
	if s.data.Chunks[idx].Tables == nil {
		s.data.Chunks[idx].Tables = make(map[string]progress.TableResult)
	}
	s.data.Chunks[idx].Tables[stepName] = progress.TableResult{
		RowsInserted: rows,
		Duration:     d,
	}
	_ = s.save()
}

func (s *Store) MarkChunkComplete(c progress.ChunkRange, chunkWidth time.Duration) {
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
	_ = s.save()
}

func (s *Store) MarkChunkFailed(c progress.ChunkRange, stepName string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return
	}
	s.data.Chunks[idx].Status = "failed"
	s.data.Chunks[idx].Error = fmt.Sprintf("%s: %v", stepName, err)
	s.data.Chunks[idx].Retries++
	_ = s.save()
}

func (s *Store) IsChunkComplete(c progress.ChunkRange) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return false
	}
	return s.data.Chunks[idx].Status == "complete"
}

func (s *Store) IsChunkFailed(c progress.ChunkRange) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.findChunk(c)
	if idx == -1 {
		return false
	}
	return s.data.Chunks[idx].Status == "failed"
}

func (s *Store) GetResumePoint() (nextStart time.Time, lastWidth time.Duration, completedCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data.Chunks) == 0 {
		return s.data.LoadStart, s.data.LastChunkWidth, 0
	}

	sorted := make([]progress.ChunkState, len(s.data.Chunks))
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

func (s *Store) TotalRows() int64 {
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

func (s *Store) Summary() (completedChunks, failedChunks int, rowsByStep map[string]int64, avgRuntime time.Duration) {
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
