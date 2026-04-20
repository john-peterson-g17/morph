package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/john-peterson-g17/morph/internal/progress"
)

// Compile-time check that Store implements progress.Store.
var _ progress.Store = (*Store)(nil)

// Store provides progress tracking backed by an S3-compatible object store.
// It keeps progress data in memory and flushes the full JSON to S3 on each mutation.
type Store struct {
	mu     sync.Mutex
	data   progress.ProgressData
	client *minio.Client
	bucket string
	key    string
}

// New creates an S3-backed progress store. It connects to the given
// endpoint and attempts to load existing progress from the bucket.
// Credentials are read from standard AWS environment variables
// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
func New(endpoint, bucket, prefix, region, jobName, jobVersion string) (*Store, error) {
	useSSL := os.Getenv("S3_USE_SSL") != "false"

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Region: region,
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}

	key := fmt.Sprintf("%s/%s.%s.progress.json", prefix, jobName, jobVersion)
	if prefix == "" {
		key = fmt.Sprintf("%s.%s.progress.json", jobName, jobVersion)
	}

	s := &Store{
		client: client,
		bucket: bucket,
		key:    key,
	}

	// Try to load existing progress.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err == nil {
		raw, readErr := io.ReadAll(obj)
		_ = obj.Close()
		if readErr == nil && len(raw) > 0 {
			_ = json.Unmarshal(raw, &s.data)
		}
	}

	return s, nil
}

func (s *Store) save() error {
	s.data.LastUpdated = time.Now()
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = s.client.PutObject(ctx, s.bucket, s.key, bytes.NewReader(raw), int64(len(raw)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return s.client.RemoveObject(ctx, s.bucket, s.key, minio.RemoveObjectOptions{})
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
