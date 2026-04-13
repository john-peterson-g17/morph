package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/john-peterson-g17/morph/internal/job"
)

// WorkerPool orchestrates concurrent chunk processing for a morph job.
type WorkerPool struct {
	concurrency int
	db          *sql.DB
	planner     *ChunkPlanner
	progress    *ProgressStore
	steps       []job.Step
	maxRetries  int
	maxRows     int64
	program     *tea.Program

	totalRows atomic.Int64
}

// NewWorkerPool creates a worker pool for chunk-based backfill execution.
func NewWorkerPool(db *sql.DB, planner *ChunkPlanner, progress *ProgressStore, steps []job.Step, concurrency, maxRetries int, maxRows int64, program *tea.Program) *WorkerPool {
	return &WorkerPool{
		concurrency: concurrency,
		db:          db,
		planner:     planner,
		progress:    progress,
		steps:       steps,
		maxRetries:  maxRetries,
		maxRows:     maxRows,
		program:     program,
	}
}

// Run dispatches chunks to workers. Blocks until done or ctx is cancelled.
func (wp *WorkerPool) Run(ctx context.Context) error {
	chunks := make(chan ChunkRange, wp.concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, wp.concurrency)

	for i := 1; i <= wp.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for chunk := range chunks {
				if ctx.Err() != nil {
					return
				}
				if err := wp.processChunk(ctx, workerID, chunk); err != nil {
					if ctx.Err() != nil {
						return
					}
					errCh <- err
				}
			}
		}(i)
	}

	go func() {
		defer close(chunks)
		for {
			if ctx.Err() != nil {
				return
			}
			if wp.maxRows > 0 && wp.totalRows.Load() >= wp.maxRows {
				return
			}
			chunk, ok := wp.planner.NextChunk()
			if !ok {
				return
			}
			if wp.progress.IsChunkComplete(chunk) {
				continue
			}
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (wp *WorkerPool) processChunk(ctx context.Context, workerID int, chunk ChunkRange) error {
	chunkStart := time.Now()

	for attempt := 0; attempt <= wp.maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		wp.progress.MarkChunkRunning(chunk, workerID)

		wp.program.Send(MsgChunkStart{
			WorkerID: workerID,
			Chunk:    chunk,
		})

		totalStepRows, err := wp.executeChunkSteps(ctx, workerID, chunk)
		if err == nil {
			chunkDuration := time.Since(chunkStart)
			currentWidth := wp.planner.CurrentWidth()
			wp.progress.MarkChunkComplete(chunk, currentWidth)
			wp.planner.RecordResult(chunkDuration, totalStepRows)

			wp.program.Send(MsgChunkDone{
				WorkerID:        workerID,
				Chunk:           chunk,
				Rows:            totalStepRows,
				Duration:        chunkDuration,
				NextWidth:       currentWidth,
				TotalLoaded:     wp.totalRows.Load(),
				EstimatedChunks: wp.planner.EstimatedTotalChunks(0),
			})
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wp.progress.MarkChunkFailed(chunk, "steps", err)
		wp.program.Send(MsgChunkFailed{
			WorkerID: workerID,
			Chunk:    chunk,
			Err:      err,
			Retrying: attempt < wp.maxRetries,
		})
	}
	return nil
}

func (wp *WorkerPool) executeChunkSteps(ctx context.Context, workerID int, chunk ChunkRange) (int64, error) {
	var totalRows int64

	for i, step := range wp.steps {
		if ctx.Err() != nil {
			return totalRows, ctx.Err()
		}

		wp.program.Send(MsgStepStart{
			WorkerID:  workerID,
			StepIndex: i,
			StepName:  step.Name,
		})

		rows, duration, err := wp.execSQL(ctx, step.ComposeSQL(), chunk.Start, chunk.End)
		if err != nil {
			return totalRows, fmt.Errorf("step %q: %w", step.Name, err)
		}

		wp.progress.MarkChunkStepDone(chunk, step.Name, rows, duration)
		totalRows += rows
		wp.totalRows.Add(rows)

		wp.program.Send(MsgStepDone{
			WorkerID:  workerID,
			StepIndex: i,
			StepName:  step.Name,
			Rows:      rows,
			Duration:  duration,
		})
	}

	return totalRows, nil
}

func (wp *WorkerPool) execSQL(ctx context.Context, query string, chunkStart, chunkEnd time.Time) (int64, time.Duration, error) {
	start := time.Now()
	res, err := wp.db.ExecContext(ctx, query, chunkStart, chunkEnd)
	if err != nil {
		return 0, time.Since(start), err
	}
	rows, _ := res.RowsAffected()
	return rows, time.Since(start), nil
}
