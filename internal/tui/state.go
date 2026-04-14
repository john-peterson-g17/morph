package tui

import (
	"time"

	"github.com/john-peterson-g17/morph/internal/engine"
)

// workerDisplay holds the rendering state for a single worker.
type workerDisplay struct {
	id       int
	chunk    *engine.ChunkRange
	step     string
	liveRows int64
	started  time.Time
	idle     bool
}

// completedChunk holds data for a finished chunk row in the completed table.
type completedChunk struct {
	workerID int
	chunk    engine.ChunkRange
	rows     int64
	duration time.Duration
}

// logEntry represents one completed-chunk entry with optional debug queries.
type logEntry struct {
	text    string
	queries []string
}

// hookEntry represents a completed before/after hook.
type hookEntry struct {
	name     string
	duration time.Duration
	phase    string
	err      error
	skipped  bool
}

// phase tracks which tab is active.
type phase int

const (
	phaseBefore phase = iota
	phaseMorph
	phaseAfter
)
