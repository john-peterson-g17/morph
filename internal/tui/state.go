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
