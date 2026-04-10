package engine

import (
	"sync"
	"time"
)

// ChunkRange represents a time-bounded chunk of work.
type ChunkRange struct {
	Start time.Time
	End   time.Time
}

// ChunkPlanner generates date-based chunks progressively with adaptive sizing.
type ChunkPlanner struct {
	mu sync.Mutex

	loadStart time.Time
	loadEnd   time.Time

	nextStart    time.Time
	currentWidth time.Duration

	targetRuntime time.Duration
	minWidth      time.Duration
	maxWidth      time.Duration

	consecutiveEmpty  int
	lastNonEmptyWidth time.Duration
}

// NewChunkPlanner creates a planner that generates chunks from loadStart to
// loadEnd with adaptive width.
func NewChunkPlanner(loadStart, loadEnd time.Time, initialWidth, targetRuntime, minWidth, maxWidth time.Duration) *ChunkPlanner {
	return &ChunkPlanner{
		loadStart:     loadStart,
		loadEnd:       loadEnd,
		nextStart:     loadStart,
		currentWidth:  initialWidth,
		targetRuntime: targetRuntime,
		minWidth:      minWidth,
		maxWidth:      maxWidth,
	}
}

// NextChunk returns the next time range to process. Returns false when done.
func (p *ChunkPlanner) NextChunk() (ChunkRange, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.nextStart.Before(p.loadEnd) {
		return ChunkRange{}, false
	}

	start := p.nextStart
	end := start.Add(p.currentWidth)
	if end.After(p.loadEnd) {
		end = p.loadEnd
	}

	p.nextStart = end
	return ChunkRange{Start: start, End: end}, true
}

// RecordResult adjusts the adaptive chunk width based on observed runtime.
func (p *ChunkPlanner) RecordResult(actual time.Duration, rows int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if actual <= 0 {
		return
	}

	// Empty chunk: expand aggressively to skip sparse regions.
	if rows == 0 {
		p.consecutiveEmpty++
		newWidth := time.Duration(float64(p.currentWidth) * 2)
		if newWidth > p.maxWidth {
			newWidth = p.maxWidth
		}
		p.currentWidth = newWidth
		return
	}

	// Non-empty: snap back from sparse-skip width if needed.
	if p.consecutiveEmpty > 0 {
		if p.lastNonEmptyWidth > 0 {
			p.currentWidth = p.lastNonEmptyWidth
		}
		p.consecutiveEmpty = 0
	}

	// suggested = currentWidth * (targetRuntime / actualRuntime)
	ratio := float64(p.targetRuntime) / float64(actual)
	suggested := time.Duration(float64(p.currentWidth) * ratio)

	// Smooth: 70% suggested + 30% current
	newWidth := time.Duration(0.7*float64(suggested) + 0.3*float64(p.currentWidth))

	// Cap growth per step to 1.5x
	if newWidth > time.Duration(float64(p.currentWidth)*1.5) {
		newWidth = time.Duration(float64(p.currentWidth) * 1.5)
	}

	if newWidth < p.minWidth {
		newWidth = p.minWidth
	}
	if newWidth > p.maxWidth {
		newWidth = p.maxWidth
	}

	p.currentWidth = newWidth
	p.lastNonEmptyWidth = newWidth
}

// ResumeFrom sets the planner's cursor and width for resuming.
func (p *ChunkPlanner) ResumeFrom(nextStart time.Time, width time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextStart = nextStart
	if width > 0 {
		if width < p.minWidth {
			width = p.minWidth
		}
		if width > p.maxWidth {
			width = p.maxWidth
		}
		p.currentWidth = width
	}
}

// CurrentWidth returns the current adaptive chunk width.
func (p *ChunkPlanner) CurrentWidth() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentWidth
}

// EstimatedTotalChunks returns done + estimated remaining chunks.
func (p *ChunkPlanner) EstimatedTotalChunks(completedChunks int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.currentWidth <= 0 {
		return completedChunks
	}
	remaining := time.Duration(0)
	if p.nextStart.Before(p.loadEnd) {
		remaining = p.loadEnd.Sub(p.nextStart)
	}
	return completedChunks + int(remaining/p.currentWidth)
}
