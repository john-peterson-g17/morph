package tui

import (
	"time"
)

// StepStatus represents the state of a single step.
type StepStatus int

const (
	StepPending StepStatus = iota
	StepRunning
	StepDone
	StepFailed
	StepSkipped
)

// Step holds the display state of one backfill step.
type Step struct {
	Name     string
	Status   StepStatus
	Chunks   int
	Total    int
	Elapsed  time.Duration
	ErrorMsg string
}

// JobState is the data the TUI renders. Commands send updates via messages.
type JobState struct {
	JobName     string
	Steps       []Step
	StartedAt   time.Time
	CurrentStep int
	Done        bool
	Err         error
}
