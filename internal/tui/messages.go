package tui

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// --- Messages sent to the TUI ---

// StepStartedMsg signals a step has begun processing.
type StepStartedMsg struct {
	Index int
	Total int
}

// ChunkDoneMsg signals a chunk within a step completed.
type ChunkDoneMsg struct {
	StepIndex int
	Chunks    int
}

// StepDoneMsg signals a step finished.
type StepDoneMsg struct {
	Index   int
	Elapsed time.Duration
}

// StepFailedMsg signals a step failed.
type StepFailedMsg struct {
	Index int
	Err   error
}

// JobDoneMsg signals the entire job is complete.
type JobDoneMsg struct{}

// JobFailedMsg signals the job failed.
type JobFailedMsg struct {
	Err error
}

// tickMsg drives the elapsed-time ticker.
type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
