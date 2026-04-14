package tui

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
)

// RunOpts holds the configuration for starting the TUI.
type RunOpts struct {
	JobName       string
	Version       string
	Concurrency   int
	WidthLabel    string
	StepName      string
	BeforeTotal   int
	AfterTotal    int
	ResumedChunks int
	ResumedRows   int64
	Cancel        context.CancelFunc
	Debug         bool
}

// Run starts the TUI and returns a program handle for sending messages.
// The TUI runs in a background goroutine.
func Run(opts RunOpts) (*tea.Program, error) {
	m := New(opts.JobName, opts.Version, opts.Concurrency, opts.WidthLabel, opts.StepName, opts.BeforeTotal, opts.AfterTotal, opts.Cancel, opts.Debug)
	m.chunksOK = opts.ResumedChunks
	m.totalLoaded = opts.ResumedRows
	p := tea.NewProgram(m)

	go func() {
		_, _ = p.Run()
	}()

	return p, nil
}
