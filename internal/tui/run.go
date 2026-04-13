package tui

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
)

// Run starts the TUI and returns a program handle for sending messages.
// The TUI runs in a background goroutine.
func Run(jobName string, concurrency int, cancel context.CancelFunc, debug bool) (*tea.Program, error) {
	m := New(jobName, concurrency, cancel, debug)
	p := tea.NewProgram(m)

	go func() {
		if _, err := p.Run(); err != nil {
			// Program exited — nothing to do.
		}
	}()

	return p, nil
}
