package tui

import tea "github.com/charmbracelet/bubbletea"

// Run starts the TUI and blocks until it exits. Send messages to the
// returned program to drive updates from your backfill logic.
func Run(jobName string, steps []string) (*tea.Program, error) {
	m := New(jobName, steps)
	p := tea.NewProgram(m)

	go func() {
		if _, err := p.Run(); err != nil {
			// Program exited — nothing to do.
		}
	}()

	return p, nil
}
