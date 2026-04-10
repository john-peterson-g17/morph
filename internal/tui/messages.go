package tui

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// tickMsg drives the elapsed-time ticker.
type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
