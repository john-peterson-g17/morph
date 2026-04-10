package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	successStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	errorStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	activeStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("14"))
)

// Model is the bubbletea model for morph job output.
type Model struct {
	state   JobState
	quiting bool
}

// New creates a new TUI model for a job.
func New(jobName string, steps []string) Model {
	ss := make([]Step, len(steps))
	for i, name := range steps {
		ss[i] = Step{Name: name, Status: StepPending}
	}
	return Model{
		state: JobState{
			JobName:   jobName,
			Steps:     ss,
			StartedAt: time.Now(),
		},
	}
}

func (m Model) Init() tea.Cmd {
	return tickCmd()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quiting = true
			return m, tea.Quit
		}

	case tickMsg:
		return m, tickCmd()

	case StepStartedMsg:
		if msg.Index < len(m.state.Steps) {
			m.state.Steps[msg.Index].Status = StepRunning
			m.state.Steps[msg.Index].Total = msg.Total
			m.state.CurrentStep = msg.Index
		}
		return m, nil

	case ChunkDoneMsg:
		if msg.StepIndex < len(m.state.Steps) {
			m.state.Steps[msg.StepIndex].Chunks = msg.Chunks
		}
		return m, nil

	case StepDoneMsg:
		if msg.Index < len(m.state.Steps) {
			m.state.Steps[msg.Index].Status = StepDone
			m.state.Steps[msg.Index].Elapsed = msg.Elapsed
		}
		return m, nil

	case StepFailedMsg:
		if msg.Index < len(m.state.Steps) {
			m.state.Steps[msg.Index].Status = StepFailed
			if msg.Err != nil {
				m.state.Steps[msg.Index].ErrorMsg = msg.Err.Error()
			}
		}
		return m, nil

	case JobDoneMsg:
		m.state.Done = true
		return m, tea.Quit

	case JobFailedMsg:
		m.state.Done = true
		m.state.Err = msg.Err
		return m, tea.Quit
	}

	return m, nil
}

func (m Model) View() string {
	if m.quiting {
		return ""
	}

	var b strings.Builder
	elapsed := time.Since(m.state.StartedAt).Truncate(time.Second)

	b.WriteString(titleStyle.Render(fmt.Sprintf("⚡ %s", m.state.JobName)))
	b.WriteString(dimStyle.Render(fmt.Sprintf("  %s", elapsed)))
	b.WriteString("\n\n")

	for i, step := range m.state.Steps {
		icon := dimStyle.Render("○")
		info := dimStyle.Render(step.Name)

		switch step.Status {
		case StepRunning:
			icon = activeStyle.Render("●")
			progress := ""
			if step.Total > 0 {
				progress = fmt.Sprintf(" %d/%d chunks", step.Chunks, step.Total)
			} else if step.Chunks > 0 {
				progress = fmt.Sprintf(" %d chunks", step.Chunks)
			}
			info = activeStyle.Render(step.Name) + dimStyle.Render(progress)
		case StepDone:
			icon = successStyle.Render("✓")
			info = successStyle.Render(step.Name) + dimStyle.Render(fmt.Sprintf(" %s", step.Elapsed.Truncate(time.Millisecond)))
		case StepFailed:
			icon = errorStyle.Render("✗")
			info = errorStyle.Render(step.Name)
			if step.ErrorMsg != "" {
				info += errorStyle.Render(fmt.Sprintf(" — %s", step.ErrorMsg))
			}
		case StepSkipped:
			icon = dimStyle.Render("–")
			info = dimStyle.Render(step.Name + " (skipped)")
		}

		prefix := "  "
		if i == len(m.state.Steps)-1 {
			prefix = "  "
		}
		b.WriteString(fmt.Sprintf("%s %s %s\n", prefix, icon, info))
	}

	if m.state.Done {
		b.WriteString("\n")
		if m.state.Err != nil {
			b.WriteString(errorStyle.Render(fmt.Sprintf("✗ Failed: %s", m.state.Err)))
		} else {
			b.WriteString(successStyle.Render(fmt.Sprintf("✓ Done in %s", elapsed)))
		}
		b.WriteString("\n")
	}

	return b.String()
}
