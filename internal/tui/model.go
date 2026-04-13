package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/john-peterson-g17/morph/internal/engine"
	"github.com/john-peterson-g17/morph/internal/monitor"
)

var (
	headerStyle  = lipgloss.NewStyle().Bold(true)
	dimStyle     = lipgloss.NewStyle().Faint(true)
	boldStyle    = lipgloss.NewStyle().Bold(true)
	activeStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	barFillStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	warnStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	failStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

// Model is the bubbletea model for morph job output.
type Model struct {
	jobName         string
	workers         []workerDisplay
	totalLoaded     int64
	nextWidth       time.Duration
	chunksOK        int
	chunksFailed    int
	estimatedChunks int
	startTime       time.Time
	recentLog       []logEntry
	done            bool
	loadErr         error
	cancel          context.CancelFunc
	debug           bool
	dbHealth        *monitor.Health
}

// New creates a new TUI model for a morph job.
func New(jobName string, concurrency int, cancel context.CancelFunc, debug bool) Model {
	workers := make([]workerDisplay, concurrency)
	for i := range workers {
		workers[i] = workerDisplay{id: i + 1, idle: true}
	}
	return Model{
		jobName:   jobName,
		workers:   workers,
		startTime: time.Now(),
		cancel:    cancel,
		debug:     debug,
	}
}

func (m Model) Init() tea.Cmd {
	return tickCmd()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.cancel()
			return m, tea.Quit
		}
		return m, nil

	case tickMsg:
		return m, tickCmd()

	case engine.MsgChunkStart:
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) {
			m.workers[idx] = workerDisplay{
				id:      msg.WorkerID,
				chunk:   &msg.Chunk,
				started: time.Now(),
				idle:    false,
			}
		}

	case engine.MsgStepStart:
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) {
			m.workers[idx].step = msg.StepName
			m.workers[idx].liveRows = 0
		}

	case engine.MsgStepDone:
		// next MsgStepStart or MsgChunkDone will update the display

	case engine.MsgChunkDone:
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) {
			m.workers[idx].idle = true
			m.workers[idx].chunk = nil
			m.workers[idx].step = ""
			m.workers[idx].liveRows = 0
		}
		m.totalLoaded = msg.TotalLoaded
		m.nextWidth = msg.NextWidth
		m.estimatedChunks = m.chunksOK + 1 + msg.EstimatedChunks
		m.chunksOK++

		chunkWidth := msg.Chunk.End.Sub(msg.Chunk.Start)
		text := fmt.Sprintf("#%d  %s → %s (%s)  %s rows  %s",
			msg.WorkerID,
			msg.Chunk.Start.Format("01-02 15:04"),
			msg.Chunk.End.Format("15:04"),
			engine.FormatDuration(chunkWidth),
			engine.FormatRows(msg.Rows),
			engine.FormatDuration(msg.Duration))
		m.recentLog = append(m.recentLog, logEntry{text: text, queries: msg.Queries})
		maxLog := 8
		if m.debug {
			maxLog = 4
		}
		if len(m.recentLog) > maxLog {
			m.recentLog = m.recentLog[len(m.recentLog)-maxLog:]
		}

	case engine.MsgChunkFailed:
		m.chunksFailed++
		status := "FAILED"
		if msg.Retrying {
			status = "RETRYING"
		}
		text := fmt.Sprintf("%s → %s  %s: %v",
			msg.Chunk.Start.Format("01-02 15:04"),
			msg.Chunk.End.Format("15:04"),
			status, msg.Err)
		m.recentLog = append(m.recentLog, logEntry{text: text})
		maxLog := 8
		if m.debug {
			maxLog = 4
		}
		if len(m.recentLog) > maxLog {
			m.recentLog = m.recentLog[len(m.recentLog)-maxLog:]
		}

	case engine.MsgJobDone:
		m.done = true
		m.loadErr = msg.Err
		return m, tea.Quit

	case monitor.MsgDBStats:
		m.dbHealth = &msg.Health
	}

	return m, nil
}

func (m Model) View() string {
	var b strings.Builder

	elapsed := time.Since(m.startTime)

	var rateStr, etaStr string
	if elapsed.Seconds() > 1 && m.totalLoaded > 0 {
		rate := float64(m.totalLoaded) / elapsed.Seconds()
		rateStr = fmt.Sprintf("  %s/s", engine.FormatRows(int64(rate)))
		if m.estimatedChunks > m.chunksOK {
			remaining := float64(m.estimatedChunks-m.chunksOK) / float64(m.chunksOK) * elapsed.Seconds()
			etaStr = fmt.Sprintf("  ETA %s",
				engine.FormatDuration(time.Duration(remaining*float64(time.Second))))
		}
	}

	b.WriteString("\n")
	b.WriteString(headerStyle.Render(fmt.Sprintf("⚡ %s", m.jobName)))
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("  %s rows loaded%s%s\n",
		boldStyle.Render(engine.FormatRows(m.totalLoaded)),
		rateStr, etaStr))

	// Progress bar.
	if m.estimatedChunks > 0 {
		pct := float64(m.chunksOK) / float64(m.estimatedChunks)
		if pct > 1 {
			pct = 1
		}
		const barWidth = 30
		filled := int(pct * barWidth)
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		b.WriteString(fmt.Sprintf("  [%s] %.1f%%\n",
			barFillStyle.Render(bar), pct*100))
	}

	chunksStr := fmt.Sprintf("  Elapsed: %s  Chunks: %d done", engine.FormatDuration(elapsed), m.chunksOK)
	if m.estimatedChunks > 0 && m.estimatedChunks > m.chunksOK {
		remaining := m.estimatedChunks - m.chunksOK
		chunksStr += fmt.Sprintf(" / ~%d remaining (~%d total)", remaining, m.estimatedChunks)
	}
	b.WriteString(chunksStr)
	if m.chunksFailed > 0 {
		b.WriteString(failStyle.Render(fmt.Sprintf("  %d failed", m.chunksFailed)))
	}
	if m.nextWidth > 0 {
		b.WriteString(fmt.Sprintf("  Width: %s", engine.FormatDuration(m.nextWidth)))
	}
	b.WriteString("\n\n")

	b.WriteString(headerStyle.Render("Workers"))
	b.WriteString("\n")
	for _, w := range m.workers {
		if w.idle {
			b.WriteString(dimStyle.Render(fmt.Sprintf("  #%d  idle", w.id)))
			b.WriteString("\n")
			continue
		}

		chunkStr := ""
		if w.chunk != nil {
			width := w.chunk.End.Sub(w.chunk.Start)
			chunkStr = fmt.Sprintf("%s → %s (%s)",
				w.chunk.Start.Format("01-02 15:04"),
				w.chunk.End.Format("15:04"),
				engine.FormatDuration(width))
		}

		stepStr := w.step
		if stepStr == "" {
			stepStr = "starting"
		}

		rowsStr := ""
		if w.liveRows > 0 {
			rowsStr = fmt.Sprintf("~%s", engine.FormatRows(w.liveRows))
		}

		workerElapsed := ""
		if !w.started.IsZero() {
			workerElapsed = engine.FormatDuration(time.Since(w.started))
		}

		b.WriteString(fmt.Sprintf("  #%d  %s  %-22s %10s  %s\n",
			w.id,
			activeStyle.Render(chunkStr),
			stepStr, rowsStr,
			dimStyle.Render(workerElapsed)))
	}
	b.WriteString("\n")

	if len(m.recentLog) > 0 {
		b.WriteString(headerStyle.Render("Completed"))
		b.WriteString("\n")
		for _, entry := range m.recentLog {
			b.WriteString(fmt.Sprintf("  %s\n", entry.text))
			if m.debug {
				for _, q := range entry.queries {
					b.WriteString(dimStyle.Render(fmt.Sprintf("    [DEBUG]: Query Executed: %s",
						strings.Join(strings.Fields(q), " "))))
					b.WriteString("\n")
				}
			}
		}
	}

	// Database health section — rendered at the bottom.
	if m.dbHealth != nil {
		b.WriteString("\n")
		overallStyle := styleForLevel(m.dbHealth.Overall)
		b.WriteString(headerStyle.Render("Database"))
		b.WriteString("  ")
		b.WriteString(overallStyle.Render(fmt.Sprintf("%s %s", m.dbHealth.Overall.Icon(), m.dbHealth.Overall)))
		b.WriteString("\n")
		for _, sig := range m.dbHealth.Signals {
			valStyle := styleForLevel(sig.Level)
			line := fmt.Sprintf("  %-14s %s", sig.Name, valStyle.Render(sig.Value))
			if sig.Baseline != "" {
				line += dimStyle.Render(fmt.Sprintf("  baseline %s", sig.Baseline))
			}
			if sig.Delta != "" {
				line += "  " + valStyle.Render(sig.Delta)
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
	}

	if m.done {
		b.WriteString("\n")
		if m.loadErr != nil {
			b.WriteString(failStyle.Render(fmt.Sprintf("✗ Failed: %s", m.loadErr)))
		} else {
			b.WriteString(boldStyle.Render(fmt.Sprintf("✓ Done in %s  —  %s rows loaded",
				engine.FormatDuration(elapsed),
				engine.FormatRows(m.totalLoaded))))
		}
		b.WriteString("\n")
	}

	return b.String()
}

func styleForLevel(l monitor.Level) lipgloss.Style {
	switch l {
	case monitor.LevelYellow:
		return warnStyle
	case monitor.LevelRed:
		return failStyle
	default:
		return lipgloss.NewStyle()
	}
}
