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

// ── Color palette ─────────────────────────────────────────────────────────

var (
	colMuted   = lipgloss.Color("#4a5280")
	colBody    = lipgloss.Color("#b8c4d8")
	colCyan    = lipgloss.Color("#00d4ff")
	colPurple  = lipgloss.Color("#8b5cf6")
	colAmber   = lipgloss.Color("#e0af68")
	colRed     = lipgloss.Color("#f87171")
	colGreen   = lipgloss.Color("#4ade80")
	colSurface = lipgloss.Color("#1a1f35")
)

// ── Reusable styles ───────────────────────────────────────────────────────

var (
	boldStyle    = lipgloss.NewStyle().Bold(true).Foreground(colBody)
	dimStyle     = lipgloss.NewStyle().Foreground(colMuted)
	bodyStyle    = lipgloss.NewStyle().Foreground(colBody)
	cyanStyle    = lipgloss.NewStyle().Foreground(colCyan)
	purpleStyle  = lipgloss.NewStyle().Foreground(colPurple)
	amberStyle   = lipgloss.NewStyle().Foreground(colAmber)
	redStyle     = lipgloss.NewStyle().Foreground(colRed)
	greenStyle   = lipgloss.NewStyle().Foreground(colGreen)
	sectionStyle = lipgloss.NewStyle().Bold(true).Foreground(colBody).MarginBottom(0)
)

// ── Model ─────────────────────────────────────────────────────────────────

// Model is the bubbletea model for morph job output.
type Model struct {
	// Job metadata.
	jobName     string
	version     string
	concurrency int
	widthLabel  string
	stepName    string

	// Before/after hook counts (set on first MsgHookStart of each phase).
	beforeTotal int
	afterTotal  int

	// Active phase for the tab bar.
	phase phase

	// Workers.
	workers []workerDisplay

	// Morph progress.
	totalLoaded     int64
	nextWidth       time.Duration
	chunksOK        int
	chunksFailed    int
	estimatedChunks int
	startTime       time.Time

	// Completed chunks table.
	completed []completedChunk

	// Failed chunks table.
	failed []failedChunk

	// Legacy log (debug queries).
	recentLog []logEntry

	done    bool
	loadErr error
	cancel  context.CancelFunc
	debug   bool

	// Database health.
	dbHealth *monitor.Health

	// Adaptive target runtime for latency colouring.
	targetRuntime time.Duration

	// Terminal dimensions.
	termWidth  int
	termHeight int

	// Hook progress state.
	hookPhase        string // "before", "after", or ""
	hookCurrent      string
	hookCurrentStart time.Time
	hookStarted      time.Time
	hookLog          []hookEntry
	hookTotal        int
	hookDone         int
	beforeHookLog    []hookEntry
	beforeElapsed    time.Duration
	afterHookLog     []hookEntry
	afterElapsed     time.Duration
}

// New creates a new TUI model for a morph job.
func New(jobName, version string, concurrency int, widthLabel, stepName string, beforeTotal, afterTotal int, targetRuntime time.Duration, cancel context.CancelFunc, debug bool) Model {
	workers := make([]workerDisplay, concurrency)
	for i := range workers {
		workers[i] = workerDisplay{id: i + 1, idle: true}
	}
	return Model{
		jobName:       jobName,
		version:       version,
		concurrency:   concurrency,
		widthLabel:    widthLabel,
		stepName:      stepName,
		beforeTotal:   beforeTotal,
		afterTotal:    afterTotal,
		targetRuntime: targetRuntime,
		workers:       workers,
		startTime:     time.Now(),
		cancel:        cancel,
		debug:         debug,
		phase:         phaseBefore,
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

	case tea.WindowSizeMsg:
		m.termWidth = msg.Width
		m.termHeight = msg.Height
		return m, nil

	case tickMsg:
		return m, tickCmd()

	case engine.MsgChunkStart:
		m.phase = phaseMorph
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) {
			m.workers[idx] = workerDisplay{
				id:       msg.WorkerID,
				chunk:    &msg.Chunk,
				started:  time.Now(),
				idle:     false,
				retrying: m.workers[idx].retrying,
			}
		}

	case engine.MsgStepStart:
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) {
			m.workers[idx].step = msg.StepName
			m.workers[idx].liveRows = 0
		}

	case engine.MsgStepDone:
		// handled by next MsgStepStart or MsgChunkDone

	case engine.MsgChunkDone:
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) {
			m.workers[idx].idle = true
			m.workers[idx].chunk = nil
			m.workers[idx].step = ""
			m.workers[idx].liveRows = 0
			m.workers[idx].retrying = false
		}
		m.totalLoaded = msg.TotalLoaded
		m.nextWidth = msg.NextWidth
		m.estimatedChunks = m.chunksOK + 1 + msg.EstimatedChunks
		m.chunksOK++

		// Add to completed table.
		m.completed = append(m.completed, completedChunk{
			workerID: msg.WorkerID,
			chunk:    msg.Chunk,
			rows:     msg.Rows,
			duration: msg.Duration,
		})
		ml := 8
		if len(m.completed) > ml {
			m.completed = m.completed[len(m.completed)-ml:]
		}

		// Legacy log for debug queries.
		if m.debug && len(msg.Queries) > 0 {
			chunkWidth := msg.Chunk.End.Sub(msg.Chunk.Start)
			text := fmt.Sprintf("#%d  %s → %s (%s)  %s rows  %s",
				msg.WorkerID,
				msg.Chunk.Start.Format("01-02 15:04"),
				msg.Chunk.End.Format("15:04"),
				engine.FormatDuration(chunkWidth),
				engine.FormatRows(msg.Rows),
				engine.FormatDuration(msg.Duration))
			m.recentLog = append(m.recentLog, logEntry{text: text, queries: msg.Queries})
			if len(m.recentLog) > 4 {
				m.recentLog = m.recentLog[len(m.recentLog)-4:]
			}
		}

	case engine.MsgChunkFailed:
		m.chunksFailed++
		if idx := msg.WorkerID - 1; idx >= 0 && idx < len(m.workers) && msg.Retrying {
			m.workers[idx].retrying = true
		}
		m.failed = append(m.failed, failedChunk{
			workerID: msg.WorkerID,
			chunk:    msg.Chunk,
			err:      msg.Err.Error(),
			retrying: msg.Retrying,
			at:       time.Now(),
		})
		if len(m.failed) > 8 {
			m.failed = m.failed[len(m.failed)-8:]
		}

	case engine.MsgJobDone:
		m.done = true
		m.loadErr = msg.Err
		return m, tea.Quit

	case engine.MsgHookStart:
		if m.hookPhase != msg.Phase {
			m.hookLog = nil
			m.hookDone = 0
			m.hookStarted = time.Now()
		}
		m.hookPhase = msg.Phase
		m.hookCurrent = msg.Name
		m.hookCurrentStart = time.Now()
		m.hookTotal = msg.Total

		switch msg.Phase {
		case "before":
			m.phase = phaseBefore
			m.beforeTotal = msg.Total
		case "after":
			m.phase = phaseAfter
			m.afterTotal = msg.Total
		}

	case engine.MsgHookDone:
		m.hookDone++
		entry := hookEntry{
			name:     msg.Name,
			duration: msg.Duration,
			phase:    msg.Phase,
			err:      msg.Err,
			skipped:  msg.Skipped,
		}
		m.hookLog = append(m.hookLog, entry)
		m.hookCurrent = ""

		switch msg.Phase {
		case "before":
			m.beforeHookLog = append(m.beforeHookLog, entry)
			m.beforeElapsed = time.Since(m.hookStarted)
		case "after":
			m.afterHookLog = append(m.afterHookLog, entry)
			m.afterElapsed = time.Since(m.hookStarted)
		}

		if m.hookDone >= m.hookTotal {
			switch msg.Phase {
			case "before":
				m.beforeElapsed = time.Since(m.hookStarted)
			case "after":
				m.afterElapsed = time.Since(m.hookStarted)
			}
			m.hookPhase = ""
			m.hookDone = 0
			m.hookTotal = 0
		}

	case monitor.MsgDBStats:
		m.dbHealth = &msg.Health
	}

	return m, nil
}

// ── View ──────────────────────────────────────────────────────────────────

func (m Model) View() string {
	var b strings.Builder
	width := m.termWidth
	if width == 0 {
		width = 80
	}

	divider := dimStyle.Render(strings.Repeat("─", width))

	// ── Top header bar ──────────────────────────────────────────────
	b.WriteString("\n\n")
	b.WriteString(m.renderHeader(width))
	b.WriteString("\n\n")

	// ── Stage tabs ──────────────────────────────────────────────────
	b.WriteString(m.renderTabs())
	b.WriteString("\n\n")

	// ── Progress section ────────────────────────────────────────────
	b.WriteString(m.renderProgress(width))
	b.WriteString("\n\n")
	b.WriteString(divider)
	b.WriteString("\n")

	// ── Main content: hooks or workers+db side by side ──────────────
	switch m.phase {
	case phaseBefore, phaseAfter:
		b.WriteString(m.renderHooksAndDB(width))
	default:
		b.WriteString(m.renderWorkersAndDB(width))
	}

	// ── Completed table (only in morph phase) ───────────────────────
	if m.phase == phaseMorph && len(m.completed) > 0 {
		// Determine how many entries fit in available terminal space.
		maxEntries := 5
		if m.termHeight > 0 {
			usedLines := strings.Count(b.String(), "\n") + 1
			// Reserve lines for divider(2) + completed header(3) + footer(2).
			available := m.termHeight - usedLines - 7
			if available > 0 {
				maxEntries = available
			}
			if maxEntries > 8 {
				maxEntries = 8
			}
		}
		b.WriteString("\n")
		b.WriteString(divider)
		b.WriteString("\n")
		b.WriteString(m.renderCompleted(width, maxEntries))
	}

	// ── Errors table (only in morph phase) ──────────────────────────
	if m.phase == phaseMorph && len(m.failed) > 0 {
		b.WriteString("\n")
		b.WriteString(divider)
		b.WriteString("\n")
		b.WriteString(m.renderErrors(width))
	}

	// ── Debug queries ───────────────────────────────────────────────
	if m.debug && len(m.recentLog) > 0 {
		b.WriteString("\n")
		for _, entry := range m.recentLog {
			for _, q := range entry.queries {
				b.WriteString(dimStyle.Render(fmt.Sprintf("  [DEBUG]: %s",
					strings.Join(strings.Fields(q), " "))))
				b.WriteString("\n")
			}
		}
	}

	// ── Done/error footer ───────────────────────────────────────────
	if m.done {
		elapsed := time.Since(m.startTime)
		b.WriteString("\n")
		if m.loadErr != nil {
			b.WriteString(redStyle.Render(fmt.Sprintf("  ✗ Failed: %s", m.loadErr)))
		} else {
			var detail string
			if m.totalLoaded > 0 {
				detail = fmt.Sprintf("%s rows processed", engine.FormatRows(m.totalLoaded))
			} else {
				detail = fmt.Sprintf("%s chunks completed", engine.FormatRows(int64(m.chunksOK)))
			}
			b.WriteString(greenStyle.Render(fmt.Sprintf("  ✓ Done in %s — %s",
				engine.FormatDuration(elapsed),
				detail)))
		}
		b.WriteString("\n")
	}

	return b.String()
}

// ── Header ────────────────────────────────────────────────────────────────

func (m Model) renderHeader(width int) string {
	bolt := cyanStyle.Bold(true).Render("⚡")
	brand := cyanStyle.Bold(true).Render("morph")

	sep := dimStyle.Render("  │  ")

	var parts []string
	parts = append(parts, bolt+" "+brand)
	parts = append(parts, boldStyle.Render(m.jobName))
	if m.version != "" {
		parts = append(parts, purpleStyle.Render(m.version))
	}
	parts = append(parts, dimStyle.Render("concurrency ")+bodyStyle.Render(fmt.Sprintf("%d", m.concurrency)))
	parts = append(parts, dimStyle.Render("width ")+bodyStyle.Render(m.widthLabel))
	if m.stepName != "" {
		parts = append(parts, dimStyle.Render("step ")+bodyStyle.Render(m.stepName))
	}

	return strings.Join(parts, sep)
}

// ── Tab bar ───────────────────────────────────────────────────────────────

func (m Model) renderTabs() string {
	tabs := make([]string, 0, 3)

	// Before tab.
	beforeLabel := m.buildTabLabel("before", m.beforeTotal, m.beforeHookLog, m.beforeElapsed, phaseBefore)
	tabs = append(tabs, beforeLabel)

	// Morph tab.
	morphLabel := m.buildMorphTab()
	tabs = append(tabs, morphLabel)

	// After tab.
	afterLabel := m.buildTabLabel("after", m.afterTotal, m.afterHookLog, m.afterElapsed, phaseAfter)
	tabs = append(tabs, afterLabel)

	return "  " + strings.Join(tabs, "  ")
}

func (m Model) buildTabLabel(name string, total int, log []hookEntry, elapsed time.Duration, p phase) string {
	done := len(log)
	allDone := total > 0 && done >= total

	// Pill styles.
	activePill := lipgloss.NewStyle().
		Foreground(colCyan).Bold(true).
		Background(colSurface).
		Padding(0, 1)
	completedPill := lipgloss.NewStyle().
		Foreground(colGreen).
		Background(colSurface).
		Padding(0, 1)
	pendingPill := lipgloss.NewStyle().
		Foreground(colMuted).
		Padding(0, 1)

	switch {
	case m.phase == p && !allDone:
		label := fmt.Sprintf("● %s %d / %d", name, done, total)
		return activePill.Render(label)
	case allDone:
		label := fmt.Sprintf("✓ %s %d hooks · %s", name, total, engine.FormatDuration(elapsed))
		return completedPill.Render(label)
	default:
		if total > 0 {
			return pendingPill.Render(fmt.Sprintf("○ %s %d hooks", name, total))
		}
		return pendingPill.Render(fmt.Sprintf("○ %s", name))
	}
}

func (m Model) buildMorphTab() string {
	activePill := lipgloss.NewStyle().
		Foreground(colBody).Bold(true).
		Background(colPurple).
		Padding(0, 1)
	completedPill := lipgloss.NewStyle().
		Foreground(colGreen).
		Background(colSurface).
		Padding(0, 1)
	pendingPill := lipgloss.NewStyle().
		Foreground(colMuted).
		Padding(0, 1)

	switch {
	case m.phase == phaseMorph:
		pct := m.progressPercent()
		label := fmt.Sprintf("■ morph %.1f%%", pct*100)
		return activePill.Render(label)
	case m.chunksOK > 0:
		var label string
		if m.totalLoaded > 0 {
			label = fmt.Sprintf("✓ morph %s rows", engine.FormatRows(m.totalLoaded))
		} else {
			label = fmt.Sprintf("✓ morph %s chunks", engine.FormatRows(int64(m.chunksOK)))
		}
		return completedPill.Render(label)
	default:
		return pendingPill.Render("○ morph")
	}
}

// ── Progress section ──────────────────────────────────────────────────────

func (m Model) renderProgress(width int) string {
	var b strings.Builder

	switch m.phase {
	case phaseBefore, phaseAfter:
		b.WriteString(m.renderHookProgress(width))
	default:
		b.WriteString(m.renderMorphProgress(width))
	}

	return b.String()
}

func (m Model) renderHookProgress(width int) string {
	var b strings.Builder

	pct := 0.0
	if m.hookTotal > 0 {
		pct = float64(m.hookDone) / float64(m.hookTotal) * 100
	}

	elapsed := time.Duration(0)
	if !m.hookStarted.IsZero() {
		elapsed = time.Since(m.hookStarted)
	}

	// "69.2%  9 of 13 hooks complete"
	pctStr := cyanStyle.Bold(true).Render(fmt.Sprintf("  %.1f%%", pct))
	detail := bodyStyle.Render(fmt.Sprintf("  %d of %d hooks complete", m.hookDone, m.hookTotal))
	elapsedStr := dimStyle.Render(fmt.Sprintf("elapsed %s", engine.FormatDuration(elapsed)))

	b.WriteString(pctStr + detail)
	// Right-align elapsed.
	leftLen := lipgloss.Width(pctStr + detail)
	gap := width - leftLen - lipgloss.Width(elapsedStr) - 2
	if gap < 2 {
		gap = 2
	}
	b.WriteString(strings.Repeat(" ", gap))
	b.WriteString(elapsedStr)
	b.WriteString("\n")

	// Progress bar.
	barWidth := width - 4
	if barWidth < 20 {
		barWidth = 20
	}
	filled := 0
	if m.hookTotal > 0 {
		filled = int(float64(m.hookDone) / float64(m.hookTotal) * float64(barWidth))
	}
	if filled > barWidth {
		filled = barWidth
	}
	b.WriteString("  " + renderGradientBar(filled, barWidth))

	return b.String()
}

func (m Model) renderMorphProgress(width int) string {
	var b strings.Builder

	elapsed := time.Since(m.startTime)
	pct := m.progressPercent()

	// Row 1: "22.8%  5,548,397 rows processed   throughput 17,050/s  eta 18m 19s  elapsed 5m 25s"
	pctStr := cyanStyle.Bold(true).Render(fmt.Sprintf("  %.1f%%", pct*100))
	rowsStr := bodyStyle.Render(fmt.Sprintf("  %s rows processed", engine.FormatRows(m.totalLoaded)))

	var stats []string
	if elapsed.Seconds() > 1 && m.chunksOK > 0 {
		if m.totalLoaded > 0 {
			rate := float64(m.totalLoaded) / elapsed.Seconds()
			stats = append(stats, dimStyle.Render("throughput ")+cyanStyle.Render(fmt.Sprintf("%s/s", engine.FormatRows(int64(rate)))))
		} else {
			chunksRate := float64(m.chunksOK) / elapsed.Seconds()
			stats = append(stats, dimStyle.Render("throughput ")+cyanStyle.Render(fmt.Sprintf("%.1f chunks/s", chunksRate)))
		}
		if m.estimatedChunks > m.chunksOK {
			remaining := float64(m.estimatedChunks-m.chunksOK) / float64(m.chunksOK) * elapsed.Seconds()
			stats = append(stats, dimStyle.Render("eta ")+bodyStyle.Render(engine.FormatDuration(time.Duration(remaining*float64(time.Second)))))
		}
	}
	stats = append(stats, dimStyle.Render("elapsed ")+bodyStyle.Render(engine.FormatDuration(elapsed)))

	leftPart := pctStr + rowsStr
	rightPart := strings.Join(stats, "  ")
	gap := width - lipgloss.Width(leftPart) - lipgloss.Width(rightPart) - 2
	if gap < 2 {
		gap = 2
	}
	b.WriteString(leftPart + strings.Repeat(" ", gap) + rightPart)
	b.WriteString("\n")

	// Row 2: progress bar.
	barWidth := width - 4
	if barWidth < 20 {
		barWidth = 20
	}
	filled := int(pct * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	b.WriteString("  " + renderGradientBar(filled, barWidth))
	b.WriteString("\n")

	// Row 3: chunks info.
	chunksLeft := fmt.Sprintf("  %s chunks done", engine.FormatRows(int64(m.chunksOK)))
	chunksRight := ""
	if m.estimatedChunks > m.chunksOK {
		remaining := m.estimatedChunks - m.chunksOK
		chunksRight = fmt.Sprintf("~%s remaining (~%s total)",
			engine.FormatRows(int64(remaining)),
			engine.FormatRows(int64(m.estimatedChunks)))
	}
	leftW := lipgloss.Width(chunksLeft)
	rightW := lipgloss.Width(chunksRight)
	gap2 := width - leftW - rightW - 2
	if gap2 < 2 {
		gap2 = 2
	}
	b.WriteString(dimStyle.Render(chunksLeft) + strings.Repeat(" ", gap2) + dimStyle.Render(chunksRight))

	return b.String()
}

// ── Workers + Database side-by-side ───────────────────────────────────────

func (m Model) renderWorkersAndDB(width int) string {
	leftWidth := width*55/100 - 1
	rightWidth := width - leftWidth - 3
	if leftWidth < 30 {
		leftWidth = 30
	}
	if rightWidth < 30 {
		rightWidth = 30
	}

	left := m.renderWorkers(leftWidth)
	right := m.renderDatabase(rightWidth)

	return renderSideBySide(left, right, leftWidth, rightWidth)
}

func (m Model) renderHooksAndDB(width int) string {
	leftWidth := width*55/100 - 1
	rightWidth := width - leftWidth - 3
	if leftWidth < 30 {
		leftWidth = 30
	}
	if rightWidth < 30 {
		rightWidth = 30
	}

	left := m.renderHooks(leftWidth)
	right := m.renderDatabase(rightWidth)

	return renderSideBySide(left, right, leftWidth, rightWidth)
}

func renderSideBySide(left, right string, leftWidth, rightWidth int) string {
	leftLines := strings.Split(left, "\n")
	rightLines := strings.Split(right, "\n")

	maxLines := len(leftLines)
	if len(rightLines) > maxLines {
		maxLines = len(rightLines)
	}

	var b strings.Builder
	sep := dimStyle.Render(" │ ")
	for i := 0; i < maxLines; i++ {
		l := ""
		if i < len(leftLines) {
			l = leftLines[i]
		}
		r := ""
		if i < len(rightLines) {
			r = rightLines[i]
		}
		// Pad left column.
		padLen := leftWidth - lipgloss.Width(l)
		if padLen < 0 {
			padLen = 0
		}
		b.WriteString(l + strings.Repeat(" ", padLen) + sep + r + "\n")
	}

	return strings.TrimRight(b.String(), "\n")
}

// ── Workers panel ─────────────────────────────────────────────────────────

func (m Model) renderWorkers(width int) string {
	var b strings.Builder

	active := 0
	for _, w := range m.workers {
		if !w.idle {
			active++
		}
	}

	title := sectionStyle.Render("WORKERS")
	count := cyanStyle.Render(fmt.Sprintf("  %d active", active))
	b.WriteString(title + count + "\n")

	// Column widths.
	const (
		colWidth = 10
		colDur   = 12
	)
	idWidth := len(fmt.Sprintf("%d", len(m.workers))) + 1 // +1 for '#'

	// Header row.
	hID := dimStyle.Render("#")
	hWindow := dimStyle.Render("window")
	hWidth := dimStyle.Render("width")
	hDur := dimStyle.Render("duration")

	idPad := idWidth - lipgloss.Width(hID)
	if idPad < 0 {
		idPad = 0
	}
	numericWidth := colWidth + colDur
	windowWidth := width - numericWidth - idPad - lipgloss.Width(hID) - 4 // indent + gaps
	if windowWidth < 20 {
		windowWidth = 20
	}
	windowPad := windowWidth - lipgloss.Width(hWindow)
	if windowPad < 0 {
		windowPad = 0
	}
	b.WriteString("\n  " + hID + strings.Repeat(" ", idPad) + "  " + hWindow + strings.Repeat(" ", windowPad) +
		padLeft(hWidth, colWidth) + padLeft(hDur, colDur) + "\n\n")

	for _, w := range m.workers {
		line := m.renderWorkerLine(w, width, windowWidth, colWidth, colDur)
		b.WriteString(line + "\n")
	}

	return b.String()
}

func (m Model) renderWorkerLine(w workerDisplay, width, windowWidth, colWidth, colDur int) string {
	// Fixed-width ID: pad to widest worker number (e.g. "#1 " vs "#10").
	idW := len(fmt.Sprintf("%d", len(m.workers))) + 1 // +1 for '#'
	id := fmt.Sprintf("#%d", w.id)
	for len(id) < idW {
		id += " "
	}

	if w.idle {
		return dimStyle.Render("  " + id)
	}

	window := ""
	chunkWidth := ""
	if w.chunk != nil {
		window = fmt.Sprintf("%s → %s",
			w.chunk.Start.Format("01-02 15:04"),
			w.chunk.End.Format("15:04"))
		chunkWidth = engine.FormatDuration(w.chunk.End.Sub(w.chunk.Start))
	}

	dur := ""
	if !w.started.IsZero() {
		dur = engine.FormatDuration(time.Since(w.started))
	}

	windowStyled := purpleStyle.Render(window)
	if w.retrying {
		windowStyled = amberStyle.Render(window + "  ⟳ retry")
	}
	wPad := windowWidth - lipgloss.Width(windowStyled)
	if wPad < 0 {
		wPad = 0
	}

	widthStr := bodyStyle.Render(chunkWidth)
	durStr := m.latencyStyle(time.Since(w.started)).Render(dur)

	return "  " + dimStyle.Render(id) + "  " + windowStyled + strings.Repeat(" ", wPad) +
		padLeft(widthStr, colWidth) + padLeft(durStr, colDur)
}

// ── Hooks panel ───────────────────────────────────────────────────────────

func (m Model) renderHooks(width int) string {
	var b strings.Builder

	label := "HOOKS"
	if m.hookCurrent != "" || len(m.hookLog) > 0 {
		// Try to find a common prefix to describe the hooks.
		if len(m.hookLog) > 0 {
			first := m.hookLog[0].name
			if strings.HasPrefix(first, "drop ") {
				label = "HOOKS  dropping indexes"
			} else if strings.HasPrefix(first, "create ") {
				label = "HOOKS  creating indexes"
			}
		}
	}
	b.WriteString(sectionStyle.Render(label) + "\n")

	for _, h := range m.hookLog {
		if h.skipped {
			b.WriteString(dimStyle.Render(fmt.Sprintf("  ⊘  %s  skipped", h.name)))
		} else if h.err != nil {
			b.WriteString(redStyle.Render(fmt.Sprintf("  ✗  %s", h.name)))
			b.WriteString("  " + dimStyle.Render(engine.FormatDuration(h.duration)))
		} else {
			b.WriteString(greenStyle.Render("  ✓  "))
			b.WriteString(bodyStyle.Render(h.name))
			dur := m.latencyStyle(h.duration).Render("  " + engine.FormatDuration(h.duration))
			b.WriteString(dur)
		}
		b.WriteString("\n")
	}

	if m.hookCurrent != "" {
		hookElapsed := time.Since(m.hookCurrentStart)
		b.WriteString(purpleStyle.Render("  ◆  "))
		b.WriteString(bodyStyle.Render(m.hookCurrent))
		b.WriteString("  " + dimStyle.Render(fmt.Sprintf("running… %s", engine.FormatDuration(hookElapsed))))
		b.WriteString("\n")
	}

	// Pending hooks (show as dim circles).
	remaining := m.hookTotal - m.hookDone
	if m.hookCurrent != "" {
		remaining--
	}
	// We don't have the names of pending hooks, but we can show dots.
	// Skip this if we don't know the total or remaining is <= 0.
	_ = remaining

	return b.String()
}

// ── Database panel ────────────────────────────────────────────────────────

func (m Model) renderDatabase(width int) string {
	var b strings.Builder

	if m.dbHealth == nil {
		b.WriteString(sectionStyle.Render("DATABASE") + "  " + dimStyle.Render("waiting…") + "\n")
		return b.String()
	}

	statusStyle := greenStyle
	statusLabel := "● healthy"
	switch m.dbHealth.Overall {
	case monitor.LevelYellow:
		statusStyle = amberStyle
		statusLabel = "● elevated"
	case monitor.LevelRed:
		statusStyle = redStyle
		statusLabel = "● degraded"
	}
	b.WriteString(sectionStyle.Render("DATABASE") + "  " + statusStyle.Render(statusLabel) + "\n\n")

	// Find the widest signal name for alignment.
	nameWidth := 0
	for _, sig := range m.dbHealth.Signals {
		if len(sig.Name) > nameWidth {
			nameWidth = len(sig.Name)
		}
	}

	// Render each signal as a table row: name (left) value (right), baseline/delta below.
	for _, sig := range m.dbHealth.Signals {
		label := dimStyle.Render(fmt.Sprintf("  %-*s", nameWidth, sig.Name))
		val := signalStyle(sig.Level).Render(sig.Value)

		// Right-align value.
		gap := width - lipgloss.Width(label) - lipgloss.Width(val)
		if gap < 2 {
			gap = 2
		}
		b.WriteString(label + strings.Repeat(" ", gap) + val + "\n")

		// Sub-detail line for baseline and/or delta.
		var details []string
		if sig.Baseline != "" {
			details = append(details, "baseline "+sig.Baseline)
		}
		if sig.Delta != "" {
			details = append(details, sig.Delta)
		}
		if len(details) > 0 {
			detail := dimStyle.Render("  " + strings.Join(details, "  "))
			detailGap := width - lipgloss.Width(detail)
			if detailGap < 0 {
				detailGap = 0
			}
			b.WriteString(strings.Repeat(" ", detailGap) + detail + "\n")
		}
	}

	return b.String()
}

func signalStyle(l monitor.Level) lipgloss.Style {
	switch l {
	case monitor.LevelYellow:
		return amberStyle
	case monitor.LevelRed:
		return redStyle
	default:
		return cyanStyle
	}
}

// ── Completed table ───────────────────────────────────────────────────────

func (m Model) renderCompleted(width int, maxShow int) string {
	var b strings.Builder

	// Only show the most recent entries.
	visible := m.completed
	if len(visible) > maxShow {
		visible = visible[len(visible)-maxShow:]
	}

	totalRows := int64(0)
	totalDur := time.Duration(0)
	for _, c := range visible {
		totalRows += c.rows
		totalDur += c.duration
	}
	avgDur := time.Duration(0)
	if len(visible) > 0 {
		avgDur = totalDur / time.Duration(len(visible))
	}

	title := sectionStyle.Render("COMPLETED")
	summary := dimStyle.Render(fmt.Sprintf("  %d chunks · %s rows · avg %s",
		len(visible), engine.FormatRows(totalRows), engine.FormatDuration(avgDur)))
	b.WriteString(title + summary + "\n")

	// Column widths matching workers style.
	idWidth := len(fmt.Sprintf("%d", len(m.workers))) + 1 // +1 for '#'
	const (
		colWidth = 10
		colRows  = 12
		colDur   = 12
		colRPS   = 10
	)
	numericWidth := colWidth + colRows + colDur + colRPS
	windowWidth := width - numericWidth - idWidth - 4 // indent + gaps
	if windowWidth < 20 {
		windowWidth = 20
	}

	// Table header.
	hID := dimStyle.Render("#")
	hWindow := dimStyle.Render("window")
	hWidth := dimStyle.Render("width")
	hRows := dimStyle.Render("rows")
	hDur := dimStyle.Render("duration")
	hRPS := dimStyle.Render("rows/s")
	idPad := idWidth - lipgloss.Width(hID)
	if idPad < 0 {
		idPad = 0
	}
	windowPad := windowWidth - lipgloss.Width(hWindow)
	if windowPad < 0 {
		windowPad = 0
	}
	b.WriteString("\n  " + hID + strings.Repeat(" ", idPad) + "  " + hWindow + strings.Repeat(" ", windowPad) +
		padLeft(hWidth, colWidth) + padLeft(hRows, colRows) + padLeft(hDur, colDur) + padLeft(hRPS, colRPS) + "\n\n")

	// Rows (newest first).
	for i := len(visible) - 1; i >= 0; i-- {
		c := visible[i]

		// ID styled like workers section.
		id := fmt.Sprintf("#%d", c.workerID)
		for len(id) < idWidth {
			id += " "
		}

		// Window styled like workers section.
		window := purpleStyle.Render(fmt.Sprintf("%s → %s",
			c.chunk.Start.Format("01-02 15:04"),
			c.chunk.End.Format("15:04")))

		chunkWidth := engine.FormatDuration(c.chunk.End.Sub(c.chunk.Start))
		widthStr := bodyStyle.Render(chunkWidth)

		rows := bodyStyle.Render(engine.FormatRows(c.rows))
		dur := m.latencyStyle(c.duration).Render(engine.FormatDuration(c.duration))
		rps := int64(0)
		if c.duration.Seconds() > 0 {
			rps = int64(float64(c.rows) / c.duration.Seconds())
		}
		rpsStr := dimStyle.Render(engine.FormatRows(rps))

		wPad := windowWidth - lipgloss.Width(window)
		if wPad < 0 {
			wPad = 0
		}
		b.WriteString("  " + dimStyle.Render(id) + "  " + window + strings.Repeat(" ", wPad) +
			padLeft(widthStr, colWidth) + padLeft(rows, colRows) + padLeft(dur, colDur) + padLeft(rpsStr, colRPS) + "\n")
	}

	return b.String()
}

// ── Errors table ──────────────────────────────────────────────────────────

func (m Model) renderErrors(width int) string {
	var b strings.Builder

	visible := m.failed
	if len(visible) > 5 {
		visible = visible[len(visible)-5:]
	}

	title := redStyle.Bold(true).Render("ERRORS")
	summary := dimStyle.Render(fmt.Sprintf("  %d total failures", m.chunksFailed))
	b.WriteString(title + summary + "\n\n")

	for i := len(visible) - 1; i >= 0; i-- {
		f := visible[i]

		window := fmt.Sprintf("%s → %s",
			f.chunk.Start.Format("01-02 15:04"),
			f.chunk.End.Format("15:04"))

		status := redStyle.Render("✗ failed")
		if f.retrying {
			status = amberStyle.Render("⟳ retrying")
		}

		// Truncate error message to fit width.
		maxErrLen := width - 6
		if maxErrLen < 20 {
			maxErrLen = 20
		}
		errMsg := f.err
		if len(errMsg) > maxErrLen {
			errMsg = errMsg[:maxErrLen-3] + "..."
		}

		b.WriteString("  " + dimStyle.Render(fmt.Sprintf("#%d", f.workerID)) + "  ")
		b.WriteString(purpleStyle.Render(window) + "  ")
		b.WriteString(status + "\n")
		b.WriteString("      " + redStyle.Render(errMsg) + "\n")
	}

	return b.String()
}

// ── Helpers ───────────────────────────────────────────────────────────────

func (m Model) progressPercent() float64 {
	if m.estimatedChunks <= 0 {
		return 0
	}
	pct := float64(m.chunksOK) / float64(m.estimatedChunks)
	if pct > 1 {
		pct = 1
	}
	return pct
}

// latencyStyle returns a color style based on the adaptive target runtime.
// At or below target: cyan, up to 2× target: amber, above 2× target: red.
func (m Model) latencyStyle(d time.Duration) lipgloss.Style {
	target := m.targetRuntime
	if target == 0 {
		target = 30 * time.Second
	}
	switch {
	case d > 2*target:
		return redStyle
	case d > target:
		return amberStyle
	default:
		return cyanStyle
	}
}

// renderGradientBar renders a progress bar with a cyan-to-purple gradient fill.
func renderGradientBar(filled, total int) string {
	var b strings.Builder
	for i := 0; i < filled; i++ {
		// Interpolate from cyan (#00d4ff) to purple (#8b5cf6).
		t := 0.0
		if filled > 1 {
			t = float64(i) / float64(filled-1)
		}
		r := int(0x00 + t*float64(0x8b-0x00))
		g := int(0xd4 + t*float64(0x5c-0xd4))
		bl := int(0xff + t*float64(0xf6-0xff))
		color := lipgloss.Color(fmt.Sprintf("#%02x%02x%02x", r, g, bl))
		b.WriteString(lipgloss.NewStyle().Foreground(color).Render("█"))
	}
	if total > filled {
		b.WriteString(dimStyle.Render(strings.Repeat("░", total-filled)))
	}
	return b.String()
}

// padLeft right-aligns a styled string within a fixed visual width.
func padLeft(s string, width int) string {
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	return strings.Repeat(" ", width-w) + s
}
