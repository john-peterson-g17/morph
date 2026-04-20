package cli

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v3"

	"github.com/john-peterson-g17/morph/internal/cli/flags"
	"github.com/john-peterson-g17/morph/internal/engine"
	"github.com/john-peterson-g17/morph/internal/job"
	"github.com/john-peterson-g17/morph/internal/previewer"
	pgprev "github.com/john-peterson-g17/morph/internal/previewer/postgres"
)

// ── Preview styles (matching run TUI palette) ─────────────────────────────

var (
	pvMuted  = lipgloss.Color("#4a5280")
	pvBody   = lipgloss.Color("#b8c4d8")
	pvCyan   = lipgloss.Color("#00d4ff")
	pvPurple = lipgloss.Color("#8b5cf6")
	pvAmber  = lipgloss.Color("#e0af68")
	pvRed    = lipgloss.Color("#f87171")
	pvGreen  = lipgloss.Color("#4ade80")

	pvBold    = lipgloss.NewStyle().Bold(true).Foreground(pvBody)
	pvDim     = lipgloss.NewStyle().Foreground(pvMuted)
	pvBodySt  = lipgloss.NewStyle().Foreground(pvBody)
	pvCyanSt  = lipgloss.NewStyle().Foreground(pvCyan)
	pvPurpSt  = lipgloss.NewStyle().Foreground(pvPurple)
	pvAmberSt = lipgloss.NewStyle().Foreground(pvAmber)
	pvRedSt   = lipgloss.NewStyle().Foreground(pvRed)
	pvGreenSt = lipgloss.NewStyle().Foreground(pvGreen)
	pvSection = lipgloss.NewStyle().Bold(true).Foreground(pvBody)
)

// PreviewCommand returns the preview CLI command.
func PreviewCommand() *cli.Command {
	return &cli.Command{
		Name:      "preview",
		Usage:     "Preview the execution plan for a morph job without running it",
		UsageText: "morph preview <job name>",
		Flags: []cli.Flag{
			flags.JobDirFlag(),
			flags.DSNFlag(),
		},
		Action: runPreview,
	}
}

func runPreview(ctx context.Context, cmd *cli.Command) error {
	jobFile, err := flags.ResolveJobFile(cmd)
	if err != nil {
		return err
	}

	cfg, err := job.Load(jobFile)
	if err != nil {
		return fmt.Errorf("loading job: %w", err)
	}

	// Parse durations.
	initialWidth, _ := job.ParseDuration(cfg.Partitioning.Adaptive.InitialWidth)
	if initialWidth == 0 {
		initialWidth = 1 * 60 * 60 * 1e9
	}
	minWidth, _ := job.ParseDuration(cfg.Partitioning.Adaptive.MinWidth)
	if minWidth == 0 {
		minWidth = 60 * 1e9
	}
	maxWidth, _ := job.ParseDuration(cfg.Partitioning.Adaptive.MaxWidth)
	if maxWidth == 0 {
		maxWidth = 24 * 60 * 60 * 1e9
	}
	targetRuntime, _ := job.ParseDuration(cfg.Partitioning.Adaptive.TargetRuntime)
	if targetRuntime == 0 {
		targetRuntime = 30 * 1e9
	}

	concurrency := cfg.Runtime.Defaults.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	totalWindow := cfg.Partitioning.Window.End.Sub(cfg.Partitioning.Window.Start)
	estimatedChunks := int(totalWindow / initialWidth)
	if estimatedChunks < 1 {
		estimatedChunks = 1
	}

	var b strings.Builder
	width := 80

	divider := pvDim.Render(strings.Repeat("─", width))
	sep := pvDim.Render("  │  ")

	// ── Header (matching run TUI) ───────────────────────────────────
	bolt := pvCyanSt.Bold(true).Render("⚡")
	brand := pvCyanSt.Bold(true).Render("morph preview")

	var parts []string
	parts = append(parts, bolt+" "+brand)
	parts = append(parts, pvBold.Render(cfg.Job.Name))
	if cfg.Version != "" {
		parts = append(parts, pvPurpSt.Render(cfg.Version))
	}
	parts = append(parts, pvDim.Render("driver ")+pvBodySt.Render(cfg.Database.Driver))

	b.WriteString("\n")
	b.WriteString(strings.Join(parts, sep))
	b.WriteString("\n")
	if cfg.Job.Description != "" {
		b.WriteString(pvDim.Render("  "+cfg.Job.Description) + "\n")
	}
	b.WriteString("\n")
	b.WriteString(divider + "\n")

	// ── Partitioning ────────────────────────────────────────────────
	b.WriteString(pvSection.Render("PARTITIONING") + "\n\n")
	pvRow(&b, "Strategy", cfg.Partitioning.Strategy)
	pvRow(&b, "Window", fmt.Sprintf("%s → %s",
		pvPurpSt.Render(cfg.Partitioning.Window.Start.Format("2006-01-02 15:04")),
		pvPurpSt.Render(cfg.Partitioning.Window.End.Format("2006-01-02 15:04"))))
	pvRow(&b, "Span", engine.FormatDuration(totalWindow))
	pvRow(&b, "Initial Width", engine.FormatDuration(initialWidth))
	pvRow(&b, "Min / Max", engine.FormatDuration(minWidth)+" / "+engine.FormatDuration(maxWidth))
	pvRow(&b, "Target Runtime", engine.FormatDuration(targetRuntime))
	pvRow(&b, "Est. Chunks", "~"+engine.FormatRows(int64(estimatedChunks)))
	b.WriteString("\n" + divider + "\n")

	// ── Runtime ─────────────────────────────────────────────────────
	b.WriteString(pvSection.Render("RUNTIME") + "\n\n")
	pvRow(&b, "Concurrency", fmt.Sprintf("%d workers", concurrency))
	pvRow(&b, "Max Retries", fmt.Sprintf("%d", cfg.Runtime.Defaults.MaxRetries))
	if cfg.Runtime.Defaults.MaxRows > 0 {
		pvRow(&b, "Max Rows", engine.FormatRows(int64(cfg.Runtime.Defaults.MaxRows)))
	} else {
		pvRow(&b, "Max Rows", pvDim.Render("unlimited"))
	}
	if cfg.Runtime.Defaults.StatementTimeout != "" {
		pvRow(&b, "Stmt Timeout", cfg.Runtime.Defaults.StatementTimeout)
	}
	b.WriteString("\n" + divider + "\n")

	// ── Steps ───────────────────────────────────────────────────────
	b.WriteString(pvSection.Render(fmt.Sprintf("STEPS  %s", pvDim.Render(fmt.Sprintf("%d total", len(cfg.Steps))))) + "\n")

	for i, step := range cfg.Steps {
		b.WriteString("\n")
		b.WriteString("  " + pvCyanSt.Bold(true).Render(fmt.Sprintf("[%d]", i+1)) + "  " + pvBold.Render(step.Name) + "\n")

		if len(step.Before) > 0 {
			b.WriteString("      " + pvAmberSt.Render(fmt.Sprintf("before hooks (%d)", len(step.Before))) + "\n")
			for _, hook := range step.Before {
				label := hook.Name
				if label == "" {
					label = "(unnamed)"
				}
				b.WriteString("        " + pvDim.Render("•") + " " + pvBodySt.Render(label) + "\n")
				b.WriteString("          " + pvDim.Render(oneline(hook.SQL)) + "\n")
			}
		}

		if step.Morph.PartitionBy != "" {
			b.WriteString("      " + pvDim.Render("partition by: ") + pvBodySt.Render(step.Morph.PartitionBy) + "\n")
		}

		b.WriteString("      " + pvDim.Render("query:") + "\n")
		composed := step.ComposeSQL()
		for _, line := range strings.Split(composed, "\n") {
			b.WriteString("        " + pvDim.Render(line) + "\n")
		}

		if len(step.After) > 0 {
			b.WriteString("      " + pvGreenSt.Render(fmt.Sprintf("after hooks (%d)", len(step.After))) + "\n")
			for _, hook := range step.After {
				label := hook.Name
				if label == "" {
					label = "(unnamed)"
				}
				b.WriteString("        " + pvDim.Render("•") + " " + pvBodySt.Render(label) + "\n")
				b.WriteString("          " + pvDim.Render(oneline(hook.SQL)) + "\n")
			}
		}
	}

	b.WriteString("\n" + divider + "\n")

	// ── EXPLAIN & Row Estimates (postgres only, requires DSN) ───────
	dsn, _ := flags.ResolveDSN(cmd)
	if dsn != "" {
		db, err := sql.Open(cfg.Database.Driver, dsn)
		if err == nil {
			defer func() { _ = db.Close() }()
			if db.PingContext(ctx) == nil {
				var p previewer.Previewer
				switch cfg.Database.Driver {
				case "postgres":
					p = pgprev.New(db)
				}
				if p != nil {
					steps := make([]previewer.StepQuery, len(cfg.Steps))
					for i, step := range cfg.Steps {
						steps[i] = previewer.StepQuery{
							Name: step.Name,
							SQL:  step.ComposeSQL(),
						}
					}

					// Row estimates (full window).
					estimates := p.EstimateRows(ctx, steps,
						cfg.Partitioning.Window.Start,
						cfg.Partitioning.Window.End,
					)

					b.WriteString(pvSection.Render("ROW ESTIMATES") + pvDim.Render("  full window via EXPLAIN") + "\n\n")
					for _, est := range estimates {
						if est.Err != nil {
							b.WriteString("  " + pvRedSt.Render("✗") + "  " + pvBodySt.Render(est.StepName) + "  " + pvRedSt.Render(est.Err.Error()) + "\n")
						} else if est.Rows > 0 {
							b.WriteString("  " + pvGreenSt.Render("✓") + "  " + pvBodySt.Render(est.StepName) + "  ~" + pvCyanSt.Render(engine.FormatRows(est.Rows)) + pvDim.Render(" rows (estimated)") + "\n")
						} else {
							b.WriteString("  " + pvAmberSt.Render("?") + "  " + pvBodySt.Render(est.StepName) + "  " + pvDim.Render("unable to estimate") + "\n")
						}
					}
					b.WriteString("\n" + divider + "\n")

					// EXPLAIN a sample chunk (one initialWidth from window start).
					// This shows the plan the planner will actually choose at runtime.
					sampleEnd := cfg.Partitioning.Window.Start.Add(initialWidth)
					if sampleEnd.After(cfg.Partitioning.Window.End) {
						sampleEnd = cfg.Partitioning.Window.End
					}

					explains := p.ExplainQuery(ctx, steps,
						cfg.Partitioning.Window.Start,
						sampleEnd,
					)

					b.WriteString(pvSection.Render("QUERY PLAN") + pvDim.Render(fmt.Sprintf("  sample chunk %s → %s",
						cfg.Partitioning.Window.Start.Format("2006-01-02 15:04"),
						sampleEnd.Format("2006-01-02 15:04"))) + "\n")
					for _, ex := range explains {
						b.WriteString("\n  " + pvCyanSt.Bold(true).Render(ex.StepName) + "\n")
						if ex.Err != nil {
							b.WriteString("  " + pvRedSt.Render(ex.Err.Error()) + "\n")
						} else {
							for _, line := range ex.Lines {
								b.WriteString("  " + pvDim.Render(line) + "\n")
							}
						}
					}
				}
			} else {
				b.WriteString(pvAmberSt.Render("  ⚠ Could not connect to database — skipping EXPLAIN") + "\n")
			}
		}
	} else {
		b.WriteString(pvDim.Render("  Provide --dsn to see EXPLAIN output and row estimates") + "\n")
	}

	b.WriteString("\n")

	fmt.Print(b.String())
	return nil
}

func pvRow(b *strings.Builder, label, value string) {
	padded := fmt.Sprintf("%-16s", label)
	b.WriteString("  " + pvDim.Render(padded) + "  " + pvBodySt.Render(value) + "\n")
}

func oneline(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}
