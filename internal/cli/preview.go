package cli

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/urfave/cli/v3"

	"github.com/john-peterson-g17/morph/internal/cli/flags"
	"github.com/john-peterson-g17/morph/internal/engine"
	"github.com/john-peterson-g17/morph/internal/job"
	"github.com/john-peterson-g17/morph/internal/previewer"
	pgprev "github.com/john-peterson-g17/morph/internal/previewer/postgres"
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

	// Header.
	fmt.Println("Preview")
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  Job:            %s\n", cfg.Job.Name)
	if cfg.Job.Description != "" {
		fmt.Printf("  Description:    %s\n", cfg.Job.Description)
	}
	fmt.Printf("  Driver:         %s\n", cfg.Database.Driver)
	fmt.Println()

	// Partitioning.
	fmt.Println("Partitioning")
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  Strategy:       %s\n", cfg.Partitioning.Strategy)
	fmt.Printf("  Window:         %s → %s\n",
		cfg.Partitioning.Window.Start.Format("2006-01-02 15:04"),
		cfg.Partitioning.Window.End.Format("2006-01-02 15:04"))
	fmt.Printf("  Window Span:    %s\n", engine.FormatDuration(totalWindow))
	fmt.Printf("  Initial Width:  %s\n", engine.FormatDuration(initialWidth))
	fmt.Printf("  Min Width:      %s\n", engine.FormatDuration(minWidth))
	fmt.Printf("  Max Width:      %s\n", engine.FormatDuration(maxWidth))
	fmt.Printf("  Target Runtime: %s\n", engine.FormatDuration(targetRuntime))
	fmt.Printf("  Est. Chunks:    ~%s (at initial width)\n", engine.FormatRows(int64(estimatedChunks)))
	fmt.Println()

	// Runtime.
	fmt.Println("Runtime")
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  Concurrency:    %d workers\n", concurrency)
	fmt.Printf("  Max Retries:    %d\n", cfg.Runtime.Defaults.MaxRetries)
	if cfg.Runtime.Defaults.MaxRows > 0 {
		fmt.Printf("  Max Rows:       %s\n", engine.FormatRows(int64(cfg.Runtime.Defaults.MaxRows)))
	} else {
		fmt.Printf("  Max Rows:       unlimited\n")
	}
	if cfg.Runtime.Defaults.StatementTimeout != "" {
		fmt.Printf("  Stmt Timeout:   %s\n", cfg.Runtime.Defaults.StatementTimeout)
	}
	fmt.Println()

	// Steps.
	fmt.Printf("Steps (%d)\n", len(cfg.Steps))
	fmt.Println(strings.Repeat("─", 60))
	for i, step := range cfg.Steps {
		fmt.Printf("\n  [%d] %s\n", i+1, step.Name)

		if len(step.Before) > 0 {
			fmt.Printf("      Before hooks (%d):\n", len(step.Before))
			for _, hook := range step.Before {
				label := hook.Name
				if label == "" {
					label = "(unnamed)"
				}
				fmt.Printf("        • %s\n", label)
				fmt.Printf("          %s\n", oneline(hook.SQL))
			}
		}

		if step.Morph.PartitionBy != "" {
			fmt.Printf("      Partition by: %s\n", step.Morph.PartitionBy)
		}
		fmt.Printf("      Query:\n")
		composed := step.ComposeSQL()
		for _, line := range strings.Split(composed, "\n") {
			fmt.Printf("        %s\n", line)
		}

		if len(step.After) > 0 {
			fmt.Printf("      After hooks (%d):\n", len(step.After))
			for _, hook := range step.After {
				label := hook.Name
				if label == "" {
					label = "(unnamed)"
				}
				fmt.Printf("        • %s\n", label)
				fmt.Printf("          %s\n", oneline(hook.SQL))
			}
		}
	}

	// Row estimate — only if DB is reachable.
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
					estimates := p.EstimateRows(ctx, steps,
						cfg.Partitioning.Window.Start,
						cfg.Partitioning.Window.End,
					)

					fmt.Println()
					fmt.Println("Row Estimates (via EXPLAIN)")
					fmt.Println(strings.Repeat("─", 60))
					for _, est := range estimates {
						if est.Err != nil {
							fmt.Printf("  %-20s  error: %v\n", est.StepName, est.Err)
						} else if est.Rows > 0 {
							fmt.Printf("  %-20s  ~%s rows (estimated)\n", est.StepName, engine.FormatRows(est.Rows))
						} else {
							fmt.Printf("  %-20s  unable to estimate\n", est.StepName)
						}
					}
				}
			}
		}
	}

	fmt.Println()
	return nil
}

func oneline(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}
