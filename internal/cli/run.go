package cli

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/urfave/cli/v3"

	"github.com/john-peterson-g17/morph/internal/cli/flags"
	"github.com/john-peterson-g17/morph/internal/engine"
	"github.com/john-peterson-g17/morph/internal/job"
	"github.com/john-peterson-g17/morph/internal/monitor"
	pgmon "github.com/john-peterson-g17/morph/internal/monitor/postgres"
	"github.com/john-peterson-g17/morph/internal/tui"
)

// RunCommand returns the run CLI command.
func RunCommand() *cli.Command {
	return &cli.Command{
		Name:      "run",
		Usage:     "Execute a morph job",
		UsageText: "morph run <job name>",
		Flags: []cli.Flag{
			flags.JobDirFlag(),
			flags.ProgressDirFlag(),
			flags.DSNFlag(),
			&cli.IntFlag{
				Name:  "concurrency",
				Usage: "Number of parallel chunk workers",
				Value: 1,
			},
			&cli.BoolFlag{
				Name:  "fresh",
				Usage: "Discard previous progress and start from scratch",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Show executed SQL queries in the output",
			},
		},
		Action: runBackfill,
	}
}

func runBackfill(ctx context.Context, cmd *cli.Command) error {
	jobFile, err := flags.ResolveJobFile(cmd)
	if err != nil {
		return err
	}

	cfg, err := job.Load(jobFile)
	if err != nil {
		return fmt.Errorf("loading job: %w", err)
	}

	progressFile, err := flags.ResolveProgressFile(cmd, cfg.Version)
	if err != nil {
		return err
	}

	concurrency := int(cmd.Int("concurrency"))
	if cfg.Runtime.Defaults.Concurrency > 0 && concurrency <= 1 {
		concurrency = cfg.Runtime.Defaults.Concurrency
	}

	// Parse adaptive partitioning durations from config.
	initialWidth, err := job.ParseDuration(cfg.Partitioning.Adaptive.InitialWidth)
	if err != nil {
		return fmt.Errorf("parsing initial_width: %w", err)
	}
	if initialWidth == 0 {
		initialWidth = 1 * 60 * 60 * 1e9 // 1h default
	}
	targetRuntime, err := job.ParseDuration(cfg.Partitioning.Adaptive.TargetRuntime)
	if err != nil {
		return fmt.Errorf("parsing target_runtime: %w", err)
	}
	if targetRuntime == 0 {
		targetRuntime = 30 * 1e9 // 30s default
	}
	minWidth, err := job.ParseDuration(cfg.Partitioning.Adaptive.MinWidth)
	if err != nil {
		return fmt.Errorf("parsing min_width: %w", err)
	}
	if minWidth == 0 {
		minWidth = 60 * 1e9 // 1m default
	}
	maxWidth, err := job.ParseDuration(cfg.Partitioning.Adaptive.MaxWidth)
	if err != nil {
		return fmt.Errorf("parsing max_width: %w", err)
	}
	if maxWidth == 0 {
		maxWidth = 24 * 60 * 60 * 1e9 // 24h default
	}

	// Connect to database.
	dsn, err := flags.ResolveDSN(cmd, cfg.Connection.DSNEnv)
	if err != nil {
		return err
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	// Set up progress store.
	progress, err := engine.LoadProgressStore(progressFile)
	if err != nil {
		return fmt.Errorf("loading progress: %w", err)
	}

	if cmd.Bool("fresh") && progress.HasData() {
		if err := progress.Reset(); err != nil {
			return fmt.Errorf("resetting progress: %w", err)
		}
		progress, err = engine.LoadProgressStore(progressFile)
		if err != nil {
			return fmt.Errorf("reloading progress: %w", err)
		}
	}

	// Set up chunk planner.
	planner := engine.NewChunkPlanner(
		cfg.Partitioning.Window.Start,
		cfg.Partitioning.Window.End,
		initialWidth,
		targetRuntime,
		minWidth,
		maxWidth,
	)

	// Resume from last progress if available.
	if progress.HasData() {
		nextStart, lastWidth, completed := progress.GetResumePoint()
		planner.ResumeFrom(nextStart, lastWidth)
		fmt.Printf("Resuming from %s (%d chunks already completed)\n",
			nextStart.Format("2006-01-02 15:04"), completed)
	}

	progress.Init(cfg.Job.Name, cfg.Version, cfg.Partitioning.Window.Start, cfg.Partitioning.Window.End)

	// Print plan summary.
	fmt.Printf("Job:         %s\n", cfg.Job.Name)
	fmt.Printf("Window:      %s → %s\n",
		cfg.Partitioning.Window.Start.Format("2006-01-02 15:04"),
		cfg.Partitioning.Window.End.Format("2006-01-02 15:04"))
	fmt.Printf("Steps:       %d (%s)\n", len(cfg.Steps), joinStepNames(cfg))
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Println()

	// Set up cancellation context for TUI.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start TUI.
	debug := cmd.Bool("debug")
	program, err := tui.Run(cfg.Job.Name, concurrency, cancel, debug)
	if err != nil {
		return fmt.Errorf("starting TUI: %w", err)
	}

	// Start database health monitor.
	switch cfg.Connection.Driver {
	case "postgres":
		mon := pgmon.New(db)
		if err := mon.Init(runCtx); err == nil {
			monitor.RunCollector(runCtx, mon, program, 2*time.Second)
		}
	default:
		fmt.Printf("Warning: no monitor available for driver %q, skipping DB health\n", cfg.Connection.Driver)
	}

	// Build and run worker pool.
	maxRetries := cfg.Runtime.Defaults.MaxRetries
	maxRows := int64(cfg.Runtime.Defaults.MaxRows)

	// Execute before hooks for all steps.
	for _, step := range cfg.Steps {
		for _, hook := range step.Before {
			label := hook.Name
			if label == "" {
				label = step.Name + " (before)"
			}
			fmt.Printf("Running: %s\n", label)
			if _, err := db.ExecContext(runCtx, hook.SQL); err != nil {
				return fmt.Errorf("before hook %q: %w", label, err)
			}
		}
	}

	pool := engine.NewWorkerPool(db, planner, progress, cfg.Steps, concurrency, maxRetries, maxRows, program)
	poolErr := pool.Run(runCtx)

	// Execute after hooks for all steps (only if job succeeded).
	if poolErr == nil {
		for _, step := range cfg.Steps {
			for _, hook := range step.After {
				label := hook.Name
				if label == "" {
					label = step.Name + " (after)"
				}
				fmt.Printf("Running: %s\n", label)
				if _, err := db.ExecContext(runCtx, hook.SQL); err != nil {
					return fmt.Errorf("after hook %q: %w", label, err)
				}
			}
		}
	}

	// Signal TUI to exit.
	program.Send(engine.MsgJobDone{Err: poolErr})
	program.Wait()

	// Print final summary.
	completedChunks, failedChunks, rowsByStep, avgRuntime := progress.Summary()
	fmt.Println()
	if poolErr != nil {
		if engine.CheckCancelled(runCtx) != nil {
			return nil
		}
		fmt.Printf("✗ Job failed: %v\n", poolErr)
	} else {
		fmt.Printf("✓ Job complete\n")
	}
	fmt.Printf("  Chunks: %d completed, %d failed\n", completedChunks, failedChunks)
	fmt.Printf("  Avg chunk runtime: %s\n", engine.FormatDuration(avgRuntime))
	for step, rows := range rowsByStep {
		fmt.Printf("  %s: %s rows\n", step, engine.FormatRows(rows))
	}

	return poolErr
}

func joinStepNames(cfg *job.Config) string {
	names := cfg.StepNames()
	result := ""
	for i, n := range names {
		if i > 0 {
			result += ", "
		}
		result += n
	}
	return result
}
