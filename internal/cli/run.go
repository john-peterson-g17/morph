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
	"github.com/john-peterson-g17/morph/internal/progress"
	progressfile "github.com/john-peterson-g17/morph/internal/progress/file"
	progresspg "github.com/john-peterson-g17/morph/internal/progress/postgres"
	progresss3 "github.com/john-peterson-g17/morph/internal/progress/s3"
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
			flags.ProgressDriverFlag(),
			flags.ProgressSchemaFlag(),
			flags.ProgressBucketFlag(),
			flags.ProgressPrefixFlag(),
			flags.ProgressEndpointFlag(),
			flags.ProgressRegionFlag(),
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
			&cli.BoolFlag{
				Name:  "skip-failed",
				Usage: "Skip previously failed chunks on resume instead of retrying them",
			},
			&cli.TimestampFlag{
				Name:   "start",
				Usage:  "Override window start (e.g. 2024-06-01T00:00:00Z)",
				Config: cli.TimestampConfig{Layouts: []string{time.RFC3339, "2006-01-02"}},
			},
			&cli.TimestampFlag{
				Name:   "end",
				Usage:  "Override window end (e.g. 2025-01-01T00:00:00Z)",
				Config: cli.TimestampConfig{Layouts: []string{time.RFC3339, "2006-01-02"}},
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

	if t := cmd.Timestamp("start"); !t.IsZero() {
		cfg.Partitioning.Window.Start = t
	}
	if t := cmd.Timestamp("end"); !t.IsZero() {
		cfg.Partitioning.Window.End = t
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
	dsn, err := flags.ResolveDSN(cmd)
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

	// Resolve progress store driver: CLI flag > config YAML > default "file".
	progressDriver := cmd.String("progress-driver")
	if progressDriver == "" {
		progressDriver = cfg.Progress.Driver
	}
	if progressDriver == "" {
		progressDriver = "file"
	}

	// Build progress store directly based on driver.
	var store progress.Store
	switch progressDriver {
	case "file":
		progressFile, err := flags.ResolveProgressFile(cmd, cfg.Version)
		if err != nil {
			return err
		}
		path := progressFile
		if cfg.Progress.File.Path != "" && cmd.String("progress-dir") == "" {
			path = cfg.Progress.File.Path
		}
		store, err = progressfile.New(path)
		if err != nil {
			return fmt.Errorf("loading progress: %w", err)
		}
	case "postgres":
		schema := cfg.Progress.Postgres.Schema
		table := cfg.Progress.Postgres.Table
		if v := cmd.String("progress-schema"); v != "" {
			schema = v
		}
		var err error
		store, err = progresspg.New(db, schema, table, cfg.Job.Name, cfg.Version)
		if err != nil {
			return fmt.Errorf("loading progress: %w", err)
		}
	case "s3":
		bucket := cfg.Progress.S3.Bucket
		prefix := cfg.Progress.S3.Prefix
		region := cfg.Progress.S3.Region
		endpoint := cfg.Progress.S3.Endpoint
		if v := cmd.String("progress-bucket"); v != "" {
			bucket = v
		}
		if v := cmd.String("progress-prefix"); v != "" {
			prefix = v
		}
		if v := cmd.String("progress-endpoint"); v != "" {
			endpoint = v
		}
		if v := cmd.String("progress-region"); v != "" {
			region = v
		}
		var err error
		store, err = progresss3.New(endpoint, bucket, prefix, region, cfg.Job.Name, cfg.Version)
		if err != nil {
			return fmt.Errorf("loading progress: %w", err)
		}
	default:
		return fmt.Errorf("unknown progress driver: %q", progressDriver)
	}

	if cmd.Bool("fresh") && store.HasData() {
		if err := store.Reset(); err != nil {
			return fmt.Errorf("resetting progress: %w", err)
		}
	}

	// Detect already-completed job and prompt user.
	if store.HasData() && !cmd.Bool("fresh") {
		nextStart, _, _ := store.GetResumePoint()
		if !nextStart.Before(cfg.Partitioning.Window.End) {
			fmt.Println("This job has already been completed.")
			fmt.Print("Would you like to delete progress and start fresh? [y/N] ")
			var answer string
			_, _ = fmt.Scanln(&answer)
			if answer == "y" || answer == "Y" || answer == "yes" || answer == "Yes" {
				if err := store.Reset(); err != nil {
					return fmt.Errorf("resetting progress: %w", err)
				}
				fmt.Println("Progress cleared. Starting fresh...")
			} else {
				fmt.Println("Exiting. Use --fresh to skip this prompt.")
				return nil
			}
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
	isResume := false
	var resumedChunks int
	var resumedRows int64
	if store.HasData() {
		isResume = true
		nextStart, lastWidth, completed := store.GetResumePoint()
		planner.ResumeFrom(nextStart, lastWidth)
		resumedChunks = completed
		resumedRows = store.TotalRows()
	}

	store.Init(cfg.Job.Name, cfg.Version, cfg.Partitioning.Window.Start, cfg.Partitioning.Window.End)

	// Set up cancellation context for TUI.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Count before/after hooks for tab display.
	var beforeHooks []struct {
		label string
		sql   string
		run   string
	}
	for _, step := range cfg.Steps {
		for _, hook := range step.Before {
			label := hook.Name
			if label == "" {
				label = step.Name + " (before)"
			}
			beforeHooks = append(beforeHooks, struct {
				label string
				sql   string
				run   string
			}{label, hook.SQL, hook.Run})
		}
	}
	var afterHooks []struct {
		label string
		sql   string
		run   string
	}
	for _, step := range cfg.Steps {
		for _, hook := range step.After {
			label := hook.Name
			if label == "" {
				label = step.Name + " (after)"
			}
			afterHooks = append(afterHooks, struct {
				label string
				sql   string
				run   string
			}{label, hook.SQL, hook.Run})
		}
	}

	// Build width label for header.
	widthLabel := engine.FormatDuration(initialWidth)
	if cfg.Partitioning.Strategy == "adaptive" || cfg.Partitioning.Adaptive.TargetRuntime != "" {
		widthLabel += " adaptive"
	}

	// Compute initial estimated total chunks for the TUI progress bar.
	initialEstimate := resumedChunks + planner.EstimatedTotalChunks(0)

	// Start TUI.
	debug := cmd.Bool("debug")
	program, err := tui.Run(tui.RunOpts{
		JobName:         cfg.Job.Name,
		Version:         cfg.Version,
		Concurrency:     concurrency,
		WidthLabel:      widthLabel,
		StepName:        joinStepNames(cfg),
		BeforeTotal:     len(beforeHooks),
		AfterTotal:      len(afterHooks),
		ResumedChunks:   resumedChunks,
		ResumedRows:     resumedRows,
		EstimatedChunks: initialEstimate,
		Cancel:          cancel,
		Debug:           debug,
	})
	if err != nil {
		return fmt.Errorf("starting TUI: %w", err)
	}

	// Start database health monitor.
	switch cfg.Database.Driver {
	case "postgres":
		mon := pgmon.New(db)
		if err := mon.Init(runCtx); err == nil {
			monitor.RunCollector(runCtx, mon, program, 2*time.Second)
		}
	default:
		fmt.Printf("Warning: no monitor available for driver %q, skipping DB health\n", cfg.Database.Driver)
	}

	// Build and run worker pool.
	maxRetries := cfg.Runtime.Defaults.MaxRetries
	maxRows := int64(cfg.Runtime.Defaults.MaxRows)

	// Execute before hooks for all steps.
	for i, hook := range beforeHooks {
		if runCtx.Err() != nil {
			break
		}

		// Skip "once" hooks on resume (empty defaults to "once").
		skipHook := isResume && hook.run != "always"

		program.Send(engine.MsgHookStart{
			Phase: "before",
			Name:  hook.label,
			Index: i,
			Total: len(beforeHooks),
		})

		if skipHook {
			program.Send(engine.MsgHookDone{
				Phase:   "before",
				Name:    hook.label,
				Index:   i,
				Total:   len(beforeHooks),
				Skipped: true,
			})
			continue
		}

		start := time.Now()
		_, execErr := db.ExecContext(runCtx, hook.sql)
		program.Send(engine.MsgHookDone{
			Phase:    "before",
			Name:     hook.label,
			Index:    i,
			Total:    len(beforeHooks),
			Duration: time.Since(start),
			Err:      execErr,
		})
		if execErr != nil {
			if runCtx.Err() != nil {
				break
			}
			program.Send(engine.MsgJobDone{Err: fmt.Errorf("before hook %q: %w", hook.label, execErr)})
			program.Wait()
			return fmt.Errorf("before hook %q: %w", hook.label, execErr)
		}
	}

	pool := engine.NewWorkerPool(db, planner, store, cfg.Steps, concurrency, maxRetries, maxRows, program, cmd.Bool("skip-failed"))
	if resumedChunks > 0 {
		pool.ResumeFrom(resumedRows, resumedChunks)
	}
	poolErr := pool.Run(runCtx)

	// Execute after hooks for all steps (only if job succeeded).
	if poolErr == nil {
		for i, hook := range afterHooks {
			if runCtx.Err() != nil {
				break
			}

			// Skip "once" hooks on resume (empty defaults to "once").
			skipHook := isResume && hook.run != "always"

			program.Send(engine.MsgHookStart{
				Phase: "after",
				Name:  hook.label,
				Index: i,
				Total: len(afterHooks),
			})

			if skipHook {
				program.Send(engine.MsgHookDone{
					Phase:   "after",
					Name:    hook.label,
					Index:   i,
					Total:   len(afterHooks),
					Skipped: true,
				})
				continue
			}

			start := time.Now()
			_, execErr := db.ExecContext(runCtx, hook.sql)
			program.Send(engine.MsgHookDone{
				Phase:    "after",
				Name:     hook.label,
				Index:    i,
				Total:    len(afterHooks),
				Duration: time.Since(start),
				Err:      execErr,
			})
			if execErr != nil {
				if runCtx.Err() != nil {
					break
				}
				program.Send(engine.MsgJobDone{Err: fmt.Errorf("after hook %q: %w", hook.label, execErr)})
				program.Wait()
				return fmt.Errorf("after hook %q: %w", hook.label, execErr)
			}
		}
	}

	// Signal TUI to exit.
	program.Send(engine.MsgJobDone{Err: poolErr})
	program.Wait()

	// If cancelled, exit cleanly.
	if runCtx.Err() != nil {
		_ = engine.CheckCancelled(runCtx)
		return nil
	}

	// Print final summary.
	completedChunks, failedChunks, rowsByStep, avgRuntime := store.Summary()
	fmt.Println()
	if poolErr != nil {
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
