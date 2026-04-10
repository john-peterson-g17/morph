package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/john-peterson-g17/morph/internal/tui"
	"github.com/urfave/cli/v3"
)

// RunCommand returns the run CLI command.
func RunCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Execute a backfill job from a config file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Aliases:  []string{"c"},
				Usage:    "Path to the backfill config file",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "concurrency",
				Usage: "Number of parallel chunk workers",
				Value: 1,
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Print the execution plan and exit without running",
			},
		},
		Action: runBackfill,
	}
}

func runBackfill(ctx context.Context, cmd *cli.Command) error {
	config := cmd.String("config")

	// TODO: parse config, extract job name and step names
	// For now, use placeholders to demonstrate the TUI.
	jobName := fmt.Sprintf("backfill (%s)", config)
	steps := []string{"step 1", "step 2", "step 3"}

	p, err := tui.Run(jobName, steps)
	if err != nil {
		return fmt.Errorf("starting TUI: %w", err)
	}

	// TODO: replace with real backfill execution loop
	for i, name := range steps {
		_ = name
		p.Send(tui.StepStartedMsg{Index: i, Total: 5})
		for c := 1; c <= 5; c++ {
			time.Sleep(200 * time.Millisecond)
			p.Send(tui.ChunkDoneMsg{StepIndex: i, Chunks: c})
		}
		p.Send(tui.StepDoneMsg{Index: i, Elapsed: time.Second})
	}

	p.Send(tui.JobDoneMsg{})
	p.Wait()
	return nil
}
