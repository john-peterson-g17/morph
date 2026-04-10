package cli

import (
	"context"
	"fmt"

	"github.com/john-peterson-g17/morph/internal/cli/flags"
	"github.com/urfave/cli/v3"
)

// PlanCommand returns the plan CLI command.
func PlanCommand() *cli.Command {
	return &cli.Command{
		Name:      "plan",
		Usage:     "Preview the execution plan for a backfill without running it",
		UsageText: "morph plan <job name>",
		Flags: []cli.Flag{
			flags.JobDirFlag(),
		},
		Action: runPlan,
	}
}

func runPlan(ctx context.Context, cmd *cli.Command) error {
	jobFile, err := flags.ResolveJobFile(cmd)
	if err != nil {
		return err
	}
	fmt.Printf("Planning backfill from config: %s\n", jobFile)
	return nil
}
