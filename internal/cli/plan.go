package cli

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

// PlanCommand returns the plan CLI command.
func PlanCommand() *cli.Command {
	return &cli.Command{
		Name:  "plan",
		Usage: "Preview the execution plan for a backfill without running it",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Aliases:  []string{"c"},
				Usage:    "Path to the backfill config file",
				Required: true,
			},
		},
		Action: runPlan,
	}
}

func runPlan(ctx context.Context, cmd *cli.Command) error {
	config := cmd.String("config")
	fmt.Printf("Planning backfill from config: %s\n", config)
	return nil
}
