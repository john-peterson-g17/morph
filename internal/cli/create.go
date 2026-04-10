package cli

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

// CreateCommand returns the create CLI command.
func CreateCommand() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Generate a new backfill config file from a template",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "Name for the new backfill job",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output path for the config file",
				Value:   ".",
			},
		},
		Action: runCreate,
	}
}

func runCreate(ctx context.Context, cmd *cli.Command) error {
	name := cmd.String("name")
	output := cmd.String("output")
	fmt.Printf("Creating backfill config: %s (output: %s)\n", name, output)
	return nil
}
