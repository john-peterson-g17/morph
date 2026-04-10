package main

import (
	"context"
	"fmt"
	"os"

	"github.com/john-peterson-g17/morph/internal/cli"
	"github.com/joho/godotenv"
	urfavecli "github.com/urfave/cli/v3"
)

func main() {
	// Load .env if present; environment variables take precedence.
	_ = godotenv.Load()

	app := &urfavecli.Command{
		Name:  "morph",
		Usage: "Safe, resumable data backfills for schema evolution",
		Commands: []*urfavecli.Command{
			cli.RunCommand(),
			cli.ValidateCommand(),
			cli.PlanCommand(),
			cli.CreateCommand(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
