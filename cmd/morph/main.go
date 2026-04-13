package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/john-peterson-g17/morph/internal/cli"
	"github.com/joho/godotenv"
	urfavecli "github.com/urfave/cli/v3"
)

func main() {
	// Load .env if present; environment variables take precedence.
	_ = godotenv.Load()

	// Set up signal-cancellable context so all commands can be interrupted.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	app := &urfavecli.Command{
		Name:  "morph",
		Usage: "Safe, resumable data backfills for schema evolution",
		Commands: []*urfavecli.Command{
			cli.RunCommand(),
			cli.ValidateCommand(),
			cli.PreviewCommand(),
			cli.CreateCommand(),
		},
	}

	if err := app.Run(ctx, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
