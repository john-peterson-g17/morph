package cli

import (
	"context"
	"fmt"
	"os"
	"text/template"

	"github.com/john-peterson-g17/morph/internal/cli/flags"
	"github.com/john-peterson-g17/morph/internal/schema"
	"github.com/urfave/cli/v3"
)

// CreateCommand returns the create CLI command.
func CreateCommand() *cli.Command {
	return &cli.Command{
		Name:      "create",
		Usage:     "Generate a new backfill config file from a template",
		UsageText: "morph create <job name>",
		Flags: []cli.Flag{
			flags.JobDirFlag(),
		},
		Action: runCreate,
	}
}

func runCreate(ctx context.Context, cmd *cli.Command) error {
	jobFile, err := flags.ResolveJobFile(cmd)
	if err != nil {
		return err
	}

	if _, err := os.Stat(jobFile); err == nil {
		return fmt.Errorf("file already exists: %s", jobFile)
	}

	jobName := cmd.Args().First()

	tmpl, err := template.New("job").Parse(string(schema.JobTemplateV1))
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	f, err := os.Create(jobFile)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := tmpl.Execute(f, map[string]string{"Name": jobName}); err != nil {
		return fmt.Errorf("writing template: %w", err)
	}

	fmt.Printf("✓ Created %s\n", jobFile)
	return nil
}
