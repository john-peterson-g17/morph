package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/john-peterson-g17/morph/internal/cli/flags"
	"github.com/john-peterson-g17/morph/internal/validator"
	pgvalidator "github.com/john-peterson-g17/morph/internal/validator/postgres"
	schemavalidator "github.com/john-peterson-g17/morph/internal/validator/schema"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

// ValidateCommand returns the validate CLI command.
func ValidateCommand() *cli.Command {
	return &cli.Command{
		Name:      "validate",
		Usage:     "Validate a job config file against the morph schema",
		UsageText: "morph validate <job name>",
		Flags: []cli.Flag{
			flags.JobDirFlag(),
		},
		Action: runValidate,
	}
}

// driverHeader is used to extract the driver from the job file.
type driverHeader struct {
	Connection struct {
		Driver string `yaml:"driver"`
	} `yaml:"connection"`
}

func runValidate(ctx context.Context, cmd *cli.Command) error {
	configPath, err := flags.ResolveJobFile(cmd)
	if err != nil {
		return err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	// Always validate the schema first
	validators := []validator.Validator{
		&schemavalidator.Validator{},
	}

	// Add driver-specific validators
	var hdr driverHeader
	if err := yaml.Unmarshal(data, &hdr); err == nil {
		driver := hdr.Connection.Driver
		if driver == "" {
			driver = "postgres"
		}
		switch driver {
		case "postgres":
			validators = append(validators, &pgvalidator.Validator{})
		default:
			return fmt.Errorf("unsupported driver %q for SQL validation", driver)
		}
	}

	for _, v := range validators {
		if err := v.Validate(data); err != nil {
			return err
		}
	}

	fmt.Printf("✓ %s is valid\n", configPath)
	return nil
}
