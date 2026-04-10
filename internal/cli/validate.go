package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/john-peterson-g17/morph/internal/schema"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

// ValidateCommand returns the validate CLI command.
func ValidateCommand() *cli.Command {
	return &cli.Command{
		Name:  "validate",
		Usage: "Validate a job config file against the morph schema",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Aliases:  []string{"c"},
				Usage:    "Path to the job config file to validate",
				Required: true,
			},
		},
		Action: runValidate,
	}
}

func runValidate(ctx context.Context, cmd *cli.Command) error {
	configPath := cmd.String("config")

	// Read the job file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	// Parse YAML into a generic structure
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	// Convert to JSON-compatible types (yaml.v3 uses map[string]any which is fine)
	doc = convertYAML(doc)

	// Load the JSON schema
	var schemaDef any
	if err := json.Unmarshal(schema.JobSchema, &schemaDef); err != nil {
		return fmt.Errorf("loading schema: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("job.schema.json", schemaDef); err != nil {
		return fmt.Errorf("adding schema resource: %w", err)
	}

	sch, err := compiler.Compile("job.schema.json")
	if err != nil {
		return fmt.Errorf("compiling schema: %w", err)
	}

	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("validation failed:\n%s", err)
	}

	fmt.Printf("✓ %s is valid\n", configPath)
	return nil
}

// convertYAML recursively converts map[any]any (from yaml.v3) to map[string]any
// so the JSON schema validator can process it.
func convertYAML(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for k, val := range v {
			v[k] = convertYAML(val)
		}
		return v
	case map[any]any:
		m := make(map[string]any, len(v))
		for k, val := range v {
			m[fmt.Sprintf("%v", k)] = convertYAML(val)
		}
		return m
	case []any:
		for i, val := range v {
			v[i] = convertYAML(val)
		}
		return v
	default:
		return v
	case time.Time:
		return v.Format(time.RFC3339)
	}
}
