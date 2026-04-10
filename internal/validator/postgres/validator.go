package postgres

import (
	"fmt"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"gopkg.in/yaml.v3"
)

// Validator validates SQL in job file steps using the PostgreSQL parser.
type Validator struct{}

type jobFile struct {
	Steps []struct {
		Name string `yaml:"name"`
		SQL  string `yaml:"sql"`
	} `yaml:"steps"`
}

func (v *Validator) Validate(data []byte) error {
	var jf jobFile
	if err := yaml.Unmarshal(data, &jf); err != nil {
		return fmt.Errorf("parsing job file for SQL validation: %w", err)
	}

	var errs []string
	for _, step := range jf.Steps {
		if step.SQL == "" {
			continue
		}
		if _, err := pgquery.Parse(step.SQL); err != nil {
			errs = append(errs, fmt.Sprintf("step %q: %s", step.Name, err))
		}
	}

	if len(errs) > 0 {
		msg := errs[0]
		for _, e := range errs[1:] {
			msg += "\n  " + e
		}
		return fmt.Errorf("SQL validation failed:\n  %s", msg)
	}

	return nil
}
