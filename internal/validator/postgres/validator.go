package postgres

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Validator validates SQL in job file steps using lightweight pure-Go checks.
type Validator struct{}

type jobFile struct {
	Steps []struct {
		Name   string `yaml:"name"`
		Before []struct {
			Name string `yaml:"name"`
			SQL  string `yaml:"sql"`
		} `yaml:"before"`
		Morph struct {
			From struct {
				SQL string `yaml:"sql"`
			} `yaml:"from"`
		} `yaml:"morph"`
		After []struct {
			Name string `yaml:"name"`
			SQL  string `yaml:"sql"`
		} `yaml:"after"`
	} `yaml:"steps"`
}

func (v *Validator) Validate(data []byte) error {
	var jf jobFile
	if err := yaml.Unmarshal(data, &jf); err != nil {
		return fmt.Errorf("parsing job file for SQL validation: %w", err)
	}

	var errs []string
	for _, step := range jf.Steps {
		for i, hook := range step.Before {
			if hook.SQL == "" {
				continue
			}
			label := hook.Name
			if label == "" {
				label = fmt.Sprintf("before[%d]", i)
			}
			if err := validateSQL(hook.SQL); err != nil {
				errs = append(errs, fmt.Sprintf("step %q %s: %s", step.Name, label, err))
			}
		}
		if step.Morph.From.SQL != "" {
			if err := validateSQL(step.Morph.From.SQL); err != nil {
				errs = append(errs, fmt.Sprintf("step %q from.sql: %s", step.Name, err))
			}
		}
		for i, hook := range step.After {
			if hook.SQL == "" {
				continue
			}
			label := hook.Name
			if label == "" {
				label = fmt.Sprintf("after[%d]", i)
			}
			if err := validateSQL(hook.SQL); err != nil {
				errs = append(errs, fmt.Sprintf("step %q %s: %s", step.Name, label, err))
			}
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

// validateSQL performs lightweight syntax checks on a SQL string.
func validateSQL(sql string) error {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return fmt.Errorf("SQL is empty")
	}

	// Check balanced parentheses.
	depth := 0
	inSingle := false
	inDouble := false
	for i, ch := range trimmed {
		switch {
		case ch == '\'' && !inDouble:
			inSingle = !inSingle
		case ch == '"' && !inSingle:
			inDouble = !inDouble
		case !inSingle && !inDouble && ch == '(':
			depth++
		case !inSingle && !inDouble && ch == ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unmatched closing parenthesis at position %d", i)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unmatched opening parenthesis (%d unclosed)", depth)
	}
	if inSingle {
		return fmt.Errorf("unterminated single-quoted string")
	}
	if inDouble {
		return fmt.Errorf("unterminated double-quoted identifier")
	}

	// Check that SQL starts with a recognized keyword.
	upper := strings.ToUpper(trimmed)
	validStarts := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "WITH",
		"CREATE", "ALTER", "DROP", "TRUNCATE",
		"DO", "SET", "REINDEX", "VACUUM", "ANALYZE",
		"EXPLAIN", "REFRESH",
	}
	valid := false
	for _, kw := range validStarts {
		if strings.HasPrefix(upper, kw) {
			valid = true
			break
		}
	}
	if !valid {
		first := trimmed
		if len(first) > 30 {
			first = first[:30] + "..."
		}
		return fmt.Errorf("SQL does not start with a recognized statement keyword: %s", first)
	}

	return nil
}
