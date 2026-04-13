package job

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the fully parsed representation of a morph job file.
type Config struct {
	Version      string       `yaml:"version"`
	Job          JobMeta      `yaml:"job"`
	Driver       string       `yaml:"driver"`
	Partitioning Partitioning `yaml:"partitioning"`
	Runtime      Runtime      `yaml:"runtime"`
	Progress     Progress     `yaml:"progress"`
	Steps        []Step       `yaml:"steps"`
}

type JobMeta struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

type Partitioning struct {
	Strategy string   `yaml:"strategy"`
	Window   Window   `yaml:"window"`
	Adaptive Adaptive `yaml:"adaptive"`
}

type Window struct {
	Start time.Time `yaml:"start"`
	End   time.Time `yaml:"end"`
}

type Adaptive struct {
	InitialWidth  string `yaml:"initial_width"`
	MinWidth      string `yaml:"min_width"`
	MaxWidth      string `yaml:"max_width"`
	TargetRuntime string `yaml:"target_runtime"`
}

type Runtime struct {
	Defaults RuntimeDefaults `yaml:"defaults"`
}

type RuntimeDefaults struct {
	Concurrency      int    `yaml:"concurrency"`
	MaxRetries       int    `yaml:"max_retries"`
	MaxRows          int    `yaml:"max_rows"`
	StatementTimeout string `yaml:"statement_timeout"`
}

type Progress struct {
	Path string `yaml:"path"`
}

type Step struct {
	Name   string    `yaml:"name"`
	Before []StepSQL `yaml:"before"`
	Morph  StepMorph `yaml:"morph"`
	After  []StepSQL `yaml:"after"`
}

type StepMorph struct {
	PartitionBy string  `yaml:"partition_by"`
	From        StepSQL `yaml:"from"`
	Into        StepSQL `yaml:"into"`
}

type StepSQL struct {
	SQL  string `yaml:"sql"`
	Name string `yaml:"name"`
}

// ComposeSQL builds the final executable SQL by combining the into statement,
// from query, and a WHERE clause derived from partition_by.
func (s Step) ComposeSQL() string {
	fromSQL := strings.TrimSpace(s.Morph.From.SQL)
	intoSQL := strings.TrimSpace(s.Morph.Into.SQL)
	partitionBy := s.Morph.PartitionBy

	insertPrefix, insertSuffix := splitInsertSQL(intoSQL)
	where := fmt.Sprintf("WHERE %s >= $1 AND %s < $2", partitionBy, partitionBy)

	parts := []string{insertPrefix, fromSQL, where}
	if insertSuffix != "" {
		parts = append(parts, insertSuffix)
	}
	return strings.Join(parts, "\n")
}

// splitInsertSQL splits an INSERT statement at ON CONFLICT, ON DUPLICATE, or
// RETURNING clauses, returning the prefix and suffix.
func splitInsertSQL(sql string) (prefix, suffix string) {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.ToUpper(line))
		if strings.HasPrefix(trimmed, "ON CONFLICT") ||
			strings.HasPrefix(trimmed, "ON DUPLICATE") ||
			strings.HasPrefix(trimmed, "RETURNING") {
			prefix = strings.TrimSpace(strings.Join(lines[:i], "\n"))
			suffix = strings.TrimSpace(strings.Join(lines[i:], "\n"))
			return
		}
	}
	return sql, ""
}

// Load reads and parses a morph job file from disk.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading job file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing job file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks that required fields are present and values are sensible.
func (c *Config) Validate() error {
	if c.Job.Name == "" {
		return fmt.Errorf("job.name is required")
	}
	if c.Driver == "" {
		return fmt.Errorf("driver is required")
	}
	if c.Partitioning.Window.Start.IsZero() || c.Partitioning.Window.End.IsZero() {
		return fmt.Errorf("partitioning.window.start and partitioning.window.end are required")
	}
	if !c.Partitioning.Window.End.After(c.Partitioning.Window.Start) {
		return fmt.Errorf("partitioning.window.end must be after partitioning.window.start")
	}
	if len(c.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}
	for i, s := range c.Steps {
		if s.Name == "" {
			return fmt.Errorf("steps[%d].name is required", i)
		}
		if s.Morph.PartitionBy == "" {
			return fmt.Errorf("steps[%d].morph.partition_by is required", i)
		}
		if s.Morph.From.SQL == "" {
			return fmt.Errorf("steps[%d].morph.from.sql is required", i)
		}
		if s.Morph.Into.SQL == "" {
			return fmt.Errorf("steps[%d].morph.into.sql is required", i)
		}
	}
	return nil
}

// StepNames returns the names of all steps in order.
func (c *Config) StepNames() []string {
	names := make([]string, len(c.Steps))
	for i, s := range c.Steps {
		names[i] = s.Name
	}
	return names
}

// ParseDuration parses a duration string like "30m", "2h", "30s".
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return d, nil
}
