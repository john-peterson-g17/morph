package job

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the fully parsed representation of a morph job file.
type Config struct {
	Version    string     `yaml:"version"`
	Job        JobMeta    `yaml:"job"`
	Connection Connection `yaml:"connection"`
	Source     Source     `yaml:"source"`
	Execution  Execution  `yaml:"execution"`
	Progress   Progress   `yaml:"progress"`
	Safety     Safety     `yaml:"safety"`
	Steps      []Step     `yaml:"steps"`
}

type JobMeta struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

type Connection struct {
	Driver string `yaml:"driver"`
	DSNEnv string `yaml:"dsn_env"`
}

type Source struct {
	Schema   string   `yaml:"schema"`
	Window   Window   `yaml:"window"`
	Chunking Chunking `yaml:"chunking"`
}

type Window struct {
	Start time.Time `yaml:"start"`
	End   time.Time `yaml:"end"`
}

type Chunking struct {
	Strategy      string `yaml:"strategy"`
	Column        string `yaml:"column"`
	InitialWidth  string `yaml:"initial_width"`
	MinWidth      string `yaml:"min_width"`
	MaxWidth      string `yaml:"max_width"`
	TargetRuntime string `yaml:"target_runtime"`
}

type Execution struct {
	Concurrency int  `yaml:"concurrency"`
	MaxRetries  int  `yaml:"max_retries"`
	MaxRows     int  `yaml:"max_rows"`
	DryRun      bool `yaml:"dry_run"`
}

type Progress struct {
	Path  string `yaml:"path"`
	Fresh bool   `yaml:"fresh"`
}

type Safety struct {
	StatementTimeout string `yaml:"statement_timeout"`
	TruncateTargets  bool   `yaml:"truncate_targets"`
	DropIndexes      bool   `yaml:"drop_indexes"`
	RebuildIndexes   bool   `yaml:"rebuild_indexes"`
}

type Step struct {
	Name   string     `yaml:"name"`
	Target StepTarget `yaml:"target"`
	SQL    string     `yaml:"sql"`
}

type StepTarget struct {
	Schema string `yaml:"schema"`
	Table  string `yaml:"table"`
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
	if c.Connection.Driver == "" {
		return fmt.Errorf("connection.driver is required")
	}
	if c.Connection.DSNEnv == "" {
		return fmt.Errorf("connection.dsn_env is required")
	}
	if c.Source.Window.Start.IsZero() || c.Source.Window.End.IsZero() {
		return fmt.Errorf("source.window.start and source.window.end are required")
	}
	if !c.Source.Window.End.After(c.Source.Window.Start) {
		return fmt.Errorf("source.window.end must be after source.window.start")
	}
	if len(c.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}
	for i, s := range c.Steps {
		if s.Name == "" {
			return fmt.Errorf("steps[%d].name is required", i)
		}
		if s.Target.Table == "" {
			return fmt.Errorf("steps[%d].target.table is required", i)
		}
		if s.SQL == "" {
			return fmt.Errorf("steps[%d].sql is required", i)
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
