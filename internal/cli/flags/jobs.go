package flags

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"
)

const defaultJobsDir = "jobs"
const defaultProgressDir = ".morph/progress"

// JobDirFlag returns the shared --dir flag for job commands.
func JobDirFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "dir",
		Aliases: []string{"d"},
		Usage:   "Directory for job files",
		Value:   defaultJobsDir,
		Sources: cli.EnvVars("JOBS_DIRECTORY"),
	}
}

// ProgressDirFlag returns the shared --progress-dir flag for commands that track progress.
func ProgressDirFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-dir",
		Usage:   "Directory for progress tracking files",
		Value:   defaultProgressDir,
		Sources: cli.EnvVars("PROGRESS_DIRECTORY"),
	}
}

// DSNFlag returns the shared --dsn flag for commands that connect to a database.
func DSNFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "dsn",
		Usage: "Database connection string (overrides dsn_env from job config)",
	}
}

// ResolveDSN returns the database connection string. It prefers the --dsn flag
// value; if not set it falls back to the environment variable named by dsnEnv.
func ResolveDSN(cmd *cli.Command, dsnEnv string) (string, error) {
	if dsn := cmd.String("dsn"); dsn != "" {
		return dsn, nil
	}
	if dsnEnv != "" {
		if dsn := os.Getenv(dsnEnv); dsn != "" {
			return dsn, nil
		}
	}
	return "", fmt.Errorf("no database connection string: pass --dsn or set %s", dsnEnv)
}

// isFilePath returns true if the argument looks like a file path rather than
// a plain job name (contains path separators or a YAML extension).
func isFilePath(arg string) bool {
	return strings.Contains(arg, string(filepath.Separator)) ||
		strings.Contains(arg, "/") ||
		strings.HasSuffix(arg, ".yml") ||
		strings.HasSuffix(arg, ".yaml")
}

// jobNameFromPath extracts the job name from a file path by stripping the
// directory, the version suffix, and the extension.
// e.g. "jobs/events-backfill.v1.yml" → "events-backfill"
func jobNameFromPath(path string) string {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, filepath.Ext(base)) // strip .yml/.yaml
	base = strings.TrimSuffix(base, ".v1")              // strip version suffix
	return base
}

// ResolveJobFile resolves a job name or file path into the full path to the
// job YAML file. If the argument contains a path separator or YAML extension
// it is treated as a direct path; otherwise it is resolved relative to --dir.
func ResolveJobFile(cmd *cli.Command) (string, error) {
	args := cmd.Args()
	if args.Len() < 1 {
		return "", fmt.Errorf("job name is required")
	}
	arg := args.First()

	if isFilePath(arg) {
		return filepath.Clean(arg), nil
	}

	dir := cmd.String("dir")

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating jobs directory %q: %w", dir, err)
	}

	return filepath.Join(dir, arg+".v1.yml"), nil
}

// ResolveProgressFile resolves the progress file path for a job. If the
// positional argument is a file path the job name is extracted from it;
// otherwise the argument is used as the job name directly.
func ResolveProgressFile(cmd *cli.Command, version string) (string, error) {
	args := cmd.Args()
	if args.Len() < 1 {
		return "", fmt.Errorf("job name is required")
	}
	arg := args.First()

	name := arg
	if isFilePath(arg) {
		name = jobNameFromPath(arg)
	}

	dir := cmd.String("progress-dir")

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating progress directory %q: %w", dir, err)
	}

	return filepath.Join(dir, name+"."+version+".progress.json"), nil
}
