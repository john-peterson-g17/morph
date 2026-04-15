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
		Name:    "dsn",
		Usage:   "Database connection string",
		Sources: cli.EnvVars("DATABASE_URL"),
	}
}

// ProgressDriverFlag returns the --progress-driver flag for selecting the progress store backend.
func ProgressDriverFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-driver",
		Usage:   "Progress store driver: file, postgres, s3",
		Sources: cli.EnvVars("PROGRESS_DRIVER"),
	}
}

// ProgressSchemaFlag returns the --progress-schema flag for the Postgres progress store.
func ProgressSchemaFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-schema",
		Usage:   "PostgreSQL schema for progress tables (default: morph)",
		Sources: cli.EnvVars("PROGRESS_SCHEMA"),
	}
}

// ProgressBucketFlag returns the --progress-bucket flag for the S3 progress store.
func ProgressBucketFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-bucket",
		Usage:   "S3 bucket for progress files",
		Sources: cli.EnvVars("PROGRESS_BUCKET"),
	}
}

// ProgressPrefixFlag returns the --progress-prefix flag for the S3 progress store.
func ProgressPrefixFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-prefix",
		Usage:   "S3 key prefix for progress files",
		Sources: cli.EnvVars("PROGRESS_PREFIX"),
	}
}

// ProgressEndpointFlag returns the --progress-endpoint flag for the S3 progress store.
func ProgressEndpointFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-endpoint",
		Usage:   "Custom S3 endpoint (for MinIO, R2, etc.)",
		Sources: cli.EnvVars("PROGRESS_ENDPOINT"),
	}
}

// ProgressRegionFlag returns the --progress-region flag for the S3 progress store.
func ProgressRegionFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "progress-region",
		Usage:   "AWS region for S3 progress store",
		Sources: cli.EnvVars("PROGRESS_REGION"),
	}
}

// ResolveDSN returns the database connection string from the --dsn flag
// (which also reads from the DATABASE_URL env var).
func ResolveDSN(cmd *cli.Command) (string, error) {
	if dsn := cmd.String("dsn"); dsn != "" {
		return dsn, nil
	}
	return "", fmt.Errorf("no database connection string: pass --dsn or set DATABASE_URL")
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
