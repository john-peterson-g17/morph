package feature

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// testDSN returns the DSN for the test database. It reads from MORPH_TEST_DSN
// or falls back to the compose defaults.
func testDSN() string {
	if dsn := os.Getenv("MORPH_TEST_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://morph:password@localhost:5432/morph?sslmode=disable"
}

var (
	binaryPath string
	buildOnce  sync.Once
	buildErr   error
)

// buildBinary compiles the morph binary once for the entire test suite.
func buildBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		// Resolve repo root relative to this test file.
		_, thisFile, _, _ := runtime.Caller(0)
		repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

		tmpDir, err := os.MkdirTemp("", "morph-test-bin-*")
		if err != nil {
			buildErr = fmt.Errorf("creating temp dir for binary: %w", err)
			return
		}
		binaryPath = filepath.Join(tmpDir, "morph")
		cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/morph")
		cmd.Dir = repoRoot
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("building morph binary: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binaryPath
}

// runMorph executes the morph binary with the given arguments and environment.
func runMorph(t *testing.T, args []string, env ...string) (string, error) {
	t.Helper()
	bin := buildBinary(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// openTestDB connects to the test database, skipping the test if unavailable.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", testDSN())
	if err != nil {
		t.Skipf("skipping: cannot open database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping: database not reachable: %v", err)
	}
	return db
}

// setupSchema creates the source and target tables used by tests. It drops
// them first to ensure a clean state.
func setupSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS morph_test_target`,
		`DROP TABLE IF EXISTS morph_test_source`,
		`CREATE TABLE morph_test_source (
			id    SERIAL PRIMARY KEY,
			name  TEXT NOT NULL,
			value INT NOT NULL,
			ts    TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE morph_test_target (
			id    INT PRIMARY KEY,
			name  TEXT NOT NULL,
			value INT NOT NULL,
			ts    TIMESTAMPTZ NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup schema: %v\n  SQL: %s", err, s)
		}
	}
}

// seedRows inserts n rows into the source table spread evenly across the time
// window [start, end).
func seedRows(t *testing.T, db *sql.DB, n int, start, end time.Time) {
	t.Helper()
	span := end.Sub(start)
	step := span / time.Duration(n)

	for i := 0; i < n; i++ {
		ts := start.Add(step * time.Duration(i))
		if _, err := db.Exec(
			`INSERT INTO morph_test_source (name, value, ts) VALUES ($1, $2, $3)`,
			fmt.Sprintf("row-%d", i), i, ts,
		); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
}

// tableCount returns the number of rows in the given table.
func tableCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count); err != nil {
		t.Fatalf("counting rows in %s: %v", table, err)
	}
	return count
}

// cleanup drops test tables.
func cleanup(t *testing.T, db *sql.DB) {
	t.Helper()
	db.Exec(`DROP TABLE IF EXISTS morph_test_target`)
	db.Exec(`DROP TABLE IF EXISTS morph_test_target_2`)
	db.Exec(`DROP TABLE IF EXISTS morph_test_source`)
}
