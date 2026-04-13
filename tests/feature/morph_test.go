package feature

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// testdataDir returns the absolute path to the testdata directory.
func testdataDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "testdata")
}

func testdataFile(name string) string {
	return filepath.Join(testdataDir(), name)
}

func TestSingleStepBackfill(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	setupSchema(t, db)
	defer cleanup(t, db)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	seedRows(t, db, 100, start, end)

	progressDir := t.TempDir()
	out, err := runMorph(t,
		[]string{"run", testdataFile("single_step.v1.yml"), "--progress-dir", progressDir},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("morph run failed: %v\noutput: %s", err, out)
	}

	if got := tableCount(t, db, "morph_test_target"); got != 100 {
		t.Errorf("expected 100 rows in target, got %d", got)
	}
}

func TestSingleStepBackfillWithConflictClause(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	setupSchema(t, db)
	defer cleanup(t, db)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	seedRows(t, db, 80, start, end)

	progressDir := t.TempDir()
	jobFile := testdataFile("single_step_conflict.v1.yml")

	// Run twice — second run should not fail due to ON CONFLICT.
	out, err := runMorph(t,
		[]string{"run", jobFile, "--progress-dir", progressDir},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("first run failed: %v\noutput: %s", err, out)
	}

	out, err = runMorph(t,
		[]string{"run", jobFile, "--progress-dir", progressDir, "--fresh"},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("second (idempotent) run failed: %v\noutput: %s", err, out)
	}

	if got := tableCount(t, db, "morph_test_target"); got != 80 {
		t.Errorf("expected 80 rows in target, got %d", got)
	}
}

func TestMultiStepBackfill(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	setupSchema(t, db)
	defer cleanup(t, db)

	if _, err := db.Exec(`CREATE TABLE morph_test_target_2 (
		id    INT PRIMARY KEY,
		name  TEXT NOT NULL,
		ts    TIMESTAMPTZ NOT NULL
	)`); err != nil {
		t.Fatalf("creating second target: %v", err)
	}

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	seedRows(t, db, 50, start, end)

	progressDir := t.TempDir()
	out, err := runMorph(t,
		[]string{"run", testdataFile("multi_step.v1.yml"), "--progress-dir", progressDir},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("morph run failed: %v\noutput: %s", err, out)
	}

	if got := tableCount(t, db, "morph_test_target"); got != 50 {
		t.Errorf("target 1: expected 50 rows, got %d", got)
	}
	if got := tableCount(t, db, "morph_test_target_2"); got != 50 {
		t.Errorf("target 2: expected 50 rows, got %d", got)
	}
}

func TestConcurrentBackfill(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	setupSchema(t, db)
	defer cleanup(t, db)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 4, 0, 0, 0, 0, time.UTC)
	seedRows(t, db, 300, start, end)

	progressDir := t.TempDir()
	out, err := runMorph(t,
		[]string{"run", testdataFile("concurrent.v1.yml"), "--progress-dir", progressDir, "--concurrency", "4"},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("morph run failed: %v\noutput: %s", err, out)
	}

	if got := tableCount(t, db, "morph_test_target"); got != 300 {
		t.Errorf("expected 300 rows in target, got %d", got)
	}
}

func TestResumeFromProgress(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	setupSchema(t, db)
	defer cleanup(t, db)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)
	seedRows(t, db, 200, start, end)

	// Shared progress dir so the second run picks up where the first left off.
	progressDir := t.TempDir()

	// First run: job file with window covering only the first day.
	out, err := runMorph(t,
		[]string{"run", testdataFile("resume_partial.v1.yml"), "--progress-dir", progressDir},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("first run failed: %v\noutput: %s", err, out)
	}

	firstCount := tableCount(t, db, "morph_test_target")
	if firstCount == 0 {
		t.Fatal("expected rows after first run, got 0")
	}
	if firstCount == 200 {
		t.Fatal("expected partial data after first run, got all 200")
	}

	// Second run: full window, same progress dir — should resume and complete.
	out, err = runMorph(t,
		[]string{"run", testdataFile("resume_full.v1.yml"), "--progress-dir", progressDir},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("second run failed: %v\noutput: %s", err, out)
	}

	if got := tableCount(t, db, "morph_test_target"); got != 200 {
		t.Errorf("expected 200 rows after resume, got %d", got)
	}
}

func TestValidateCommand(t *testing.T) {
	out, err := runMorph(t, []string{"validate", testdataFile("valid.v1.yml")})
	if err != nil {
		t.Errorf("validate failed for valid job: %v\noutput: %s", err, out)
	}
}

func TestValidateCommandRejectsInvalid(t *testing.T) {
	_, err := runMorph(t, []string{"validate", testdataFile("invalid.v1.yml")})
	if err == nil {
		t.Error("expected validate to fail for invalid job, but it succeeded")
	}
}

func TestBeforeAfterHooks(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	setupSchema(t, db)
	defer cleanup(t, db)
	defer func() { _, _ = db.Exec(`DROP INDEX IF EXISTS idx_morph_test_target_value`) }()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	seedRows(t, db, 60, start, end)

	// Pre-populate target with some rows so that the TRUNCATE in the before
	// hook has something to clear.
	for i := 0; i < 10; i++ {
		ts := start.Add(time.Duration(i) * time.Hour)
		if _, err := db.Exec(
			`INSERT INTO morph_test_target (id, name, value, ts) VALUES ($1, $2, $3, $4)`,
			9000+i, "stale", i, ts,
		); err != nil {
			t.Fatalf("inserting pre-existing target row: %v", err)
		}
	}

	if got := tableCount(t, db, "morph_test_target"); got != 10 {
		t.Fatalf("expected 10 pre-existing target rows, got %d", got)
	}

	progressDir := t.TempDir()
	out, err := runMorph(t,
		[]string{"run", testdataFile("before_after.v1.yml"), "--progress-dir", progressDir},
		"DATABASE_URL="+testDSN(),
	)
	if err != nil {
		t.Fatalf("morph run failed: %v\noutput: %s", err, out)
	}

	// The before hook TRUNCATEs the target, so the 10 stale rows should be
	// gone and only the 60 source rows should be present.
	if got := tableCount(t, db, "morph_test_target"); got != 60 {
		t.Errorf("expected 60 rows in target (truncate + copy), got %d", got)
	}

	// The after hook creates an index on the value column.
	if !indexExists(t, db, "idx_morph_test_target_value") {
		t.Error("expected index idx_morph_test_target_value to exist after run")
	}
}
