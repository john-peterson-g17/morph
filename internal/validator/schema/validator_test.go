package schema

import (
	"testing"
)

func validJobYAML() []byte {
	return []byte(`
version: v1

job:
  name: test-job
  description: A test backfill

connection:
  driver: postgres
  dsn_env: DATABASE_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"
  adaptive:
    initial_width: 30m
    min_width: 5m
    max_width: 2h
    target_runtime: 30s

runtime:
  defaults:
    concurrency: 4
    max_retries: 3
    max_rows: 0
    statement_timeout: 1h

steps:
  - name: my_step
    morph:
      partition_by: created_at
      from:
        sql: SELECT id, name FROM source
      into:
        sql: INSERT INTO target (id, name)
`)
}

func TestValidate_ValidJob(t *testing.T) {
	v := &Validator{}
	if err := v.Validate(validJobYAML()); err != nil {
		t.Errorf("expected valid, got error: %v", err)
	}
}

func TestValidate_MinimalJob(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: minimal

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err != nil {
		t.Errorf("expected valid minimal job, got error: %v", err)
	}
}

func TestValidate_MissingVersion(t *testing.T) {
	data := []byte(`
job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for missing version")
	}
}

func TestValidate_MissingJobName(t *testing.T) {
	data := []byte(`
version: v1

job:
  description: no name

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for missing job name")
	}
}

func TestValidate_MissingConnection(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for missing connection")
	}
}

func TestValidate_InvalidDriver(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

connection:
  driver: mysql
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for unsupported driver")
	}
}

func TestValidate_MissingSteps(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for missing steps")
	}
}

func TestValidate_EmptySteps(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps: []
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for empty steps array")
	}
}

func TestValidate_InvalidVersion(t *testing.T) {
	data := []byte(`
version: v99

job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for invalid version")
	}
}

func TestValidate_InvalidStrategy(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: id_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for invalid strategy")
	}
}

func TestValidate_InvalidDurationFormat(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"
  adaptive:
    initial_width: "30 minutes"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for invalid duration format")
	}
}

func TestValidate_AdditionalProperties(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test
  unknown_field: bad

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for unknown field in job")
	}
}

func TestValidate_WithBeforeAfterHooks(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: hooks-test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: s1
    before:
      - name: drop idx
        sql: DROP INDEX IF EXISTS idx
    morph:
      partition_by: ts
      from:
        sql: SELECT 1
      into:
        sql: INSERT INTO t (id)
    after:
      - name: create idx
        sql: CREATE INDEX idx ON t (col)
`)
	v := &Validator{}
	if err := v.Validate(data); err != nil {
		t.Errorf("expected valid job with hooks, got error: %v", err)
	}
}

func TestValidate_StepMissingMorph(t *testing.T) {
	data := []byte(`
version: v1

job:
  name: test

connection:
  driver: postgres
  dsn_env: DB_URL

partitioning:
  strategy: time_range
  window:
    start: "2025-01-01T00:00:00Z"
    end: "2025-02-01T00:00:00Z"

steps:
  - name: bad_step
`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for step missing morph")
	}
}

func TestValidate_InvalidYAML(t *testing.T) {
	data := []byte(`{{{not yaml`)
	v := &Validator{}
	if err := v.Validate(data); err == nil {
		t.Error("expected error for invalid YAML")
	}
}
