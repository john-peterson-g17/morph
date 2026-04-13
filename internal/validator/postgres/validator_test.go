package postgres

import (
	"testing"
)

func TestValidateSQL_Valid(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"simple select", "SELECT id FROM users"},
		{"insert", "INSERT INTO t (a) VALUES (1)"},
		{"update", "UPDATE t SET a = 1 WHERE id = 2"},
		{"delete", "DELETE FROM t WHERE id = 1"},
		{"with cte", "WITH x AS (SELECT 1) SELECT * FROM x"},
		{"create index", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON t (col)"},
		{"drop index", "DROP INDEX IF EXISTS idx"},
		{"truncate", "TRUNCATE TABLE t"},
		{"set", "SET statement_timeout = '30s'"},
		{"analyze", "ANALYZE my_table"},
		{"refresh", "REFRESH MATERIALIZED VIEW mv"},
		{"nested parens", "SELECT (a + (b * c)) FROM t"},
		{"single quoted string with parens", "SELECT '(not a paren)' FROM t"},
		{"double quoted identifier", `SELECT "Column Name" FROM t`},
		{"do block", "DO $$ BEGIN RAISE NOTICE 'hi'; END $$"},
		{"lowercase keyword", "select 1"},
		{"mixed case", "Select id From users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateSQL(tt.sql); err != nil {
				t.Errorf("expected valid SQL, got error: %v", err)
			}
		})
	}
}

func TestValidateSQL_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{"empty string", "", "SQL is empty"},
		{"whitespace only", "   \n\t  ", "SQL is empty"},
		{"unmatched open paren", "SELECT (a FROM t", "unmatched opening parenthesis"},
		{"unmatched close paren", "SELECT a) FROM t", "unmatched closing parenthesis"},
		{"nested unmatched", "SELECT ((a + b) FROM t", "unmatched opening parenthesis"},
		{"unterminated single quote", "SELECT 'hello FROM t", "unterminated single-quoted string"},
		{"unterminated double quote", `SELECT "col FROM t`, "unterminated double-quoted identifier"},
		{"bad keyword", "GIBBERISH something", "does not start with a recognized statement keyword"},
		{"number start", "123 SELECT", "does not start with a recognized statement keyword"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSQL(tt.sql)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if got := err.Error(); !contains(got, tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, got)
			}
		})
	}
}

func TestValidate_FullJobFile(t *testing.T) {
	valid := []byte(`
steps:
  - name: backfill
    morph:
      from:
        sql: |
          SELECT id, name FROM source
    before:
      - name: drop idx
        sql: DROP INDEX IF EXISTS idx_target
    after:
      - name: create idx
        sql: CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_target ON target (name)
`)

	v := &Validator{}
	if err := v.Validate(valid); err != nil {
		t.Errorf("expected valid job file, got error: %v", err)
	}
}

func TestValidate_InvalidSQL(t *testing.T) {
	data := []byte(`
steps:
  - name: bad_step
    morph:
      from:
        sql: "SELECT ((a FROM t"
`)

	v := &Validator{}
	err := v.Validate(data)
	if err == nil {
		t.Fatal("expected error for invalid SQL, got nil")
	}
	if !contains(err.Error(), "SQL validation failed") {
		t.Errorf("expected SQL validation error, got: %v", err)
	}
}

func TestValidate_InvalidBeforeHook(t *testing.T) {
	data := []byte(`
steps:
  - name: my_step
    before:
      - sql: "GIBBERISH something"
    morph:
      from:
        sql: SELECT 1
`)

	v := &Validator{}
	err := v.Validate(data)
	if err == nil {
		t.Fatal("expected error for invalid before hook SQL, got nil")
	}
	if !contains(err.Error(), "before[0]") {
		t.Errorf("expected error mentioning before[0], got: %v", err)
	}
}

func TestValidate_InvalidAfterHook(t *testing.T) {
	data := []byte(`
steps:
  - name: my_step
    morph:
      from:
        sql: SELECT 1
    after:
      - name: bad hook
        sql: "SELECT 'unclosed"
`)

	v := &Validator{}
	err := v.Validate(data)
	if err == nil {
		t.Fatal("expected error for invalid after hook SQL, got nil")
	}
	if !contains(err.Error(), "bad hook") {
		t.Errorf("expected error mentioning hook name, got: %v", err)
	}
}

func TestValidate_SkipsEmptySQL(t *testing.T) {
	data := []byte(`
steps:
  - name: my_step
    before:
      - name: empty hook
        sql: ""
    morph:
      from:
        sql: SELECT 1
`)

	v := &Validator{}
	if err := v.Validate(data); err != nil {
		t.Errorf("expected no error for empty hook SQL, got: %v", err)
	}
}

func TestValidate_MultipleErrors(t *testing.T) {
	data := []byte(`
steps:
  - name: step1
    morph:
      from:
        sql: "GIBBERISH one"
    before:
      - sql: "GIBBERISH two"
`)

	v := &Validator{}
	err := v.Validate(data)
	if err == nil {
		t.Fatal("expected errors, got nil")
	}
	msg := err.Error()
	if !contains(msg, "before[0]") || !contains(msg, "from.sql") {
		t.Errorf("expected both errors reported, got: %v", err)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
