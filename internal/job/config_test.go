package job

import (
	"testing"
)

func TestComposeSQL_RawSQL(t *testing.T) {
	raw := `WITH cte AS (
  SELECT id FROM t WHERE ts >= $1 AND ts < $2
)
INSERT INTO target (id)
SELECT id FROM cte
ON CONFLICT (id) DO NOTHING`

	step := Step{
		Name: "raw",
		Morph: StepMorph{
			SQL: raw,
		},
	}

	got := step.ComposeSQL()
	if got != raw {
		t.Errorf("expected raw SQL returned verbatim\ngot:\n%s", got)
	}
}

func TestComposeSQL_RawSQLTrimmed(t *testing.T) {
	raw := "  INSERT INTO t (id) SELECT id FROM s WHERE ts >= $1 AND ts < $2  "
	step := Step{
		Name: "raw-trimmed",
		Morph: StepMorph{
			SQL: raw,
		},
	}

	got := step.ComposeSQL()
	want := "INSERT INTO t (id) SELECT id FROM s WHERE ts >= $1 AND ts < $2"
	if got != want {
		t.Errorf("expected trimmed raw SQL\ngot:  %q\nwant: %q", got, want)
	}
}

func TestComposeSQL_Composed(t *testing.T) {
	step := Step{
		Name: "composed",
		Morph: StepMorph{
			PartitionBy: "created_at",
			From:        StepSQL{SQL: "SELECT id, name FROM source"},
			Into:        StepSQL{SQL: "INSERT INTO target (id, name)\nON CONFLICT (id) DO NOTHING"},
		},
	}

	got := step.ComposeSQL()
	want := "INSERT INTO target (id, name)\nSELECT id, name FROM source\nWHERE created_at >= $1 AND created_at < $2\nON CONFLICT (id) DO NOTHING"
	if got != want {
		t.Errorf("composed SQL mismatch\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestComposeSQL_EmptySQLFallsThrough(t *testing.T) {
	step := Step{
		Name: "fallthrough",
		Morph: StepMorph{
			SQL:         "",
			PartitionBy: "ts",
			From:        StepSQL{SQL: "SELECT 1 FROM t"},
			Into:        StepSQL{SQL: "INSERT INTO t2 (id)"},
		},
	}

	got := step.ComposeSQL()
	want := "INSERT INTO t2 (id)\nSELECT 1 FROM t\nWHERE ts >= $1 AND ts < $2"
	if got != want {
		t.Errorf("expected composed fallthrough\ngot:\n%s\nwant:\n%s", got, want)
	}
}
