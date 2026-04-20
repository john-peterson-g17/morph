package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/john-peterson-g17/morph/internal/progress"
)

// Compile-time check that Store implements progress.Store.
var _ progress.Store = (*Store)(nil)

// Store provides progress tracking backed by a PostgreSQL table.
type Store struct {
	db         *sql.DB
	schema     string
	table      string
	metaTable  string
	jobName    string
	jobVersion string
}

// validIdentifier matches safe SQL identifiers (letters, digits, underscores).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func quoteIdent(s string) (string, error) {
	if !validIdentifier.MatchString(s) {
		return "", fmt.Errorf("invalid SQL identifier: %q", s)
	}
	return `"` + s + `"`, nil
}

// New creates a Postgres-backed progress store, auto-creating
// the schema and tables if they don't exist.
func New(db *sql.DB, schema, table, jobName, jobVersion string) (*Store, error) {
	if schema == "" {
		schema = "morph"
	}
	if table == "" {
		table = "progress"
	}

	qSchema, err := quoteIdent(schema)
	if err != nil {
		return nil, fmt.Errorf("progress schema: %w", err)
	}
	qTable, err := quoteIdent(table)
	if err != nil {
		return nil, fmt.Errorf("progress table: %w", err)
	}
	qMeta, err := quoteIdent(table + "_meta")
	if err != nil {
		return nil, fmt.Errorf("progress meta table: %w", err)
	}

	s := &Store{
		db:         db,
		schema:     qSchema,
		table:      qSchema + "." + qTable,
		metaTable:  qSchema + "." + qMeta,
		jobName:    jobName,
		jobVersion: jobVersion,
	}

	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("migrating progress tables: %w", err)
	}

	return s, nil
}

func (s *Store) migrate() error {
	stmts := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, s.schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			job_name       TEXT NOT NULL,
			job_version    TEXT NOT NULL,
			chunk_start    TIMESTAMPTZ NOT NULL,
			chunk_end      TIMESTAMPTZ NOT NULL,
			status         TEXT NOT NULL DEFAULT 'pending',
			worker_id      INT,
			retries        INT DEFAULT 0,
			tables         JSONB DEFAULT '{}',
			error          TEXT,
			completed_at   TIMESTAMPTZ,
			last_updated   TIMESTAMPTZ DEFAULT now(),
			PRIMARY KEY (job_name, job_version, chunk_start, chunk_end)
		)`, s.table),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			job_name         TEXT NOT NULL,
			job_version      TEXT NOT NULL,
			load_start       TIMESTAMPTZ NOT NULL,
			load_end         TIMESTAMPTZ NOT NULL,
			started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
			last_updated     TIMESTAMPTZ DEFAULT now(),
			last_chunk_width BIGINT DEFAULT 0,
			PRIMARY KEY (job_name, job_version)
		)`, s.metaTable),
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("executing %q: %w", stmt[:40], err)
		}
	}
	return nil
}

func (s *Store) Init(jobName, jobVersion string, loadStart, loadEnd time.Time) {
	s.jobName = jobName
	s.jobVersion = jobVersion

	query := fmt.Sprintf(`INSERT INTO %s (job_name, job_version, load_start, load_end, started_at, last_updated)
		VALUES ($1, $2, $3, $4, now(), now())
		ON CONFLICT (job_name, job_version) DO UPDATE SET
			load_start = EXCLUDED.load_start,
			load_end = EXCLUDED.load_end,
			started_at = now(),
			last_updated = now()`, s.metaTable)
	_, _ = s.db.Exec(query, jobName, jobVersion, loadStart, loadEnd)
}

func (s *Store) HasData() bool {
	query := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE job_name = $1 AND job_version = $2)`, s.table)
	var exists bool
	if err := s.db.QueryRow(query, s.jobName, s.jobVersion).Scan(&exists); err != nil {
		return false
	}
	return exists
}

func (s *Store) Reset() error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE job_name = $1 AND job_version = $2`, s.table)
	if _, err := s.db.Exec(query, s.jobName, s.jobVersion); err != nil {
		return err
	}
	queryMeta := fmt.Sprintf(`DELETE FROM %s WHERE job_name = $1 AND job_version = $2`, s.metaTable)
	_, err := s.db.Exec(queryMeta, s.jobName, s.jobVersion)
	return err
}

func (s *Store) MarkChunkRunning(c progress.ChunkRange, workerID int) {
	query := fmt.Sprintf(`INSERT INTO %s (job_name, job_version, chunk_start, chunk_end, status, worker_id, last_updated)
		VALUES ($1, $2, $3, $4, 'running', $5, now())
		ON CONFLICT (job_name, job_version, chunk_start, chunk_end) DO UPDATE SET
			status = 'running',
			worker_id = EXCLUDED.worker_id,
			last_updated = now()`, s.table)
	_, _ = s.db.Exec(query, s.jobName, s.jobVersion, c.Start, c.End, workerID)
}

func (s *Store) MarkChunkStepDone(c progress.ChunkRange, stepName string, rows int64, d time.Duration) {
	stepData, _ := json.Marshal(map[string]progress.TableResult{
		stepName: {RowsInserted: rows, Duration: d},
	})
	query := fmt.Sprintf(`UPDATE %s SET
		tables = tables || $1::jsonb,
		last_updated = now()
		WHERE job_name = $2 AND job_version = $3 AND chunk_start = $4 AND chunk_end = $5`, s.table)
	_, _ = s.db.Exec(query, string(stepData), s.jobName, s.jobVersion, c.Start, c.End)
}

func (s *Store) MarkChunkComplete(c progress.ChunkRange, chunkWidth time.Duration) {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'complete',
		completed_at = now(),
		last_updated = now()
		WHERE job_name = $1 AND job_version = $2 AND chunk_start = $3 AND chunk_end = $4`, s.table)
	_, _ = s.db.Exec(query, s.jobName, s.jobVersion, c.Start, c.End)

	metaQuery := fmt.Sprintf(`UPDATE %s SET last_chunk_width = $1, last_updated = now()
		WHERE job_name = $2 AND job_version = $3`, s.metaTable)
	_, _ = s.db.Exec(metaQuery, int64(chunkWidth), s.jobName, s.jobVersion)
}

func (s *Store) MarkChunkFailed(c progress.ChunkRange, stepName string, err error) {
	errMsg := fmt.Sprintf("%s: %v", stepName, err)
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'failed',
		error = $1,
		retries = retries + 1,
		last_updated = now()
		WHERE job_name = $2 AND job_version = $3 AND chunk_start = $4 AND chunk_end = $5`, s.table)
	_, _ = s.db.Exec(query, errMsg, s.jobName, s.jobVersion, c.Start, c.End)
}

func (s *Store) IsChunkComplete(c progress.ChunkRange) bool {
	query := fmt.Sprintf(`SELECT status FROM %s
		WHERE job_name = $1 AND job_version = $2 AND chunk_start = $3 AND chunk_end = $4`, s.table)
	var status string
	if err := s.db.QueryRow(query, s.jobName, s.jobVersion, c.Start, c.End).Scan(&status); err != nil {
		return false
	}
	return status == "complete"
}

func (s *Store) IsChunkFailed(c progress.ChunkRange) bool {
	query := fmt.Sprintf(`SELECT status FROM %s
		WHERE job_name = $1 AND job_version = $2 AND chunk_start = $3 AND chunk_end = $4`, s.table)
	var status string
	if err := s.db.QueryRow(query, s.jobName, s.jobVersion, c.Start, c.End).Scan(&status); err != nil {
		return false
	}
	return status == "failed"
}

func (s *Store) GetResumePoint() (nextStart time.Time, lastWidth time.Duration, completedCount int) {
	var loadStart time.Time
	var lastChunkWidth int64
	metaQuery := fmt.Sprintf(`SELECT load_start, last_chunk_width FROM %s
		WHERE job_name = $1 AND job_version = $2`, s.metaTable)
	if err := s.db.QueryRow(metaQuery, s.jobName, s.jobVersion).Scan(&loadStart, &lastChunkWidth); err != nil {
		return time.Time{}, 0, 0
	}

	query := fmt.Sprintf(`SELECT chunk_start, chunk_end, status FROM %s
		WHERE job_name = $1 AND job_version = $2
		ORDER BY chunk_start`, s.table)
	rows, err := s.db.Query(query, s.jobName, s.jobVersion)
	if err != nil {
		return loadStart, time.Duration(lastChunkWidth), 0
	}
	defer func() { _ = rows.Close() }()

	type chunkRow struct {
		start, end time.Time
		status     string
	}
	var chunks []chunkRow
	for rows.Next() {
		var cr chunkRow
		if err := rows.Scan(&cr.start, &cr.end, &cr.status); err != nil {
			continue
		}
		chunks = append(chunks, cr)
	}

	cursor := loadStart
	completed := 0
	for _, c := range chunks {
		if c.status == "complete" && !c.start.After(cursor) {
			if c.end.After(cursor) {
				cursor = c.end
			}
			completed++
		}
	}

	return cursor, time.Duration(lastChunkWidth), completed
}

func (s *Store) TotalRows() int64 {
	query := fmt.Sprintf(`SELECT COALESCE(SUM((val->>'rows_inserted')::bigint), 0)
		FROM %s, jsonb_each(tables) AS kv(key, val)
		WHERE job_name = $1 AND job_version = $2 AND status = 'complete'`, s.table)

	var total int64
	if err := s.db.QueryRow(query, s.jobName, s.jobVersion).Scan(&total); err != nil {
		return 0
	}
	return total
}

func (s *Store) Summary() (completedChunks, failedChunks int, rowsByStep map[string]int64, avgRuntime time.Duration) {
	rowsByStep = make(map[string]int64)

	countQuery := fmt.Sprintf(`SELECT
		COALESCE(SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0)
		FROM %s WHERE job_name = $1 AND job_version = $2`, s.table)
	_ = s.db.QueryRow(countQuery, s.jobName, s.jobVersion).Scan(&completedChunks, &failedChunks)

	rowsQuery := fmt.Sprintf(`SELECT kv.key, COALESCE(SUM((kv.val->>'rows_inserted')::bigint), 0)
		FROM %s, jsonb_each(tables) AS kv(key, val)
		WHERE job_name = $1 AND job_version = $2 AND status = 'complete'
		GROUP BY kv.key`, s.table)
	rows, err := s.db.Query(rowsQuery, s.jobName, s.jobVersion)
	if err == nil {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var step string
			var count int64
			if err := rows.Scan(&step, &count); err == nil {
				rowsByStep[step] = count
			}
		}
	}

	if completedChunks > 0 {
		runtimeQuery := fmt.Sprintf(`SELECT COALESCE(SUM((kv.val->>'duration')::bigint), 0)
			FROM %s, jsonb_each(tables) AS kv(key, val)
			WHERE job_name = $1 AND job_version = $2 AND status = 'complete'`, s.table)
		var totalRuntime int64
		if err := s.db.QueryRow(runtimeQuery, s.jobName, s.jobVersion).Scan(&totalRuntime); err == nil {
			avgRuntime = time.Duration(totalRuntime) / time.Duration(completedChunks)
		}
	}

	return
}
