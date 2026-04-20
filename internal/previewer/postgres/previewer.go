package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/john-peterson-g17/morph/internal/previewer"
)

// Postgres implements previewer.Previewer using EXPLAIN for row estimates.
type Postgres struct {
	db *sql.DB
}

// New returns a new Postgres previewer.
func New(db *sql.DB) *Postgres {
	return &Postgres{db: db}
}

func (p *Postgres) EstimateRows(ctx context.Context, steps []previewer.StepQuery, windowStart, windowEnd time.Time) []previewer.RowEstimate {
	estimates := make([]previewer.RowEstimate, len(steps))

	for i, step := range steps {
		estimates[i].StepName = step.Name

		explainSQL := "EXPLAIN " + step.SQL
		rows, err := p.db.QueryContext(ctx, explainSQL, windowStart, windowEnd)
		if err != nil {
			estimates[i].Err = err
			continue
		}

		var totalRows float64
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				continue
			}
			// The first line contains the top-level node's row estimate.
			if totalRows == 0 {
				if idx := strings.Index(line, "rows="); idx >= 0 {
					_, _ = fmt.Sscanf(line[idx:], "rows=%f", &totalRows)
				}
			}
		}
		_ = rows.Close()

		estimates[i].Rows = int64(totalRows)
	}

	return estimates
}

func (p *Postgres) ExplainQuery(ctx context.Context, steps []previewer.StepQuery, windowStart, windowEnd time.Time) []previewer.ExplainResult {
	results := make([]previewer.ExplainResult, len(steps))

	for i, step := range steps {
		results[i].StepName = step.Name

		explainSQL := "EXPLAIN (ANALYZE false, COSTS true, FORMAT TEXT) " + step.SQL
		rows, err := p.db.QueryContext(ctx, explainSQL, windowStart, windowEnd)
		if err != nil {
			results[i].Err = err
			continue
		}

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				continue
			}
			results[i].Lines = append(results[i].Lines, line)
		}
		_ = rows.Close()
	}

	return results
}
