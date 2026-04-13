package previewer

import (
	"context"
	"time"
)

// RowEstimate holds the estimated row count for a single step.
type RowEstimate struct {
	StepName string
	Rows     int64
	Err      error
}

// StepQuery describes a step's query for estimation purposes.
type StepQuery struct {
	Name string
	SQL  string
}

// Previewer estimates row counts for morph job steps without executing them.
type Previewer interface {
	// EstimateRows returns estimated row counts for each step query within
	// the given time window. Implementations should use lightweight methods
	// (e.g. query planner estimates) rather than full table scans.
	EstimateRows(ctx context.Context, steps []StepQuery, windowStart, windowEnd time.Time) []RowEstimate
}
