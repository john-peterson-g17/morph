package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/john-peterson-g17/morph/internal/monitor"
)

// stats holds a raw PG metrics snapshot.
type stats struct {
	activeConns   int
	idleConns     int
	waitingOnLock int
	cacheHitRatio float64
	txCommit      int64
	txRollback    int64
	deadlocks     int64
	tempBytes     int64
	oldestQuery   time.Duration
	replLag       time.Duration
	replAvailable bool
	collectedAt   time.Time
}

// config holds static PG server configuration.
type config struct {
	effectiveMax int
}

// Monitor implements monitor.Monitor for PostgreSQL databases.
type Monitor struct {
	db       *sql.DB
	cfg      config
	baseline *stats
	prev     *stats
}

// New creates a new PostgreSQL monitor using the given database connection.
func New(db *sql.DB) *Monitor {
	return &Monitor{db: db}
}

// Init queries static configuration and captures a baseline snapshot.
func (m *Monitor) Init(ctx context.Context) error {
	maxConns := 0
	if err := m.db.QueryRowContext(ctx, `SHOW max_connections`).Scan(&maxConns); err != nil {
		return err
	}

	reserved := 3
	_ = m.db.QueryRowContext(ctx, `SHOW superuser_reserved_connections`).Scan(&reserved)

	m.cfg.effectiveMax = maxConns - reserved
	if m.cfg.effectiveMax < 1 {
		m.cfg.effectiveMax = maxConns
	}

	baseline, err := m.collect(ctx)
	if err != nil {
		return err
	}
	m.baseline = &baseline
	m.prev = &baseline
	return nil
}

// Collect gathers current metrics, evaluates thresholds, and returns Health.
func (m *Monitor) Collect(ctx context.Context) (monitor.Health, error) {
	cur, err := m.collect(ctx)
	if err != nil {
		return monitor.Health{}, err
	}

	h := m.evaluate(cur)
	m.prev = &cur
	return h, nil
}

func (m *Monitor) collect(ctx context.Context) (stats, error) {
	var s stats
	s.collectedAt = time.Now()

	// Active/idle connections and lock waits.
	err := m.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE state IS DISTINCT FROM 'idle'),
			COUNT(*) FILTER (WHERE state = 'idle'),
			COUNT(*) FILTER (WHERE wait_event_type = 'Lock')
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid != pg_backend_pid()
	`).Scan(&s.activeConns, &s.idleConns, &s.waitingOnLock)
	if err != nil {
		return s, err
	}

	// Oldest active query age.
	var oldestSecs sql.NullFloat64
	err = m.db.QueryRowContext(ctx, `
		SELECT EXTRACT(EPOCH FROM max(now() - query_start))
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND state = 'active'
		  AND pid != pg_backend_pid()
	`).Scan(&oldestSecs)
	if err == nil && oldestSecs.Valid {
		s.oldestQuery = time.Duration(oldestSecs.Float64 * float64(time.Second))
	}

	// Database stats from pg_stat_database.
	var blksHit, blksRead int64
	err = m.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(xact_commit, 0),
			COALESCE(xact_rollback, 0),
			COALESCE(blks_hit, 0),
			COALESCE(blks_read, 0),
			COALESCE(deadlocks, 0),
			COALESCE(temp_bytes, 0)
		FROM pg_stat_database
		WHERE datname = current_database()
	`).Scan(&s.txCommit, &s.txRollback, &blksHit, &blksRead, &s.deadlocks, &s.tempBytes)
	if err != nil {
		return s, err
	}

	if total := blksHit + blksRead; total > 0 {
		s.cacheHitRatio = float64(blksHit) / float64(total) * 100
	} else {
		s.cacheHitRatio = 100
	}

	// Replication lag (only available on primaries with replicas).
	var lagSecs sql.NullFloat64
	err = m.db.QueryRowContext(ctx, `
		SELECT EXTRACT(EPOCH FROM max(replay_lag))
		FROM pg_stat_replication
	`).Scan(&lagSecs)
	if err == nil && lagSecs.Valid {
		s.replLag = time.Duration(lagSecs.Float64 * float64(time.Second))
		s.replAvailable = true
	}

	return s, nil
}

// evaluate builds a Health result from the current stats, using the baseline
// and config for context-aware thresholds.
func (m *Monitor) evaluate(cur stats) monitor.Health {
	var h monitor.Health

	h.Signals = append(h.Signals, m.evalConnections(cur))
	h.Signals = append(h.Signals, m.evalCacheHit(cur))
	h.Signals = append(h.Signals, m.evalLockWaits(cur))
	h.Signals = append(h.Signals, m.evalDeadlocks(cur))
	h.Signals = append(h.Signals, m.evalOldestQuery(cur))
	if cur.replAvailable {
		h.Signals = append(h.Signals, m.evalReplLag(cur))
	}
	h.Signals = append(h.Signals, m.evalTempDisk(cur))

	h.Overall = monitor.LevelGreen
	for _, s := range h.Signals {
		if s.Level > h.Overall {
			h.Overall = s.Level
		}
	}

	return h
}

func (m *Monitor) evalConnections(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Connections"}
	total := cur.activeConns + cur.idleConns
	pct := 0.0
	if m.cfg.effectiveMax > 0 {
		pct = float64(total) / float64(m.cfg.effectiveMax) * 100
	}

	s.Value = fmt.Sprintf("%d/%d (%.0f%%)  %d active, %d idle", total, m.cfg.effectiveMax, pct, cur.activeConns, cur.idleConns)
	if m.baseline != nil {
		baseTotal := m.baseline.activeConns + m.baseline.idleConns
		s.Baseline = fmt.Sprintf("%d/%d", baseTotal, m.cfg.effectiveMax)
	}

	switch {
	case pct > 90:
		s.Level = monitor.LevelRed
	case pct > 70:
		s.Level = monitor.LevelYellow
	}
	return s
}

func (m *Monitor) evalCacheHit(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Cache Hit"}
	s.Value = fmt.Sprintf("%.1f%%", cur.cacheHitRatio)

	if m.baseline != nil {
		s.Baseline = fmt.Sprintf("%.1f%%", m.baseline.cacheHitRatio)
		drop := m.baseline.cacheHitRatio - cur.cacheHitRatio
		if drop > 0 {
			s.Delta = fmt.Sprintf("▼%.1f%%", drop)
		}
	}

	switch {
	case cur.cacheHitRatio < 90:
		s.Level = monitor.LevelRed
	case cur.cacheHitRatio < 95:
		s.Level = monitor.LevelYellow
	}
	return s
}

func (m *Monitor) evalLockWaits(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Lock Waits"}
	s.Value = fmt.Sprintf("%d", cur.waitingOnLock)

	baselineVal := 0
	if m.baseline != nil {
		baselineVal = m.baseline.waitingOnLock
		s.Baseline = fmt.Sprintf("%d", baselineVal)
		if delta := cur.waitingOnLock - baselineVal; delta > 0 {
			s.Delta = fmt.Sprintf("▲%d", delta)
		}
	}

	increase := cur.waitingOnLock - baselineVal
	switch {
	case increase > 5:
		s.Level = monitor.LevelRed
	case increase > 0:
		s.Level = monitor.LevelYellow
	}
	return s
}

func (m *Monitor) evalDeadlocks(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Deadlocks"}

	baselineVal := int64(0)
	if m.baseline != nil {
		baselineVal = m.baseline.deadlocks
	}
	delta := cur.deadlocks - baselineVal
	if delta < 0 {
		delta = 0
	}

	s.Value = fmt.Sprintf("%d", delta)
	if delta > 0 {
		s.Level = monitor.LevelRed
		s.Delta = fmt.Sprintf("▲%d since start", delta)
	}
	return s
}

func (m *Monitor) evalOldestQuery(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Oldest Query"}
	s.Value = formatDur(cur.oldestQuery)

	switch {
	case cur.oldestQuery > 2*time.Minute:
		s.Level = monitor.LevelRed
	case cur.oldestQuery > 30*time.Second:
		s.Level = monitor.LevelYellow
	}
	return s
}

func (m *Monitor) evalReplLag(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Repl Lag"}
	s.Value = formatDur(cur.replLag)

	if m.baseline != nil && m.baseline.replAvailable {
		s.Baseline = formatDur(m.baseline.replLag)
		if increase := cur.replLag - m.baseline.replLag; increase > 0 {
			s.Delta = fmt.Sprintf("▲%s", formatDur(increase))
		}
	}

	switch {
	case cur.replLag > 10*time.Second:
		s.Level = monitor.LevelRed
	case cur.replLag > time.Second:
		s.Level = monitor.LevelYellow
	}
	return s
}

func (m *Monitor) evalTempDisk(cur stats) monitor.SignalHealth {
	s := monitor.SignalHealth{Name: "Temp Disk"}
	s.Value = formatBytes(cur.tempBytes)

	switch {
	case cur.tempBytes > 100*1024*1024:
		s.Level = monitor.LevelRed
	case cur.tempBytes > 10*1024*1024:
		s.Level = monitor.LevelYellow
	}
	return s
}

func formatDur(d time.Duration) string {
	d = d.Truncate(time.Millisecond)
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm %02ds", m, s)
}

func formatBytes(n int64) string {
	if n < 0 {
		return "0 B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	suffixes := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), suffixes[exp])
}
