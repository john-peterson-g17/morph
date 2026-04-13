<p align="center">
  <img src=".docs/images/logo.png" alt="Morph" width="600" />
</p>

<p align="center">
  <strong>Safe, resumable data backfills for schema evolution.</strong>
</p>

<p align="center">
  <a href="#installation">Install</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#commands">Commands</a> •
  <a href="#job-file-format">Config</a> •
  <a href="#how-it-works">How It Works</a>
</p>

---

> [!WARNING]
> This project is in early development. Expect breaking changes. Feedback and contributions are welcome.

## Why Morph?

Schema migrations are easy — until you need to move **millions or billions of rows**.

Most teams end up writing one-off scripts that overload the database, can't be resumed, don't track progress, and break halfway through. Morph exists to solve that.

## What It Does

- **Chunked backfills** — process large datasets incrementally without overwhelming your database
- **Adaptive sizing** — chunk width auto-tunes based on observed query runtime
- **Resumable jobs** — safely stop and restart without losing progress
- **Real-time TUI** — progress bar, worker status, row counts, ETA, and database health
- **Database health monitoring** — tracks connections, cache hit ratio, locks, replication lag
- **Before/after hooks** — drop indexes before a backfill, recreate them after
- **Row estimation** — EXPLAIN-based row estimates in the preview command
- **Schema validation** — validates job configs against a JSON schema before execution
- **Idempotent execution** — designed to safely re-run without duplicating data

## Installation

```bash
go install github.com/john-peterson-g17/morph/cmd/morph@latest
```

No C dependencies — pure Go, builds anywhere.

Or clone and build:

```bash
git clone https://github.com/john-peterson-g17/morph.git
cd morph
go build -o morph ./cmd/morph
```

## Quick Start

**1. Create a job file:**

```bash
morph create my-backfill
```

This generates `jobs/my-backfill.v1.yml` from a template.

**2. Edit the job file** with your source query, target table, and time window.

**3. Preview the execution plan:**

```bash
morph preview my-backfill --dsn "postgres://user:pass@host:5432/db"
```

**4. Run the backfill:**

```bash
morph run my-backfill --dsn "postgres://user:pass@host:5432/db"
```

Or set `DATABASE_URL` in your environment and skip `--dsn`.

## Commands

### `morph run <job name>`

Execute a backfill job with real-time TUI output.

```
Flags:
  --dsn              Database connection string (env: DATABASE_URL)
  --dir, -d          Job files directory (default: "jobs")
  --progress-dir     Progress file directory (default: ".morph/progress")
  --concurrency      Parallel chunk workers (default: from job config or 1)
  --fresh            Discard previous progress and start from scratch
  --debug            Show executed SQL queries in output
```

### `morph preview <job name>`

Preview the execution plan without running anything. Shows job metadata, partitioning config, runtime settings, composed SQL queries, and EXPLAIN-based row estimates (if the database is reachable).

```
Flags:
  --dsn              Database connection string (env: DATABASE_URL)
  --dir, -d          Job files directory (default: "jobs")
```

### `morph validate <job name>`

Validate a job config against the schema and check SQL syntax.

```
Flags:
  --dir, -d          Job files directory (default: "jobs")
```

### `morph create <job name>`

Generate a new job file from the built-in template.

```
Flags:
  --dir, -d          Job files directory (default: "jobs")
```

## Job File Format

Job files use YAML with a `.v1.yml` extension. You can pass a job by name (`morph run my-backfill`) and it resolves to `jobs/my-backfill.v1.yml`, or pass a path directly (`morph run path/to/file.v1.yml`).

```yaml
version: v1

job:
  name: event-states-backfill
  description: Populate event_states from events and attempts tables

driver: postgres

partitioning:
  strategy: time_range
  window:
    start: "2024-01-01T00:00:00Z"
    end: "2026-04-13T00:00:00Z"
  adaptive:
    initial_width: 30m
    min_width: 5m
    max_width: 2h
    target_runtime: 30s

runtime:
  defaults:
    concurrency: 10
    max_retries: 3
    max_rows: 0            # 0 = unlimited
    statement_timeout: 1h

steps:
  - name: event_states
    before:
      - name: drop index
        sql: DROP INDEX IF EXISTS idx_event_states_updated_at

    morph:
      partition_by: e.occurred_at
      from:
        sql: |
          SELECT e.id, e.status, e.updated_at
          FROM events.events e
      into:
        sql: |
          INSERT INTO events.event_states (event_id, status, updated_at)
          ON CONFLICT (event_id) DO NOTHING

    after:
      - name: create index
        sql: CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_states_updated_at ON events.event_states (updated_at)
```

The `from.sql` query is automatically wrapped with `WHERE {partition_by} >= $1 AND {partition_by} < $2` using the chunk boundaries. The `into.sql` is split at `ON CONFLICT` / `RETURNING` to compose the final statement.

## How It Works

### Adaptive Chunking

Morph divides the time window into chunks and adapts their width based on observed runtime:

1. Start with `initial_width` (e.g. 30 minutes)
2. After each chunk, compare actual runtime to `target_runtime`
3. Adjust width proportionally with smoothing (70% new / 30% old) and a 1.5x growth cap
4. Empty chunks automatically expand; width snaps back when data appears
5. Width is always clamped between `min_width` and `max_width`

### Progress Tracking

Progress is saved to a JSON file after each chunk. If a job is interrupted:

- Re-running the same job resumes from where it left off
- Use `--fresh` to discard progress and start over
- Progress files live in `.morph/progress/` by default

### Worker Pool

Multiple workers process chunks in parallel from a shared queue. Each worker:

1. Picks a chunk from the queue
2. Executes all steps sequentially within that chunk
3. Records results and adjusts the adaptive planner
4. Retries failed chunks up to `max_retries` times

### Database Health Monitoring

During `morph run`, a background monitor polls the database every 2 seconds and displays health signals in the TUI with traffic-light coloring:

| Signal | Green | Yellow | Red |
|---|---|---|---|
| Cache hit ratio | > 95% | 90–95% | < 90% |
| Active connections | normal | elevated | near limit |
| Lock waits | none | some | many |
| Deadlocks | none | — | increasing |
| Replication lag | low | elevated | high |

### TUI Display

The real-time terminal UI shows:

- **Progress bar** with percentage and ETA
- **Per-worker status** — current chunk, step, rows, elapsed time
- **Completed chunks log** — recent successes with timing and row counts
- **Database health** — per-signal status with baseline comparison
- **Debug mode** (`--debug`) — shows executed SQL for each completed chunk

## Local Development

```bash
# Start postgres
docker compose up -d

# Get the mapped port
docker compose port postgres 5432

# Run tests
DATABASE_URL="postgres://morph:password@localhost:<port>/morph?sslmode=disable" go test ./...
```

### Code Quality

```bash
go fmt ./...
go vet ./...
golangci-lint run
```

## License

MIT
