package monitor

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Monitor collects database health metrics periodically. Each database driver
// provides its own implementation with driver-specific queries, signals, and
// threshold logic. The shared interface only traffics in Health — a
// driver-agnostic representation of signal statuses.
type Monitor interface {
	// Init performs one-time setup: queries static config and captures a
	// baseline snapshot for relative threshold evaluation. Must be called
	// before Collect.
	Init(ctx context.Context) error

	// Collect gathers current metrics, evaluates thresholds against the
	// baseline captured in Init, and returns an evaluated Health result.
	Collect(ctx context.Context) (Health, error)
}

// MsgDBStats is sent to the TUI with the latest evaluated health.
type MsgDBStats struct {
	Health Health
}

// RunCollector starts a background goroutine that calls mon.Collect every
// interval and sends MsgDBStats to the TUI program. It stops when ctx is
// cancelled. Init must be called before RunCollector.
func RunCollector(ctx context.Context, mon Monitor, program *tea.Program, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h, err := mon.Collect(ctx)
				if err != nil || ctx.Err() != nil {
					continue
				}
				program.Send(MsgDBStats{Health: h})
			}
		}
	}()
}
