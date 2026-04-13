package engine

import (
	"context"
	"fmt"
	"time"
)

// FormatRows returns a human-readable string for a row count (e.g. "12,450,000").
func FormatRows(n int64) string {
	if n < 0 {
		return "unknown"
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

// FormatDuration returns a compact human-readable duration string.
func FormatDuration(d time.Duration) string {
	d = d.Truncate(time.Millisecond)
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %02ds", m, s)
	}
	if d < 24*time.Hour {
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh %02dm", h, m)
	}
	days := int(d.Hours()) / 24
	h := int(d.Hours()) % 24
	return fmt.Sprintf("%dd %dh", days, h)
}

// CheckCancelled returns a user-friendly error if the context was cancelled.
func CheckCancelled(ctx context.Context) error {
	if ctx.Err() != nil {
		fmt.Println("\n\n✗ Interrupted — all in-flight queries have been cancelled on the server.")
		return fmt.Errorf("cancelled")
	}
	return nil
}

// FormatBytes returns a human-readable byte size string.
func FormatBytes(n int64) string {
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
