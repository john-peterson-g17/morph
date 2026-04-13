package monitor

// Level represents the health status of a signal.
type Level int

const (
	LevelGreen  Level = iota // Healthy
	LevelYellow              // Elevated
	LevelRed                 // Degraded
)

// String returns the display label for a health level.
func (l Level) String() string {
	switch l {
	case LevelYellow:
		return "Elevated"
	case LevelRed:
		return "Degraded"
	default:
		return "Healthy"
	}
}

// Icon returns the status emoji for a health level.
func (l Level) Icon() string {
	switch l {
	case LevelYellow:
		return "🟡"
	case LevelRed:
		return "🔴"
	default:
		return "🟢"
	}
}

// SignalHealth holds the evaluated health of a single metric.
type SignalHealth struct {
	Name     string
	Value    string
	Baseline string
	Delta    string
	Level    Level
}

// Health holds the evaluated health for all signals.
type Health struct {
	Overall Level
	Signals []SignalHealth
}
