package event

type Status string

const (
	StatusLost     Status = "lost"
	StatusPending  Status = "pending"
	StatusRunning  Status = "running"
	StatusComplete Status = "complete"
	StatusFailed   Status = "failed"
	StatusRetry    Status = "retry"
	StatusArchived Status = "archived"
)

func (s Status) String() string {
	return string(s)
}

func (s Status) IsTerminal() bool {
	return s == StatusComplete || s == StatusFailed || s == StatusArchived
}
