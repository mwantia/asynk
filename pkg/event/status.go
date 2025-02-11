package event

type EventStatus string

const (
	StatusLost     EventStatus = "lost"
	StatusPending  EventStatus = "pending"
	StatusRunning  EventStatus = "running"
	StatusComplete EventStatus = "complete"
	StatusFailed   EventStatus = "failed"
)

func (s EventStatus) String() string {
	return string(s)
}

func (s EventStatus) IsTerminal() bool {
	return s == StatusComplete || s == StatusFailed
}
