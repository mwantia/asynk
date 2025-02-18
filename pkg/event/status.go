package event

import "encoding/json"

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

type StatusEvent struct {
	ID       string          `json:"id"`
	Status   Status          `json:"status"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Metadata Metadata        `json:"metadata,omitempty"`
}

func (ev *StatusEvent) GetID() string {
	return ev.ID
}

func (ev *StatusEvent) Marshal() (json.RawMessage, error) {
	return json.Marshal(ev)
}

func (ev *StatusEvent) Unmarshal(data json.RawMessage) error {
	return json.Unmarshal(data, ev)
}
