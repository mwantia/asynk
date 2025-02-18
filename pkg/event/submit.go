package event

import "encoding/json"

type EventType string

const (
	TypeSubmit EventType = "submit"
)

func (s EventType) String() string {
	return string(s)
}

type SubmitEvent struct {
	ID       string          `json:"id"`
	Type     EventType       `json:"type,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Metadata Metadata        `json:"metadata,omitempty"`
}

func (ev *SubmitEvent) GetID() string {
	return ev.ID
}

func (ev *SubmitEvent) Marshal() (json.RawMessage, error) {
	return json.Marshal(ev)
}

func (ev *SubmitEvent) Unmarshal(data json.RawMessage) error {
	return json.Unmarshal(data, ev)
}
