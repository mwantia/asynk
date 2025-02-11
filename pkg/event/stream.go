package event

import "encoding/json"

type StreamEvent struct {
	Task    string          `json:"task"`
	Status  EventStatus     `json:"status"`
	Type    EventType       `json:"type,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewStreamEvent(opts ...StreamOption) (*StreamEvent, error) {
	ev := StreamEvent{}
	for _, opt := range opts {
		if err := opt(&ev); err != nil {
			return &ev, err
		}
	}

	return &ev, nil
}

type StreamOption func(*StreamEvent) error
