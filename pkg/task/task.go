package task

import (
	"encoding/json"
	"fmt"
	"time"
)

type Task struct {
	ID        string          `json:"id"`
	Pool      string          `json:"pool"`
	Type      Type            `json:"type"`
	Status    Status          `json:"status"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	CreatedAt time.Time       `json:"created_at,omitempty"`
}

func NewTask(pool string, payload interface{}) (*Task, error) {
	buf, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	id, err := GenUUIDv7()
	if err != nil {
		return nil, fmt.Errorf("failed to generate id: %w", err)
	}

	return &Task{
		ID:        id,
		Pool:      pool,
		Type:      TypeTask,
		Status:    StatusPending,
		Payload:   buf,
		CreatedAt: time.Now(),
	}, nil
}
