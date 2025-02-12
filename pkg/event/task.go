package event

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

type TaskSubmitEvent struct {
	ID       string          `json:"id"`
	Type     EventType       `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Metadata TaskMetadata    `json:"metadata,omitempty"`
}

type TaskStatusEvent struct {
	TaskID   string          `json:"task_id"`
	Status   EventStatus     `json:"status"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Metadata TaskMetadata    `json:"metadata,omitempty"`
}

type TaskMetadata map[string]string

func NewSubmitEvent(v interface{}) (*TaskSubmitEvent, error) {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		return nil, err
	}

	ts := big.NewInt(time.Now().UnixMilli())
	ts.FillBytes(buf[0:6])

	buf[6] = (buf[6] & 0x0F) | 0x70
	buf[8] = (buf[8] & 0x3F) | 0x80

	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return &TaskSubmitEvent{
		ID:       fmt.Sprintf("%x", buf),
		Type:     TypeSubmit,
		Payload:  payload,
		Metadata: make(TaskMetadata),
	}, nil
}
