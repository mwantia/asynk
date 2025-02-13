package event

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

type SubmitEvent struct {
	ID       string          `json:"id"`
	Type     EventType       `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Metadata Metadata        `json:"metadata,omitempty"`
}

type StatusEvent struct {
	TaskID   string          `json:"task_id"`
	Status   Status          `json:"status"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Metadata Metadata        `json:"metadata,omitempty"`
}

type Metadata map[string]string

func NewSubmitEvent(v interface{}) (*SubmitEvent, error) {
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

	return &SubmitEvent{
		ID:       fmt.Sprintf("%x", buf),
		Type:     TypeSubmit,
		Payload:  payload,
		Metadata: make(Metadata),
	}, nil
}
