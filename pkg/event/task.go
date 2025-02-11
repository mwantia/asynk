package event

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

type TaskEvent struct {
	ID      string          `json:"id"`
	Status  EventStatus     `json:"status"`
	Type    EventType       `json:"type,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewTaskEvent(opts ...TaskOption) (*TaskEvent, error) {
	ev := TaskEvent{}
	for _, opt := range opts {
		if err := opt(&ev); err != nil {
			return &ev, err
		}
	}

	return &ev, nil
}

type TaskOption func(*TaskEvent) error

func WithKey(key []byte) TaskOption {
	return func(te *TaskEvent) error {
		te.ID = string(key)
		return nil
	}
}

func WithUUIDv7() TaskOption {
	return func(t *TaskEvent) error {
		var buf [16]byte
		_, err := rand.Read(buf[:])
		if err != nil {
			return err
		}

		ts := big.NewInt(time.Now().UnixMilli())
		ts.FillBytes(buf[0:6])

		buf[6] = (buf[6] & 0x0F) | 0x70
		buf[8] = (buf[8] & 0x3F) | 0x80

		t.ID = fmt.Sprintf("%x", buf)
		return nil
	}
}

func WithPayload(payload []byte) TaskOption {
	return func(te *TaskEvent) error {
		te.Payload = payload
		return nil
	}
}

func WithMarshal(v interface{}) TaskOption {
	return func(t *TaskEvent) error {
		payload, err := json.Marshal(v)
		t.Payload = payload
		return err
	}
}
