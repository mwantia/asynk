package kafka

import (
	"context"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	reader *kafka.Reader
}

func (r *Reader) ReadEvent(ctx context.Context, ev event.Event) error {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to read kafka message: %w", err)
	}

	return ev.Unmarshal(msg.Value)
}

func (r *Reader) _ReadSubmit(ctx context.Context) (event.SubmitEvent, error) {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return event.SubmitEvent{}, fmt.Errorf("failed to read kafka message: %w", err)
	}

	ev := event.SubmitEvent{
		ID:       string(msg.Key),
		Payload:  msg.Value,
		Metadata: make(event.Metadata),
	}
	for _, header := range msg.Headers {
		switch header.Key {
		case "id":
			ev.ID = string(header.Value)
		case "type":
			ev.Type = event.EventType(header.Value)
		default:
			ev.Metadata[header.Key] = string(header.Value)
		}
	}

	return ev, nil
}

func (r *Reader) _ReadStatus(ctx context.Context) (event.StatusEvent, error) {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return event.StatusEvent{}, fmt.Errorf("failed to read kafka message: %w", err)
	}

	ev := event.StatusEvent{
		ID:      string(msg.Key),
		Status:  event.StatusLost,
		Payload: msg.Value,
	}
	for _, header := range msg.Headers {
		switch header.Key {
		case "id":
			ev.ID = string(header.Value)
		case "status":
			ev.Status = event.Status(header.Value)
		default:
			ev.Metadata[header.Key] = string(header.Value)
		}
	}

	return ev, nil
}
