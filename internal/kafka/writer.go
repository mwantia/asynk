package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/segmentio/kafka-go"
)

type Writer struct {
	writer *kafka.Writer
}

func (w *Writer) WriteEvent(ctx context.Context, ev event.Event) error {
	type Key struct {
		ID string `json:"id"`
	}

	key, err := json.Marshal(Key{ID: ev.GetID()})
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	value, err := ev.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return w.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

func (w *Writer) _WriteSubmit(ctx context.Context, ev event.SubmitEvent) error {
	id := []byte(ev.ID)
	msg := kafka.Message{
		Key:   id,
		Value: ev.Payload,
		Headers: []kafka.Header{
			{
				Key:   "id",
				Value: id,
			},
			{
				Key:   "type",
				Value: []byte(ev.Type),
			},
		},
	}
	for k, v := range ev.Metadata {
		msg.Headers = append(msg.Headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	return w.writer.WriteMessages(ctx, msg)
}

func (w *Writer) _WriteStatus(ctx context.Context, ev event.StatusEvent) error {
	id := []byte(ev.ID)
	msg := kafka.Message{
		Key:   id,
		Value: ev.Payload,
		Headers: []kafka.Header{
			{
				Key:   "id",
				Value: id,
			},
			{
				Key:   "status",
				Value: []byte(ev.Status),
			},
		},
	}
	for k, v := range ev.Metadata {
		msg.Headers = append(msg.Headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	return w.writer.WriteMessages(ctx, msg)
}
