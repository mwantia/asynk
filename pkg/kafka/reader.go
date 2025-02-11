package kafka

import (
	"context"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	client *Client
	reader *kafka.Reader
}

func (c *Client) NewReader(topic string) *Reader {
	return &Reader{
		client: c,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  c.options.Brokers,
			Topic:    c.fullTopic(topic),
			GroupID:  c.options.GroupID,
			MinBytes: c.options.MinBytes,
			MaxBytes: c.options.MaxBytes,
		}),
	}
}

func (r *Reader) ReadTask(ctx context.Context) (*event.TaskEvent, error) {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	te := &event.TaskEvent{
		ID:      string(msg.Key),
		Payload: msg.Value,
	}

	for _, header := range msg.Headers {
		switch header.Key {
		case "id":
			te.ID = string(header.Value)
		case "status":
			te.Status = event.EventStatus(header.Value)
		case "type":
			te.Type = event.EventType(header.Value)
		}
	}

	return te, nil
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
