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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.options.Brokers,
		Topic:          c.fullTopic(topic),
		GroupID:        c.options.GroupID,
		MaxWait:        c.options.MaxWait,
		CommitInterval: c.options.CommitInterval,
		MinBytes:       c.options.MinBytes,
		MaxBytes:       c.options.MaxBytes,
	})
	c.cleanups = append(c.cleanups, reader.Close)

	return &Reader{
		client: c,
		reader: reader,
	}
}

func (r *Reader) ReadSubmitEvent(ctx context.Context) (*event.TaskSubmitEvent, error) {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	ev := &event.TaskSubmitEvent{
		ID:       string(msg.Key),
		Payload:  msg.Value,
		Metadata: make(event.TaskMetadata),
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

func (r *Reader) ReadStatusEvent(ctx context.Context) (*event.TaskStatusEvent, error) {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	ev := &event.TaskStatusEvent{
		TaskID:   string(msg.Key),
		Status:   event.StatusLost,
		Payload:  msg.Value,
		Metadata: make(event.TaskMetadata),
	}

	for _, header := range msg.Headers {
		switch header.Key {
		case "id":
			ev.TaskID = string(header.Value)
		case "status":
			ev.Status = event.EventStatus(header.Value)
		default:
			ev.Metadata[header.Key] = string(header.Value)
		}
	}

	return ev, nil
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
