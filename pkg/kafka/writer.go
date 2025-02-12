package kafka

import (
	"context"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/segmentio/kafka-go"
)

type Writer struct {
	client *Client
	writer *kafka.Writer
}

func (c *Client) NewWriter(topic string) *Writer {
	if c.options.Network != "tcp" {
		panic("not supported")
	}

	return &Writer{
		client: c,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(c.options.Brokers...),
			Topic:    c.fullTopic(topic),
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (w *Writer) WriteSubmitEvent(ctx context.Context, ev *event.TaskSubmitEvent) error {
	msg := kafka.Message{
		Key:   []byte(ev.ID),
		Value: ev.Payload,
		Headers: []kafka.Header{
			{
				Key:   "id",
				Value: []byte(ev.ID),
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

	if err := w.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (w *Writer) WriteStatusEvent(ctx context.Context, ev *event.TaskStatusEvent) error {
	msg := kafka.Message{
		Key:   []byte(ev.TaskID),
		Value: ev.Payload,
		Headers: []kafka.Header{
			{
				Key:   "id",
				Value: []byte(ev.TaskID),
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

	if err := w.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (w *Writer) Close() error {
	return w.writer.Close()
}
