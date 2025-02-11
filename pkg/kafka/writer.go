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

func (w *Writer) WriteTask(ctx context.Context, te *event.TaskEvent) error {
	msg := kafka.Message{
		Key: []byte(te.ID),
		Headers: []kafka.Header{
			{
				Key:   "id",
				Value: []byte(te.ID),
			},
			{
				Key:   "status",
				Value: []byte(te.Status),
			},
			{
				Key:   "type",
				Value: []byte(te.Type),
			},
		},
		Value: te.Payload,
	}

	if err := w.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (w *Writer) Close() error {
	return w.writer.Close()
}
