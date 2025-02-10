package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Reader struct {
	reader *kafka.Reader
}

func NewReader(brokers []string, topic, group string) (*Reader, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	return &Reader{
		reader: reader,
	}, nil
}

func (r *Reader) Read(ctx context.Context) ([]byte, error) {
	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	return msg.Value, nil
}

func (r *Reader) ReadJSON(ctx context.Context, v interface{}) error {
	data, err := r.Read(ctx)
	if err != nil {
		return err
	}

	// log.Println(string(data))

	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
