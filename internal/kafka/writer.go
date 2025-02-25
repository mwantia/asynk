package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/log"
	"github.com/segmentio/kafka-go"
)

type Writer struct {
	writer *kafka.Writer
	logger log.LogWrapper
}

func (w *Writer) WriteEvent(ctx context.Context, ev event.Event) error {
	type Key struct {
		ID string `json:"id"`
	}

	w.logger.Info("Writing new kafka event...")

	key, err := json.Marshal(Key{ID: ev.GetID()})
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	value, err := ev.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	w.logger.Debug("New kafka event written with key '%s'", string(key))

	return w.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}
