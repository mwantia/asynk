package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/log"
	"github.com/segmentio/kafka-go"
)

type Writer struct {
	session *Session
	logger  log.LogWrapper
	writer  *kafka.Writer
}

func (w *Writer) WriteEvent(ctx context.Context, ev event.Event) error {
	w.logger.Info("Writing new kafka event...")

	key := ev.GetID()
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	value, err := ev.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	w.logger.Debug("New kafka event written with key '%s'", key)

	return w.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Headers: []kafka.Header{
			{
				Key:   "session_id",
				Value: []byte(w.session.ID),
			},
			{
				Key:   "timestamp",
				Value: []byte(timestamp),
			},
		},
	})
}
