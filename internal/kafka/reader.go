package kafka

import (
	"context"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/log"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	session *Session
	logger  log.LogWrapper
	reader  *kafka.Reader
}

func (r *Reader) ReadEvent(ctx context.Context, ev event.Event) error {
	r.logger.Info("Reading new kafka event...")

	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		err = fmt.Errorf("failed to read kafka message: %w", err)

		r.logger.Error("%v", err)
		return err
	}

	r.logger.Debug("New kafka event read with key '%s'", string(msg.Key))

	return ev.Unmarshal(msg.Value)
}
