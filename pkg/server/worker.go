package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/options"
)

type Worker struct {
	session *kafka.Session
}

func NewWorker(session *kafka.Session) (*Worker, error) {
	return &Worker{
		session: session,
	}, nil
}

func (w *Worker) Process(ctx context.Context, handler Handler) error {
	if err := w.session.CreateTopic(ctx, "events.submit",
		options.WithRetentionTime(time.Hour*24),
	); err != nil {
		return fmt.Errorf("failed to create topic '%s': %w", "events.submit", err)
	}

	if err := w.session.CreateTopic(ctx, "events.status",
		options.WithRetentionTime(time.Hour*2),
	); err != nil {
		return fmt.Errorf("failed to create topic '%s': %w", "events.status", err)
	}

	reader, err := w.session.GetReader("events.submit")
	if err != nil {
		return fmt.Errorf("failed to get kafka reader: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
		default:
			ev := &event.SubmitEvent{}
			if err := reader.ReadEvent(ctx, ev); err != nil {
				return fmt.Errorf("failed to read kafka message: %w", err)
			}

			pipeline := &Pipeline{
				session: w.session,
				submit:  ev,
			}

			if err := handler.ProcessPipeline(ctx, pipeline); err != nil {
				return fmt.Errorf("failed to process submit event: %w", err)
			}
		}
	}
}
