package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/asynk/pkg/options"
)

func (w *Worker) initializeTopic(ctx context.Context) error {
	w.logger.Info("Initializing topics for worker")

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

	w.logger.Info("Topics initialized successfully")
	return nil
}
