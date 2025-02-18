package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/event"
)

func (w *Worker) processPipeline(ctx context.Context, p *Pipeline, h Handler) error {
	process, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	if err := h.ProcessPipeline(process, p); err != nil {
		errs := p.Status(ctx, &event.StatusEvent{
			Status: event.StatusFailed,
			Metadata: event.Metadata{
				event.MetadataLastError:   err.Error(),
				event.MetadataLastAttempt: time.Now().Format(time.RFC3339),
			},
		})
		if errs != nil {
			return fmt.Errorf("failed to update status after error: %v (original error: %w)", errs, err)
		}
		return fmt.Errorf("failed to process submit event: %w", err)
	}

	return nil
}

func (w *Worker) processNextEvent(ctx context.Context, reader *kafka.Reader, h Handler) error {
	timeout, cancel := context.WithTimeout(ctx, time.Minute*1)
	defer cancel()

	ev := &event.SubmitEvent{}
	if err := reader.ReadEvent(ctx, ev); err != nil {
		if timeout.Err() != nil {
			return nil
		}
		return fmt.Errorf("failed to read kafka message: %w", err)
	}

	return w.processPipeline(ctx, &Pipeline{
		session: w.session,
		submit:  ev,
	}, h)
}

func (w *Worker) Process(ctx context.Context, handler Handler) error {
	processing, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	if err := w.initializeTopic(ctx); err != nil {
		return fmt.Errorf("failed to initialize topics: %w", err)
	}

	reader, err := w.session.GetReader("events.submit")
	if err != nil {
		return fmt.Errorf("failed to get kafka reader: %w", err)
	}

	w.running.Add(1)
	defer w.running.Done()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := w.processNextEvent(processing, reader, handler); err != nil {
				fmt.Printf("Error processing event: %v\n", err)
				continue
			}
		}
	}
}
