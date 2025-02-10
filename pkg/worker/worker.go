package worker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/options"
	"github.com/mwantia/asynk/pkg/shared"
)

type Worker struct {
	mutex   sync.RWMutex
	options options.Options
}

func NewWorker(opts ...options.Option) (*Worker, error) {
	options := options.DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	return &Worker{
		options: options,
	}, nil
}

func (w *Worker) Run(ctx context.Context, mux *ServeMux) error {
	var wg sync.WaitGroup
	cleanups := []func() error{}
	var errs []error

	for suffix, handler := range mux.handlers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			topic := fmt.Sprintf("%s.%s.%s", w.options.Prefix, w.options.Pool, suffix)
			r, err := kafka.NewReader(w.options.Brokers, topic, w.options.GroupID)
			if err != nil {
				errs = append(errs, err)
				return
			}

			cleanups = append(cleanups, r.Close)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					var task shared.Task
					if err := r.ReadJSON(ctx, &task); err != nil {
						log.Println(err)
						continue
					}

					if err := handler.handler.ProcessTask(ctx, &task); err != nil {
						log.Println(err)
						continue
					}
				}
			}
		}()
	}

	<-ctx.Done()

	for _, cleanup := range cleanups {
		if err := cleanup(); err != nil {
			errs = append(errs, err)
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("errors during cleanup: %v", errs)
	}

	return nil
}

func (w *Worker) RunHandler(ctx context.Context, topic string, handler Handler) error {
	mux := NewServeMux()
	if err := mux.Handle(topic, handler); err != nil {
		return fmt.Errorf("failed to create mux: %w", err)
	}

	return w.Run(ctx, mux)
}

func (w *Worker) RunHandlerFunc(ctx context.Context, topic string, fn HandlerFunc) error {
	mux := NewServeMux()
	if err := mux.HandleFunc(topic, fn); err != nil {
		return fmt.Errorf("failed to create mux: %w", err)
	}

	return w.Run(ctx, mux)
}
