package worker

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/mwantia/asynk/pkg/kafka"
	"github.com/mwantia/asynk/pkg/options"
)

type Worker struct {
	mutex  sync.RWMutex
	client *kafka.Client
}

func New(opts ...options.ClientOption) (*Worker, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, err
	}

	return &Worker{
		client: client,
	}, nil
}

func (w *Worker) Run(ctx context.Context, mux *ServeMux) error {
	var wg sync.WaitGroup
	// cleanups := []func() error{}
	var errs []error

	for suffix, handler := range mux.handlers {
		// Ensure that the topics have been created beforehand
		if err := w.client.CreateTopic(ctx, suffix+".tasks.submit",
			options.WithRetentionTime(time.Hour*24*7),
		); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := w.client.CreateTopic(ctx, suffix+".tasks.status",
			options.WithRetentionTime(time.Hour*24),
		); err != nil {
			errs = append(errs, err)
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			r := w.client.NewReader(suffix + ".tasks.submit")
			// cleanups = append(cleanups, r.Close)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					ev, err := r.ReadSubmitEvent(ctx)
					if err != nil {
						log.Println(err)
						continue
					}

					if err := handler.handler.ProcessSubmitEvent(ctx, w.client, ev); err != nil {
						log.Println(err)
						continue
					}
				}
			}
		}()
	}

	wg.Wait()

	/* for _, cleanup := range cleanups {
		if err := cleanup(); err != nil {
			errs = append(errs, err)
		}
	} */

	if err := w.client.Cleanup(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
