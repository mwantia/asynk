package worker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/mwantia/asynk/pkg/kafka"
)

type Worker struct {
	mutex  sync.RWMutex
	client *kafka.Client
}

func New(opts ...kafka.Option) (*Worker, error) {
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
	cleanups := []func() error{}
	var errs []error

	for suffix, handler := range mux.handlers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			r := w.client.NewReader(suffix)
			cleanups = append(cleanups, r.Close)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					task, err := r.ReadTask(ctx)
					if err != nil {
						log.Println(err)
						continue
					}

					if err := handler.handler.ProcessTask(ctx, w.client, task); err != nil {
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
