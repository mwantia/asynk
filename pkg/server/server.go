package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mwantia/asynk/pkg/kafka"
	"github.com/mwantia/asynk/pkg/options"
)

type Server struct {
	mutex  sync.RWMutex
	client *kafka.Client
}

func NewServer(opts ...options.ClientOption) (*Server, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Server{
		client: client,
	}, nil
}

func (s *Server) ServeMutex(ctx context.Context, mux *ServeMux) error {
	var wg sync.WaitGroup
	var errs []error

	for suffix, handler := range mux.handlers {

		session, err := s.client.Session(ctx, suffix)
		if err != nil {
			e := fmt.Errorf("failed to create session for suffix '%s': %w", suffix, err)
			errs = append(errs, e)
			continue
		}

		worker, err := NewWorker(session)
		if err != nil {
			e := fmt.Errorf("failed to create worker for suffix '%s': %w", suffix, err)
			errs = append(errs, e)
			continue
		}

		wg.Add(1)

		go func(w *Worker) {
			if err := w.Process(ctx, handler.handler); err != nil {
				e := fmt.Errorf("failed to process handler for suffix '%s': %w", suffix, err)
				errs = append(errs, e)
			}

			defer wg.Done()
		}(worker)
	}

	<-ctx.Done()
	wg.Wait()

	if err := s.client.Cleanup(); err != nil {
		e := fmt.Errorf("failed to perform client cleanup: %w", err)
		errs = append(errs, e)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
