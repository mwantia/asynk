package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/options"
)

type Server struct {
	mutex   sync.RWMutex
	client  *kafka.Client
	workers map[string]*Worker
	active  atomic.Bool
}

func NewServer(opts ...options.ClientOption) (*Server, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Server{
		client:  client,
		workers: make(map[string]*Worker),
	}, nil
}

func (s *Server) ServeMutex(ctx context.Context, mux *ServeMux) error {
	if !s.active.CompareAndSwap(false, true) {
		return fmt.Errorf("server is already running")
	}
	defer s.active.Store(false)

	var wg sync.WaitGroup
	errs := &Errors{}

	for suffix, handler := range mux.handlers {
		wg.Add(1)

		go func(suffix string, handler Handler) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := s.runWorker(ctx, suffix, handler); err != nil {
						log.Printf("%v", err)
						time.Sleep(time.Second * 10)
						continue
					}
				}
			}
		}(suffix, handler.handler)
	}

	<-ctx.Done()

	s.mutex.Lock()
	for _, worker := range s.workers {
		if err := worker.Shutdown(ctx); err != nil {
			errs.Add(fmt.Errorf("failed to shutdown worker: %w", err))
		}
	}
	// Reset list of running workers
	s.workers = make(map[string]*Worker)
	s.mutex.Unlock()

	wg.Wait()

	if err := s.client.Cleanup(); err != nil {
		errs.Add(fmt.Errorf("failed to perform client cleanup: %w", err))
	}

	return errs.Errors()
}
