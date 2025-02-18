package server

import (
	"context"
	"fmt"
)

func (s *Server) runWorker(ctx context.Context, suffix string, handler Handler) error {
	session, err := s.client.Session(ctx, suffix)
	if err != nil {
		return fmt.Errorf("failed to create session for suffix '%s': %w", suffix, err)
	}

	worker, err := NewWorker(session)
	if err != nil {
		return fmt.Errorf("failed to create worker for suffix '%s': %w", suffix, err)
	}

	s.mutex.Lock()
	s.workers[suffix] = worker
	s.mutex.Unlock()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("worker panic: %v", r)
			}
		}()

		if err := worker.Process(ctx, handler); err != nil {
			done <- err
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		s.mutex.Lock()
		delete(s.workers, suffix)
		s.mutex.Unlock()
		return err
	}
}
