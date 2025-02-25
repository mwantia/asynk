package server

import (
	"context"
	"fmt"
)

func (s *Server) runWorker(ctx context.Context, suffix string, handler Handler) error {
	session, err := s.client.Session(suffix)
	if err != nil {
		return fmt.Errorf("failed to create session for suffix '%s': %w", suffix, err)
	}

	worker, err := NewWorker(s, session)
	if err != nil {
		return fmt.Errorf("failed to create worker for suffix '%s': %w", suffix, err)
	}

	s.mutex.Lock()
	s.workers[suffix] = worker
	s.mutex.Unlock()

	s.logger.Debug("Registering worker for suffix '%s'", suffix)

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("Worker panic for suffix '%s': %v", suffix, r)
				done <- fmt.Errorf("worker panic: %v", r)
			}
		}()

		if err := worker.Process(ctx, handler); err != nil {
			done <- err
		}
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("Context cancelled for worker '%s'", suffix)
		return ctx.Err()

	case err := <-done:
		s.mutex.Lock()
		delete(s.workers, suffix)
		s.mutex.Unlock()
		s.logger.Debug("Worker for suffix '%s' has exited", suffix)

		return err
	}
}
