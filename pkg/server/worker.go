package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/log"
)

type Worker struct {
	logger  log.LogWrapper
	session *kafka.Session

	running sync.WaitGroup
	cancel  context.CancelFunc
}

func NewWorker(server *Server, session *kafka.Session) (*Worker, error) {
	return &Worker{
		logger:  server.logger.Named("asynk/worker"),
		session: session,
	}, nil
}

func (w *Worker) Shutdown(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}

	w.logger.Info("Starting worker shutdown...")

	done := make(chan struct{})
	go func() {
		w.running.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("Worker shutdown complete")
		return nil

	case <-ctx.Done():
		w.logger.Warn("Worker shutdown timeout exceeded")
		return fmt.Errorf("shutdown timeout exceeded")
	}
}
