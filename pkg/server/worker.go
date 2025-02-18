package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/mwantia/asynk/internal/kafka"
)

type Worker struct {
	session *kafka.Session
	running sync.WaitGroup
	cancel  context.CancelFunc
}

func NewWorker(session *kafka.Session) (*Worker, error) {
	return &Worker{
		session: session,
	}, nil
}

func (w *Worker) Shutdown(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}

	done := make(chan struct{})
	go func() {
		w.running.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown timeout exceeded")
	}
}
