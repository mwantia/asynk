package worker

import (
	"context"

	"github.com/mwantia/asynk/pkg/shared"
)

type Handler interface {
	ProcessTask(context.Context, *shared.Task) error
}

type HandlerFunc func(context.Context, *shared.Task) error

func (fn HandlerFunc) ProcessTask(ctx context.Context, task *shared.Task) error {
	return fn(ctx, task)
}
