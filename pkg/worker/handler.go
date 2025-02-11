package worker

import (
	"context"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

type Handler interface {
	ProcessTask(context.Context, *kafka.Client, *event.TaskEvent) error
}

type HandlerFunc func(context.Context, *kafka.Client, *event.TaskEvent) error

func (fn HandlerFunc) ProcessTask(ctx context.Context, c *kafka.Client, t *event.TaskEvent) error {
	return fn(ctx, c, t)
}
