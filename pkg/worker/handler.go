package worker

import (
	"context"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

type Handler interface {
	ProcessSubmitEvent(context.Context, *kafka.Client, *event.SubmitEvent) error
}

type HandlerFunc func(context.Context, *kafka.Client, *event.SubmitEvent) error

func (fn HandlerFunc) ProcessSubmitEvent(ctx context.Context, c *kafka.Client, t *event.SubmitEvent) error {
	return fn(ctx, c, t)
}
