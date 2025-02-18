package server

import (
	"context"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/event"
)

type Pipeline struct {
	session *kafka.Session
	submit  *event.SubmitEvent
}

func (p *Pipeline) Submit() *event.SubmitEvent {
	return p.submit // Simply return the privately stored submit event
}

func (p *Pipeline) Status(ctx context.Context, ev *event.StatusEvent) error {
	writer, err := p.session.GetWriter("events.status")
	if err != nil {
		return err
	}

	if ev.ID == "" {
		ev.ID = p.submit.ID
	}

	return writer.WriteEvent(ctx, ev)
}

func (p *Pipeline) Done(ctx context.Context, s event.Status) error {
	return p.Status(ctx, &event.StatusEvent{
		ID:     p.submit.ID,
		Status: s,
	})
}
