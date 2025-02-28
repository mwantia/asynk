package server

import (
	"context"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/log"
)

type Pipeline struct {
	logger  log.LogWrapper
	session *kafka.Session
	submit  *event.SubmitEvent
}

func (p *Pipeline) Submit() *event.SubmitEvent {
	return p.submit // Simply return the privately stored submit event
}

func (p *Pipeline) Status(ctx context.Context, ev *event.StatusEvent) error {
	p.logger.Debug("Updating status for task '%s' to '%s'", p.submit.ID, ev.Status)

	writer := p.session.GetWriter("events.status")

	if ev.Time.IsZero() {
		now := time.Now()
		p.logger.Debug("Time not set; Setting time with '%v'", now)
		ev.Time = now
	}

	if ev.ID == "" {
		p.logger.Debug("ID not set; Using ID from submit event '%s'", p.submit.ID)
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
