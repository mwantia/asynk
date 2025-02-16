package server

import (
	"context"
	"fmt"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

type Worker struct {
	session *kafka.Session
}

func NewWorker(session *kafka.Session) (*Worker, error) {
	return &Worker{
		session: session,
	}, nil
}

func (w *Worker) Process(ctx context.Context, handler Handler) error {
	for {
		select {
		case <-ctx.Done():
		default:
			msg, err := w.session.ReadMessage(ctx, "events.submit")
			if err != nil {
				return fmt.Errorf("failed to read submit message: %w", err)
			}

			ev := &event.SubmitEvent{
				ID:       string(msg.Key),
				Payload:  msg.Value,
				Metadata: make(event.Metadata),
			}
			for _, header := range msg.Headers {
				switch header.Key {
				case "id":
					ev.ID = string(header.Value)
				case "type":
					ev.Type = event.EventType(header.Value)
				default:
					ev.Metadata[header.Key] = string(header.Value)
				}
			}

			if err := handler.ProcessSubmitEvent(ctx, nil, ev); err != nil {
				return fmt.Errorf("failed to process submit event: %w", err)
			}
		}
	}
}
