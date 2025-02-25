package client

import (
	"context"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/event"
)

func (c *Client) processEvent(ctx context.Context, id string, reader *kafka.Reader, ch chan *event.StatusEvent) {
	lctx, cancel := context.WithCancel(c.ctx)

	defer c.wait.Done()
	defer cancel()

	defer func() {
		c.mutex.Lock()
		if _, exist := c.events[id]; exist {
			delete(c.events, id)
			close(ch)
		}
		c.mutex.Unlock()
	}()

	for {
		select {
		case <-lctx.Done():
			c.logger.Debug("Kafka reader for task '%s' was cancelled", id)
			return

		case <-ctx.Done():
			c.logger.Debug("Context for task '%s' was cancelled", id)
			return

		default:
			evs := &event.StatusEvent{}
			if err := reader.ReadEvent(ctx, evs); err != nil {
				if ctx.Err() != nil {
					return
				}

				c.logger.Warn("Error reading status event: %v", err)
				select {
				case <-lctx.Done():
					return

				case <-ctx.Done():
					return

				case <-time.After(time.Second * 2):
					// Continue after a short delay
				}
				continue
			}

			if evs.ID == id {
				select {
				case ch <- evs:
					// Channel send successfully

				case <-lctx.Done():
					c.logger.Debug("Kafka reader for task '%s' was cancelled", id)
					return

				case <-ctx.Done():
					c.logger.Debug("Context for task '%s' was cancelled", id)
					return
				}

				if evs.Status.IsTerminal() {
					c.logger.Debug("Task '%s' has reached terminal status", id)
					return
				}
			}
		}
	}
}
