package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

type Client struct {
	suffix  string
	writer  *kafka.Writer
	reader  *kafka.Reader
	events  map[string]chan event.TaskStatusEvent
	channel chan event.TaskStatusEvent
	mutex   sync.RWMutex
}

func New(suffix string, opts ...kafka.Option) (*Client, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, err
	}
	// Ensure topics exist
	if err := client.CreateTopics(context.Background(), suffix+".tasks.submit", suffix+".tasks.status"); err != nil {
		return nil, err
	}

	return &Client{
		suffix:  suffix,
		writer:  client.NewWriter(suffix + ".tasks.submit"),
		reader:  client.NewReader(suffix + ".tasks.status"),
		events:  make(map[string]chan event.TaskStatusEvent),
		channel: make(chan event.TaskStatusEvent, 100),
	}, nil
}

func (c *Client) Submit(ctx context.Context, ev *event.TaskSubmitEvent) (chan event.TaskStatusEvent, error) {
	if err := c.writer.WriteSubmitEvent(ctx, ev); err != nil {
		return nil, fmt.Errorf("failed to submit task: %w", err)
	}

	channel := make(chan event.TaskStatusEvent, 100)
	c.mutex.Lock()
	c.events[ev.ID] = channel
	c.mutex.Unlock()

	go func() {
		defer func() {
			c.mutex.Lock()
			delete(c.events, ev.ID)
			close(channel)
			c.mutex.Unlock()
		}()

		for {
			evs, err := c.reader.ReadStatusEvent(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			if evs.TaskID == ev.ID {
				select {
				case channel <- *evs:
					if evs.Status.IsTerminal() {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return channel, nil
}

func (c *Client) Close() error {
	c.mutex.Lock()
	for _, channel := range c.events {
		close(channel)
	}
	c.events = make(map[string]chan event.TaskStatusEvent)
	c.mutex.Unlock()

	var errs []error
	if err := c.writer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close writer: %w", err))
	}
	if err := c.reader.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close reader: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}

	return nil
}
