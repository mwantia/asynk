package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

type Client struct {
	writer  *kafka.Writer
	reader  *kafka.Reader
	streams map[string]chan event.TaskEvent
	channel chan event.TaskEvent
	mutex   sync.RWMutex
}

func New(opts ...kafka.Option) (*Client, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, err
	}
	// Ensure topics exist
	if err := client.CreateTopics(context.Background(), "tasks", "streams"); err != nil {
		return nil, err
	}

	writer := client.NewWriter("tasks")
	reader := client.NewReader("streams")

	return &Client{
		writer:  writer,
		reader:  reader,
		streams: make(map[string]chan event.TaskEvent),
		channel: make(chan event.TaskEvent, 100),
	}, nil
}

func (c *Client) Submit(ctx context.Context, te *event.TaskEvent) (chan event.TaskEvent, error) {
	if err := c.writer.WriteTask(ctx, te); err != nil {
		return nil, fmt.Errorf("failed to submit task: %w", err)
	}

	channel := make(chan event.TaskEvent, 100)
	c.mutex.Lock()
	c.streams[te.ID] = channel
	c.mutex.Unlock()

	go func() {
		defer func() {
			c.mutex.Lock()
			delete(c.streams, te.ID)
			close(channel)
			c.mutex.Unlock()
		}()

		/* for {
			var stream task.Task
			if err := c.reader.ReadJSON(ctx, &stream); err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			if stream.Task == task.ID {
				select {
				case channel <- stream:
					if stream.Status.IsTerminal() {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		} */
	}()

	return channel, nil
}

func (c *Client) Close() error {
	c.mutex.Lock()
	for _, channel := range c.streams {
		close(channel)
	}
	c.streams = make(map[string]chan event.TaskEvent)
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
