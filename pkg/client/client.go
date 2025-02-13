package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
	"github.com/mwantia/asynk/pkg/options"
)

type Client struct {
	suffix  string
	client  *kafka.Client
	writer  *kafka.Writer
	reader  *kafka.Reader
	events  map[string]chan event.StatusEvent
	channel chan event.StatusEvent
	mutex   sync.RWMutex
}

func New(suffix string, opts ...options.ClientOption) (*Client, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	// Ensure topics exist
	if err := client.CreateTopic(ctx, suffix+".tasks.submit",
		options.WithRetentionTime(time.Hour*24*7),
	); err != nil {
		return nil, err
	}
	if err := client.CreateTopic(ctx, suffix+".tasks.status",
		options.WithRetentionTime(time.Hour*24),
	); err != nil {
		return nil, err
	}

	return &Client{
		suffix:  suffix,
		client:  client,
		writer:  client.NewWriter(suffix + ".tasks.submit"),
		reader:  client.NewReader(suffix + ".tasks.status"),
		events:  make(map[string]chan event.StatusEvent),
		channel: make(chan event.StatusEvent, 100),
	}, nil
}

func (c *Client) Submit(ctx context.Context, ev *event.SubmitEvent) (chan event.StatusEvent, error) {
	if err := c.writer.WriteSubmitEvent(ctx, ev); err != nil {
		return nil, fmt.Errorf("failed to submit task: %w", err)
	}

	c.mutex.Lock()
	channel := make(chan event.StatusEvent, 100)
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
	defer c.mutex.Unlock()

	for _, channel := range c.events {
		close(channel)
	}
	c.events = make(map[string]chan event.StatusEvent)

	return c.client.Cleanup()
}
