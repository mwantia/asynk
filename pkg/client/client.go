package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/options"
)

type Client struct {
	suffix  string
	client  *kafka.Client
	session *kafka.Session
	events  map[string]chan *event.StatusEvent
	channel chan event.StatusEvent
	mutex   sync.RWMutex
}

func New(suffix string, opts ...options.ClientOption) (*Client, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	session, err := client.Session(ctx, suffix)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka session: %w", err)
	}

	// Ensure topics exist
	if err := session.CreateTopic(ctx, "events.submit",
		options.WithRetentionTime(time.Hour*24),
	); err != nil {
		return nil, err
	}
	if err := session.CreateTopic(ctx, "events.status",
		options.WithRetentionTime(time.Hour*2),
	); err != nil {
		return nil, err
	}

	return &Client{
		suffix:  suffix,
		client:  client,
		session: session,
		events:  make(map[string]chan *event.StatusEvent),
		channel: make(chan event.StatusEvent, 100),
	}, nil
}

func (c *Client) Archive(ctx context.Context, ev *event.SubmitEvent, reason string) error {
	return nil
}

func (c *Client) Submit(ctx context.Context, ev event.SubmitEvent) (chan *event.StatusEvent, error) {
	writer, err := c.session.GetWriter("events.submit")
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka writer: %w", err)
	}

	reader, err := c.session.GetReader("events.status")
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka reader: %w", err)
	}

	if err := writer.WriteEvent(ctx, &ev); err != nil {
		return nil, fmt.Errorf("failed to write kafka message: %w", err)
	}

	c.mutex.Lock()
	channel := make(chan *event.StatusEvent, 100)
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
			evs := &event.StatusEvent{}
			if err := reader.ReadEvent(ctx, evs); err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			if evs.ID == ev.ID {
				select {
				case channel <- evs:
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
	c.events = make(map[string]chan *event.StatusEvent)

	return c.client.Cleanup()
}
