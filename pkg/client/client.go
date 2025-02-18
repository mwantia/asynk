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
	mutex   sync.RWMutex
}

func NewClient(suffix string, opts ...options.ClientOption) (*Client, error) {
	client, err := kafka.New(opts...)
	if err != nil {
		return nil, err
	}

	session, err := client.Session(suffix)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka session: %w", err)
	}

	// I don't think clients should share the responsibility of creating topics

	/* if err := session.CreateTopic(ctx, "events.submit",
		options.WithRetentionTime(time.Hour*24),
	); err != nil {
		return nil, err
	}
	if err := session.CreateTopic(ctx, "events.status",
		options.WithRetentionTime(time.Hour*2),
	); err != nil {
		return nil, err
	} */

	return &Client{
		suffix:  suffix,
		client:  client,
		session: session,
		events:  make(map[string]chan *event.StatusEvent),
	}, nil
}

func (c *Client) Archive(ctx context.Context, ev *event.SubmitEvent, reason string) error {
	return nil
}

func (c *Client) Submit(ctx context.Context, ev event.SubmitEvent) (chan *event.StatusEvent, error) {
	if ev.ID == "" {
		ev.ID = event.UUIDv7()
	}

	writer, err := c.session.GetWriter("events.submit")
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka writer: %w", err)
	}

	if err := writer.WriteEvent(ctx, &ev); err != nil {
		return nil, fmt.Errorf("failed to write kafka message: %w", err)
	}

	reader, err := c.session.GetReader("events.status")
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka reader: %w", err)
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
			select {
			case <-ctx.Done():
				return
			default:
				evs := &event.StatusEvent{}
				if err := reader.ReadEvent(ctx, evs); err != nil {
					if ctx.Err() != nil {
						return
					}

					time.Sleep(time.Second * 2)
					continue
				}

				if evs.ID == ev.ID {
					channel <- evs
					if evs.Status.IsTerminal() {
						return
					}
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
	c.events = make(map[string]chan *event.StatusEvent)
	c.mutex.Unlock()

	return c.client.Cleanup()
}
