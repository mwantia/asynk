package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/mwantia/asynk/internal/kafka"
	"github.com/mwantia/asynk/pkg/options"
	"github.com/mwantia/asynk/pkg/shared"
)

type Client struct {
	writer  *kafka.Writer
	reader  *kafka.Reader
	streams map[string]chan shared.Stream
	channel chan shared.Stream
	mutex   sync.RWMutex
	options options.Options
}

func New(opts ...options.Option) (*Client, error) {
	options := options.DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	admin, err := kafka.NewAdmin(options.Brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka connection: %w", err)
	}
	defer admin.Close()

	// Ensure topics exist
	admin.CreateTopics(
		fmt.Sprintf("%s_tasks", options.Prefix),
		fmt.Sprintf("%s_streams", options.Prefix),
	)

	writer, err := kafka.NewWriter(options.Brokers, fmt.Sprintf("%s_tasks", options.Prefix))
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	reader, err := kafka.NewReader(options.Brokers, fmt.Sprintf("%s_streams", options.Prefix), "stream-group")
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return &Client{
		writer:  writer,
		reader:  reader,
		streams: make(map[string]chan shared.Stream),
		channel: make(chan shared.Stream, options.StreamBufferSize),
		options: options,
	}, nil
}

func (c *Client) Submit(ctx context.Context, task *shared.Task) (chan shared.Stream, error) {
	if err := c.writer.WriteJSON(ctx, task.ID, task); err != nil {
		return nil, fmt.Errorf("failed to submit task: %w", err)
	}

	channel := make(chan shared.Stream, c.options.StreamBufferSize)
	c.mutex.Lock()
	c.streams[task.ID] = channel
	c.mutex.Unlock()

	go func() {
		defer func() {
			c.mutex.Lock()
			delete(c.streams, task.ID)
			close(channel)
			c.mutex.Unlock()
		}()

		for {
			var stream shared.Stream
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
		}
	}()

	return channel, nil
}

func (c *Client) Close() error {
	c.mutex.Lock()
	for _, channel := range c.streams {
		close(channel)
	}
	c.streams = make(map[string]chan shared.Stream)
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
