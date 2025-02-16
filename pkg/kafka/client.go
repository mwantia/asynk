package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mwantia/asynk/pkg/options"
	"github.com/segmentio/kafka-go"
)

type Client struct {
	mutex    sync.RWMutex
	options  options.ClientOptions
	conn     *kafka.Conn
	cleanups []func() error
}

func New(opts ...options.ClientOption) (*Client, error) {
	options := options.DefaultClientOptions()
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	return &Client{
		options: options,
	}, nil
}

func (c *Client) Session(ctx context.Context, suffix string, opts ...options.TopicOption) (*Session, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := c.CreateTopic(ctx, suffix, opts...); err != nil {
		return nil, err
	}

	return &Session{
		suffix:  suffix,
		client:  c,
		options: opts,
	}, nil
}

func (c *Client) Cleanup() error {
	if len(c.cleanups) == 0 {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	var errs []error

	for _, cleanup := range c.cleanups {
		if err := cleanup(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (c *Client) Marshal() ([]byte, error) {
	return json.Marshal(c.options)
}

func (c *Client) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &c.options)
}

func (c *Client) dial(ctx context.Context) (*kafka.Conn, error) {
	if c.conn == nil {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		conn, err := kafka.DialContext(ctx, c.options.Network, c.options.Brokers[0])
		if err != nil {
			return nil, err
		}

		c.cleanups = append(c.cleanups, conn.Close)
		c.conn = conn
	}

	return c.conn, nil
}

func (c *Client) fullTopic(topic string) string {
	var text strings.Builder
	if c.options.TopicPrefix != "" {
		text.WriteString(c.options.TopicPrefix + ".")
	}
	if c.options.Pool != "" {
		text.WriteString(c.options.Pool + ".")
	}

	text.WriteString(topic)
	return text.String()
}

func (c *Client) CreateTopic(ctx context.Context, topic string, opts ...options.TopicOption) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return fmt.Errorf("error during dial: %w", err)
	}

	options := options.DefaultTopicOptions()
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return err
		}
	}

	config := kafka.TopicConfig{
		Topic:             c.fullTopic(topic),
		NumPartitions:     options.NumPartitions,
		ReplicationFactor: options.ReplicationFactor,
		ConfigEntries: []kafka.ConfigEntry{
			{
				ConfigName:  "retention.ms",
				ConfigValue: fmt.Sprintf("%d", options.RetentionTime.Milliseconds()),
			},
			{
				ConfigName:  "retention.bytes",
				ConfigValue: fmt.Sprintf("%d", options.RetentionBytes),
			},
		},
	}

	return conn.CreateTopics(config)
}
