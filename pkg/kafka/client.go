package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Client struct {
	mutex    sync.RWMutex
	options  Options
	conn     *kafka.Conn
	cleanups []func() error
}

func New(opts ...Option) (*Client, error) {
	options := DefaultOptions()
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	return &Client{
		options: options,
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

func (c *Client) CreateTopics(ctx context.Context, topics ...string) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return fmt.Errorf("error during dial: %w", err)
	}
	defer conn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{
			Topic:             c.fullTopic(topic),
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "604800000",
				},
			},
		})
	}

	return conn.CreateTopics(configs...)
}
