package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

type Client struct {
	options Options
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

func (c *Client) Marshal() ([]byte, error) {
	return json.Marshal(c.options)
}

func (c *Client) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &c.options)
}

func (c *Client) dial(ctx context.Context) (*kafka.Conn, error) {
	return kafka.DialContext(ctx, c.options.Network, c.options.Brokers[0])
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
