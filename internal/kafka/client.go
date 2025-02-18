package kafka

import (
	"context"
	"encoding/json"
	"errors"
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

func (c *Client) Session(ctx context.Context, suffix string) (*Session, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return &Session{
		suffix:   suffix,
		client:   c,
		readers:  make(map[string]*Reader),
		writers:  make(map[string]*Writer),
		cleanups: make([]func() error, 0),
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
