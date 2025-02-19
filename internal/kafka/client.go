package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/mwantia/asynk/pkg/log"
	"github.com/mwantia/asynk/pkg/options"
	"github.com/segmentio/kafka-go"
)

type Client struct {
	mutex    sync.RWMutex
	logger   log.LogWrapper
	options  options.ClientOptions
	conn     *kafka.Conn
	cleanups []func() error
}

func NewKafka(options options.ClientOptions, logger log.LogWrapper) (*Client, error) {
	return &Client{
		logger:  logger.Named("kafka/client"),
		options: options,
	}, nil
}

func (c *Client) Session(suffix string) (*Session, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.logger.Info("Creating new session with suffix '%s'", suffix)

	return &Session{
		suffix:   suffix,
		client:   c,
		logger:   c.logger.Named("kafka/session"),
		readers:  make(map[string]*Reader),
		writers:  make(map[string]*Writer),
		cleanups: make([]func() error, 0),
	}, nil
}

func (c *Client) Cleanup() error {
	if len(c.cleanups) == 0 {
		return nil
	}

	c.logger.Info("Performing kafka client cleanup")

	c.mutex.Lock()
	defer c.mutex.Unlock()

	var errs []error

	for i, cleanup := range c.cleanups {
		c.logger.Debug("Executing cleanup '%d'", i)

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
	c.logger.Debug("Dialing kafka client connection")

	if c.conn == nil {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		conn, err := kafka.DialContext(ctx, c.options.Network, c.options.Brokers[0])
		if err != nil {
			return nil, err
		}

		c.cleanups = append(c.cleanups, conn.Close)
		c.conn = conn

		c.logger.Debug("New dial connection created and returned")
		return c.conn, nil
	}

	c.logger.Debug("Reusing existing dial connection")
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
	c.logger.Debug("Returning full topic creation for '%s'", text.String())

	return text.String()
}
