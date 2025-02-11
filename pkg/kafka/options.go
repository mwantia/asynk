package kafka

import (
	"time"
)

const (
	DefaultBroker          = "localhost:9092"
	DefaultNetwork         = "tcp"
	DefaultGroupID         = ""
	DefaultTopicPrefix     = "asynk"
	DefaultPool            = "default"
	DefaultMinBytes        = 10e3
	DefaultMaxBytes        = 10e3
	DefaultConnectTimeout  = time.Second * 30
	DefaultShutdownTimeout = time.Second * 30
)

type Options struct {
	Brokers         []string      `json:"brokers,omitempty"`
	Network         string        `json:"network,omitempty"`
	GroupID         string        `json:"group_id,omitempty"`
	TopicPrefix     string        `json:"topic_prefix,omitempty"`
	Pool            string        `json:"pool,omitempty"`
	MinBytes        int           `json:"min_bytes,omitempty"`
	MaxBytes        int           `json:"max_bytes,omitempty"`
	ConnectTimeout  time.Duration `json:"connect_timeout,omitempty"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout,omitempty"`
}

func DefaultOptions() Options {
	return Options{
		Brokers: []string{
			DefaultBroker,
		},
		Network:         DefaultNetwork,
		GroupID:         DefaultGroupID,
		TopicPrefix:     DefaultTopicPrefix,
		Pool:            DefaultPool,
		MinBytes:        DefaultMinBytes,
		MaxBytes:        DefaultMaxBytes,
		ConnectTimeout:  DefaultConnectTimeout,
		ShutdownTimeout: DefaultShutdownTimeout,
	}
}

type Option func(*Options) error

func WithBrokers(brokers ...string) Option {
	return func(o *Options) error {
		o.Brokers = brokers
		return nil
	}
}

func WithNetwork(network string) Option {
	return func(o *Options) error {
		o.Network = network
		return nil
	}
}

func WithGroupID(groupID string) Option {
	return func(o *Options) error {
		o.GroupID = groupID
		return nil
	}
}

func WithTopicPrefix(topicPrefix string) Option {
	return func(o *Options) error {
		o.TopicPrefix = topicPrefix
		return nil
	}
}

func WithPool(pool string) Option {
	return func(o *Options) error {
		o.Pool = pool
		return nil
	}
}

func WithMinBytes(minBytes int) Option {
	return func(o *Options) error {
		o.MinBytes = minBytes
		return nil
	}
}

func WithMaxBytes(maxBytes int) Option {
	return func(o *Options) error {
		o.MaxBytes = maxBytes
		return nil
	}
}

func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.ConnectTimeout = timeout
		return nil
	}
}

func WithShutdownTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.ShutdownTimeout = timeout
		return nil
	}
}
