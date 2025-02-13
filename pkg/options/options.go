package options

import (
	"time"
)

const (
	DefaultBroker      = "localhost:9092"
	DefaultNetwork     = "tcp"
	DefaultGroupID     = ""
	DefaultTopicPrefix = "asynk"
	DefaultPool        = "default"

	DefaultMaxWait         = time.Millisecond * 50
	DefaultCommitInterval  = time.Millisecond * 100
	DefaultMinBytes        = 10e3
	DefaultMaxBytes        = 10e3
	DefaultConnectTimeout  = time.Second * 30
	DefaultShutdownTimeout = time.Second * 30
	DefaultBatchSize       = 16
	DefaultBatchBytes      = 1e5 // 100KB
	DefaultBatchTimeout    = time.Millisecond * 50
	DefaultAsync           = false
)

type ClientOptions struct {
	Brokers         []string      `json:"brokers,omitempty"`
	Network         string        `json:"network,omitempty"`
	GroupID         string        `json:"group_id,omitempty"`
	TopicPrefix     string        `json:"topic_prefix,omitempty"`
	Pool            string        `json:"pool,omitempty"`
	MaxWait         time.Duration `json:"max_wait,omitempty"`
	CommitInterval  time.Duration `json:"commit_interval,omitempty"`
	MinBytes        int64         `json:"min_bytes,omitempty"`
	MaxBytes        int64         `json:"max_bytes,omitempty"`
	ConnectTimeout  time.Duration `json:"connect_timeout,omitempty"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout,omitempty"`
	BatchSize       int           `json:"batch_size,omitempty"`
	BatchBytes      int64         `json:"batch_bytes,omitempty"`
	BatchTimeout    time.Duration `json:"batch_timeout,omitempty"`
	Async           bool          `json:"async,omitempty"`
}

func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Brokers: []string{
			DefaultBroker,
		},
		Network:         DefaultNetwork,
		GroupID:         DefaultGroupID,
		TopicPrefix:     DefaultTopicPrefix,
		Pool:            DefaultPool,
		MaxWait:         DefaultMaxWait,
		CommitInterval:  DefaultCommitInterval,
		MinBytes:        DefaultMinBytes,
		MaxBytes:        DefaultMaxBytes,
		ConnectTimeout:  DefaultConnectTimeout,
		ShutdownTimeout: DefaultShutdownTimeout,
		BatchSize:       DefaultBatchSize,
		BatchBytes:      DefaultBatchBytes,
		BatchTimeout:    DefaultBatchTimeout,
		Async:           DefaultAsync,
	}
}

type ClientOption func(*ClientOptions) error

func WithBrokers(brokers ...string) ClientOption {
	return func(o *ClientOptions) error {
		o.Brokers = brokers
		return nil
	}
}

func WithNetwork(network string) ClientOption {
	return func(o *ClientOptions) error {
		o.Network = network
		return nil
	}
}

func WithGroupID(groupID string) ClientOption {
	return func(o *ClientOptions) error {
		o.GroupID = groupID
		return nil
	}
}

func WithTopicPrefix(topicPrefix string) ClientOption {
	return func(o *ClientOptions) error {
		o.TopicPrefix = topicPrefix
		return nil
	}
}

func WithPool(pool string) ClientOption {
	return func(o *ClientOptions) error {
		o.Pool = pool
		return nil
	}
}

func WithMaxWait(wait time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.MaxWait = wait
		return nil
	}
}

func WithCommitInterval(interval time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.CommitInterval = interval
		return nil
	}
}

func WithMinBytes(minBytes string) ClientOption {
	return func(o *ClientOptions) error {
		bytes, err := parseBytes(minBytes)
		if err != nil {
			return err
		}
		o.MinBytes = bytes
		return nil
	}
}

func WithMaxBytes(maxBytes string) ClientOption {
	return func(o *ClientOptions) error {
		bytes, err := parseBytes(maxBytes)
		if err != nil {
			return err
		}
		o.MaxBytes = bytes
		return nil
	}
}

func WithConnectTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.ConnectTimeout = timeout
		return nil
	}
}

func WithShutdownTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.ShutdownTimeout = timeout
		return nil
	}
}

func WithBatchSize(size int) ClientOption {
	return func(o *ClientOptions) error {
		o.BatchSize = size
		return nil
	}
}

func WithBatchBytes(batchBytes string) ClientOption {
	return func(o *ClientOptions) error {
		bytes, err := parseBytes(batchBytes)
		if err != nil {
			return err
		}
		o.BatchBytes = bytes
		return nil
	}
}

func WithBatchTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.BatchTimeout = timeout
		return nil
	}
}

func WithAsync(async bool) ClientOption {
	return func(o *ClientOptions) error {
		o.Async = async
		return nil
	}
}
