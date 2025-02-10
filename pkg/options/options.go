package options

import "time"

const (
	DefaultBroker = "localhost:9092"
	DefaultPrefix = "asynk"
	DefaultPool   = "default"
)

type Options struct {
	Brokers          []string
	Prefix           string
	Pool             string
	GroupID          string
	StreamBufferSize int
	ConnectTimeout   time.Duration
	ShutdownTimeout  time.Duration
}

func DefaultOptions() Options {
	return Options{
		Brokers: []string{
			DefaultBroker,
		},
		Prefix:           DefaultPrefix,
		Pool:             DefaultPool,
		ConnectTimeout:   time.Second * 30,
		ShutdownTimeout:  time.Second * 30,
		StreamBufferSize: 100,
	}
}

type Option func(*Options)

func WithBrokers(brokers ...string) Option {
	return func(o *Options) {
		o.Brokers = brokers
	}
}

func WithPrefix(prefix string) Option {
	return func(o *Options) {
		o.Prefix = prefix
	}
}

func WithPool(pool string) Option {
	return func(o *Options) {
		o.Pool = pool
	}
}

func WithGroupID(groupID string) Option {
	return func(o *Options) {
		o.GroupID = groupID
	}
}

func WithStreamBufferSize(size int) Option {
	return func(o *Options) {
		o.StreamBufferSize = size
	}
}

func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ConnectTimeout = timeout
	}
}

func WithShutdownTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ShutdownTimeout = timeout
	}
}
