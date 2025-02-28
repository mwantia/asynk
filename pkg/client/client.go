package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mwantia/asynk/internal/kafka"
	basic "github.com/mwantia/asynk/internal/log"
	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/log"
	"github.com/mwantia/asynk/pkg/options"
)

type Client struct {
	suffix string

	options options.ClientOptions
	logger  log.LogWrapper
	session *kafka.Session
	events  map[string]chan *event.StatusEvent

	mutex  sync.RWMutex
	active atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
	wait   sync.WaitGroup
}

func NewClient(suffix string, opts ...options.ClientOption) (*Client, error) {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return nil, errors.New("suffix cannot be empty")
	}

	options := options.DefaultClientOptions()
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	var logger log.LogWrapper

	if options.Logger != nil {
		logger = basic.NewNamed(*options.Logger, "asynk/client")
	}
	if logger == nil {
		l := basic.NewBasic(options.LogLevel)
		logger = l.Named("asynk/client")
	}

	k, err := kafka.NewKafka(options, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	s, err := k.Session(suffix)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka session: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		suffix: suffix,

		options: options,
		logger:  logger,
		session: s,
		events:  make(map[string]chan *event.StatusEvent),

		ctx:    ctx,
		cancel: cancel,
	}

	c.active.Store(true)
	return c, nil
}

func (c *Client) Archive(ctx context.Context, ev *event.SubmitEvent, reason string) error {
	if !c.active.Load() {
		return fmt.Errorf("client has already been closed")
	}

	return errors.New("not implemented")
}

func (c *Client) Submit(ctx context.Context, ev event.SubmitEvent) (chan *event.StatusEvent, error) {
	if !c.active.Load() {
		return nil, fmt.Errorf("client has already been closed")
	}

	c.logger.Info("Submitting task '%s' to Kafka", ev.ID)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-c.ctx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	if ev.Time.IsZero() {
		now := time.Now()
		c.logger.Debug("Time not set; Setting time with '%v'", now)
		ev.Time = now
	}

	if ev.ID == "" {
		id := event.UUIDv7()
		c.logger.Debug("ID not set; Creating new uuidv7 '%s'", id)
		ev.ID = id
	}

	writer := c.session.GetWriter("events.submit")

	if err := writer.WriteEvent(ctx, &ev); err != nil {
		return nil, fmt.Errorf("failed to write kafka message: %w", err)
	}

	reader := c.session.GetReader("events.status")

	c.logger.Debug("Creating status channel for task '%s'", ev.ID)

	c.mutex.Lock()
	ch := make(chan *event.StatusEvent, 100)
	c.events[ev.ID] = ch
	c.mutex.Unlock()

	c.wait.Add(1)

	c.logger.Debug("Started event processing goroutine for task '%s'", ev.ID)
	go c.processEvent(ctx, ev.ID, reader, ch)

	return ch, nil
}

func (c *Client) Close() error {
	if !c.active.CompareAndSwap(true, false) {
		return fmt.Errorf("client has already been closed")
	}

	c.logger.Info("Closing client connections...")

	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wait.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Goroutines completed cleanly
	case <-time.After(c.options.ShutdownTimeout):
		c.logger.Warn("Shutdown timed out; Some goroutines may still be running...")
	}

	c.mutex.Lock()
	for id, channel := range c.events {
		c.logger.Debug("Closing channel for task '%s'", id)
		close(channel)
	}
	c.events = make(map[string]chan *event.StatusEvent)
	c.mutex.Unlock()

	return c.session.Client().Cleanup()
}
