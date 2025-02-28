package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/mwantia/asynk/pkg/log"
	"github.com/mwantia/asynk/pkg/options"
	"github.com/segmentio/kafka-go"
)

type Session struct {
	ID     string
	Suffix string

	mutex  sync.RWMutex
	client *Client
	logger log.LogWrapper

	readers  map[string]*Reader
	writers  map[string]*Writer
	cleanups []func() error
}

func (s *Session) Client() *Client {
	return s.client
}

func (s *Session) CreateTopic(ctx context.Context, topic string, opts ...options.TopicOption) error {
	conn, err := s.client.dial(ctx)
	if err != nil {
		return fmt.Errorf("error during dial: %w", err)
	}

	options := options.DefaultTopicOptions()
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return err
		}
	}

	s.logger.Info("Creating new topic '%s'", topic)

	config := kafka.TopicConfig{
		Topic:             s.fullTopic(topic),
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

func (s *Session) GetReader(suffix string) *Reader {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if reader, exist := s.readers[suffix]; exist {
		s.logger.Debug("Returning existing reader for suffix '%s'", suffix)
		return reader
	}

	reader := &Reader{
		session: s,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:          s.client.options.Brokers,
			Topic:            s.fullTopic(suffix),
			GroupID:          s.client.options.GroupID,
			MaxWait:          s.client.options.MaxWait,
			CommitInterval:   s.client.options.CommitInterval,
			MinBytes:         int(s.client.options.MinBytes),
			MaxBytes:         int(s.client.options.MaxBytes),
			ReadBatchTimeout: s.client.options.BatchTimeout,
		}),
		logger: s.logger.Named("kafka/reader"),
	}

	s.logger.Debug("Creating new reader for suffix '%s'", suffix)

	s.readers[suffix] = reader
	s.client.cleanups = append(s.cleanups, reader.reader.Close)

	return reader
}

func (s *Session) GetWriter(suffix string) *Writer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if writer, exist := s.writers[suffix]; exist {
		s.logger.Debug("Returning existing writer for suffix '%s'", suffix)
		return writer
	}

	writer := &Writer{
		session: s,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(s.client.options.Brokers...),
			Topic:        s.fullTopic(suffix),
			Balancer:     &kafka.LeastBytes{},
			BatchSize:    s.client.options.BatchSize,
			BatchTimeout: s.client.options.BatchTimeout,
			BatchBytes:   s.client.options.BatchBytes,
			Async:        s.client.options.Async,
		},
		logger: s.logger.Named("kafka/writer"),
	}

	s.logger.Debug("Creating new writer for suffix '%s'", suffix)

	s.writers[suffix] = writer
	s.client.cleanups = append(s.cleanups, writer.writer.Close)

	return writer
}

func (s *Session) fullTopic(suffix string) string {
	var text strings.Builder
	if s.client.options.TopicPrefix != "" {
		text.WriteString(s.client.options.TopicPrefix + ".")
	}
	if s.client.options.Pool != "" {
		text.WriteString(s.client.options.Pool + ".")
	}

	text.WriteString(s.Suffix)
	if suffix != "" {
		text.WriteString("." + suffix)
	}

	return text.String()
}
