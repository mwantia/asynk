package kafka

import (
	"context"
	"strings"
	"sync"

	"github.com/mwantia/asynk/pkg/options"
	"github.com/segmentio/kafka-go"
)

type Session struct {
	mutex   sync.RWMutex
	client  *Client
	suffix  string
	options []options.TopicOption

	readers  map[string]*kafka.Reader
	writers  map[string]*kafka.Writer
	cleanups []func() error
}

func (s *Session) fullTopic(suffix string) string {
	var text strings.Builder
	if s.client.options.TopicPrefix != "" {
		text.WriteString(s.client.options.TopicPrefix + ".")
	}
	if s.client.options.Pool != "" {
		text.WriteString(s.client.options.Pool + ".")
	}

	text.WriteString(s.suffix)
	if suffix != "" {
		text.WriteString("." + suffix)
	}

	return text.String()
}

func (s *Session) cachedReader(suffix string) (*kafka.Reader, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if reader, exist := s.readers[suffix]; exist {
		return reader, nil
	}

	if err := s.client.CreateTopic(context.Background(), suffix, s.options...); err != nil {
		return nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          s.client.options.Brokers,
		Topic:            s.fullTopic(suffix),
		GroupID:          s.client.options.GroupID,
		MaxWait:          s.client.options.MaxWait,
		CommitInterval:   s.client.options.CommitInterval,
		MinBytes:         int(s.client.options.MinBytes),
		MaxBytes:         int(s.client.options.MaxBytes),
		ReadBatchTimeout: s.client.options.BatchTimeout,
	})

	s.readers[suffix] = reader
	s.cleanups = append(s.cleanups, reader.Close)

	return reader, nil
}

func (s *Session) cachedWriter(suffix string) (*kafka.Writer, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if writer, exist := s.writers[suffix]; exist {
		return writer, nil
	}

	if err := s.client.CreateTopic(context.Background(), suffix, s.options...); err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(s.client.options.Brokers...),
		Topic:        s.fullTopic(suffix),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    s.client.options.BatchSize,
		BatchTimeout: s.client.options.BatchTimeout,
		BatchBytes:   s.client.options.BatchBytes,
		Async:        s.client.options.Async,
	}

	s.writers[suffix] = writer
	s.cleanups = append(s.cleanups, writer.Close)

	return writer, nil
}

func (s *Session) ReadMessage(ctx context.Context, suffix string) (kafka.Message, error) {
	reader, err := s.cachedReader(suffix)
	if err != nil {
		return kafka.Message{}, err
	}

	return reader.ReadMessage(ctx)
}
