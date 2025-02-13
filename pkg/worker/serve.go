package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

type ServeMux struct {
	mutex    sync.RWMutex
	handlers map[string]serveMuxEntry
}

type serveMuxEntry struct {
	handler Handler
	topic   string
}

func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers: make(map[string]serveMuxEntry),
	}
}

func (mux *ServeMux) Handle(topic string, handler Handler) error {
	mux.mutex.Lock()
	defer mux.mutex.Unlock()

	if handler == nil {
		return fmt.Errorf("invalid handler")
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("invalid suffix")
	}

	if _, exist := mux.handlers[topic]; exist {
		return fmt.Errorf("suffix already registered")
	}

	mux.handlers[topic] = serveMuxEntry{
		handler: handler,
		topic:   topic,
	}

	return nil
}

func (mux *ServeMux) HandleFunc(topic string, handler func(context.Context, *kafka.Client, *event.SubmitEvent) error) error {
	if handler == nil {
		return fmt.Errorf("invalid handler")
	}

	return mux.Handle(topic, HandlerFunc(handler))
}

func (mux *ServeMux) ProcessSubmitEvent(ctx context.Context, c *kafka.Client, ev *event.SubmitEvent) error {
	mux.mutex.RLock()
	defer mux.mutex.RUnlock()

	handler, exist := mux.handlers[""]
	if exist {
		return handler.handler.ProcessSubmitEvent(ctx, c, ev)
	}

	return fmt.Errorf("topic not found")
}
