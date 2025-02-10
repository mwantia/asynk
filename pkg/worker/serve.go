package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/mwantia/asynk/pkg/shared"
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

func (mux *ServeMux) HandleFunc(topic string, handler func(context.Context, *shared.Task) error) error {
	if handler == nil {
		return fmt.Errorf("invalid handler")
	}

	return mux.Handle(topic, HandlerFunc(handler))
}

func (mux *ServeMux) ProcessTask(ctx context.Context, task *shared.Task) error {
	mux.mutex.RLock()
	defer mux.mutex.RUnlock()

	handler, exist := mux.handlers[""]
	if exist {
		return handler.handler.ProcessTask(ctx, task)
	}

	return fmt.Errorf("topic not found")
}
