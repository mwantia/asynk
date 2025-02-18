package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type ServeMux struct {
	mutex    sync.RWMutex
	handlers map[string]serveMuxEntry
}

type serveMuxEntry struct {
	handler Handler
	topic   string
}

type Handler interface {
	ProcessPipeline(context.Context, *Pipeline) error
}

type HandlerFunc func(context.Context, *Pipeline) error

func (fn HandlerFunc) ProcessPipeline(ctx context.Context, p *Pipeline) error {
	return fn(ctx, p)
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

func (mux *ServeMux) HandleFunc(topic string, handler func(context.Context, *Pipeline) error) error {
	if handler == nil {
		return fmt.Errorf("invalid handler")
	}

	return mux.Handle(topic, HandlerFunc(handler))
}

func (mux *ServeMux) ProcessPipeline(ctx context.Context, p *Pipeline) error {
	mux.mutex.RLock()
	defer mux.mutex.RUnlock()

	handler, exist := mux.handlers[""]
	if exist {
		return handler.handler.ProcessPipeline(ctx, p)
	}

	return fmt.Errorf("topic not found")
}
