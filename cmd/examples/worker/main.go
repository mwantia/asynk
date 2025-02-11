package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
	"github.com/mwantia/asynk/pkg/worker"
)

const (
	MockTopic = "mock"
)

type MockData struct {
	Content string `json:"content"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	w, err := worker.New(
		kafka.WithBrokers("kafka-proxy.ingress.consul:29090"),
		kafka.WithTopicPrefix("asynk"),
		kafka.WithPool("debug"),
		kafka.WithGroupID("group1"),
	)
	if err != nil {
		panic(err)
	}

	log.Println("Running worker...")

	mux := worker.NewServeMux()
	mux.HandleFunc(MockTopic, HandleMock)

	if err := w.Run(ctx, mux); err != nil {
		panic(err)
	}

	log.Println("Worker cleanup completed...")
}

func HandleMock(ctx context.Context, c *kafka.Client, te *event.TaskEvent) error {
	var data MockData
	if err := json.Unmarshal(te.Payload, &data); err != nil {
		return err
	}

	log.Println(te.ID)
	log.Println(data.Content)

	return nil
}
