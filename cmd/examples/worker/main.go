package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/mwantia/asynk/pkg/options"
	"github.com/mwantia/asynk/pkg/shared"
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

	w, err := worker.NewWorker(
		options.WithBrokers("kafka:9092"),
		options.WithPool("debug"),
		options.WithGroupID("group1"),
	)
	if err != nil {
		panic(err)
	}

	log.Println("Running worker...")

	if err := w.RunHandlerFunc(ctx, MockTopic, HandleMock); err != nil {
		panic(err)
	}

	log.Println("Worker cleanup completed...")
}

func HandleMock(ctx context.Context, task *shared.Task) error {
	var data MockData
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return err
	}

	log.Println(task.ID)
	log.Println(data.Content)

	return nil
}
