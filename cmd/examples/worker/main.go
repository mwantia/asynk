package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

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
		kafka.WithBrokers("kafka:9092"),
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

func HandleMock(ctx context.Context, c *kafka.Client, ev *event.TaskSubmitEvent) error {
	var data MockData
	if err := json.Unmarshal(ev.Payload, &data); err != nil {
		return err
	}

	log.Printf("New task '%s' received with mock data:\n", ev.ID)
	log.Println(data.Content)

	writer := c.NewWriter(MockTopic + ".tasks.status")

	timeout := time.After(1 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("Performing work for mock...")

	for {
		select {
		case <-ticker.C:
			if err := writer.WriteStatusEvent(ctx, &event.TaskStatusEvent{
				TaskID: ev.ID,
				Status: event.StatusRunning,
			}); err != nil {
				return err
			}
		case <-ctx.Done():
			log.Println("Work has been cancelled...")

			return writer.WriteStatusEvent(ctx, &event.TaskStatusEvent{
				TaskID: ev.ID,
				Status: event.StatusLost,
			})
		case <-timeout:
			log.Println("Work has been completed...")

			return writer.WriteStatusEvent(ctx, &event.TaskStatusEvent{
				TaskID:  ev.ID,
				Status:  event.StatusComplete,
				Payload: ev.Payload,
			})
		}
	}
}
