package main

import (
	"context"
	"log"

	"github.com/mwantia/asynk/pkg/client"
	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
)

const (
	MockTopic = "mock"
)

type MockData struct {
	Content string `json:"content"`
}

func main() {
	c, err := client.New(MockTopic,
		kafka.WithBrokers("kafka:9092"),
		kafka.WithPool("debug"),
	)
	if err != nil {
		panic(err)
	}

	defer c.Close()

	log.Println("Submitting new task with mock data...")

	ev, _ := event.NewSubmitEvent(MockData{
		Content: "Hello World",
	})

	streams, err := c.Submit(context.Background(), ev)
	if err != nil {
		panic(err)
	}

	log.Println("Task submitted and waiting for status reports...")

	for stream := range streams {
		if stream.Status.IsTerminal() {
			log.Printf("Task completed with payload: %v", string(stream.Payload))
			break
		}

		log.Printf("Received status: %s", stream.Status)
	}
}
