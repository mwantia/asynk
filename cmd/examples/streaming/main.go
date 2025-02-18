package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/mwantia/asynk/pkg/client"
	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/options"
)

const (
	MockTopic = "mock"
)

type MockData struct {
	Content string `json:"content"`
}

func main() {
	c, err := client.NewClient(MockTopic,
		options.WithBrokers("kafka:9092"),
		options.WithPool("debug"),
	)
	if err != nil {
		panic(err)
	}

	defer c.Close()

	log.Println("Submitting new task with mock data...")

	payload, err := json.Marshal(MockData{
		Content: "Hello World",
	})
	if err != nil {
		panic(err)
	}

	streams, err := c.Submit(context.Background(), event.SubmitEvent{
		ID:      event.UUIDv7(),
		Payload: payload,
	})
	if err != nil {
		panic(err)
	}

	log.Println("Task submitted and waiting for status reports...")

	for stream := range streams {
		if stream.Status.IsTerminal() {
			log.Println("Task completed with payload...")
			break
		}

		var data MockData
		if err := json.Unmarshal(stream.Payload, &data); err != nil {
			panic(err)
		}

		fmt.Print(data.Content)
	}
}
