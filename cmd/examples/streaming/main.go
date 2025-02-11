package main

import (
	"context"
	"fmt"

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
	c, err := client.New(
		kafka.WithBrokers("kafka:9092"),
		kafka.WithPool("debug"),
	)
	if err != nil {
		panic(err)
	}

	defer c.Close()

	te, _ := event.NewTaskEvent(
		event.WithUUIDv7(),
		event.WithMarshal(MockData{
			Content: "Hello World",
		}),
	)

	streams, err := c.Submit(context.Background(), te)
	if err != nil {
		panic(err)
	}

	for stream := range streams {
		fmt.Printf("Task: %s: Status = '%s'\n", stream.ID, stream.Status)
		if stream.Status.IsTerminal() {
			break
		}
	}
}
