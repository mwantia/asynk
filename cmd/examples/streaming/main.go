package main

import (
	"context"
	"fmt"

	"github.com/mwantia/asynk/pkg/client"
	"github.com/mwantia/asynk/pkg/options"
	"github.com/mwantia/asynk/pkg/shared"
)

const (
	MockTopic = "mock"
)

type MockData struct {
	Content string `json:"content"`
}

func main() {
	c, err := client.New(
		options.WithBrokers("kafka:9092"),
		options.WithPool("debug"),
	)
	if err != nil {
		panic(err)
	}

	defer c.Close()

	task, err := shared.NewTask(MockData{
		Content: "Hello World",
	})
	if err != nil {
		panic(err)
	}

	streams, err := c.Submit(context.Background(), task)
	if err != nil {
		panic(err)
	}

	for stream := range streams {
		fmt.Printf("Task: %s: Status = '%s'\n", stream.Task, stream.Status)
		if stream.Status.IsTerminal() {
			break
		}
	}
}
