package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/mwantia/asynk/pkg/event"
	"github.com/mwantia/asynk/pkg/kafka"
	"github.com/mwantia/asynk/pkg/options"
	"github.com/mwantia/asynk/pkg/server"
)

const (
	MockTopic   = "mock"
	MockContent = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
	Pellentesque ac elit vitae lectus ultricies ornare id nec neque. 
	Mauris condimentum nec leo at sagittis. 
	Nam aliquam neque in nunc dapibus, non venenatis est tincidunt. 
	Etiam pulvinar felis eget magna fringilla luctus. 
	Quisque est ante, eleifend vitae dui non, luctus laoreet ligula. 
	Curabitur nec tortor scelerisque, rhoncus felis in, volutpat nibh. 
	Ut ac efficitur ante. 
	Phasellus sed tellus et metus vestibulum placerat ac in orci. 
	Maecenas eget urna nulla. 
	Fusce eget ligula tristique, ultricies ante ut, porta lacus. 
	Donec consectetur ipsum posuere, suscipit augue vel, auctor eros. 
	Vestibulum vestibulum nisl velit, eget consectetur ante condimentum eu. 
	In finibus elementum fringilla.`
)

type MockData struct {
	Content string `json:"content"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	srv, err := server.NewServer(
		options.WithBrokers("kafka:9092"),
		options.WithPool("debug"),
		options.WithGroupID("group1"),
	)
	if err != nil {
		panic(err)
	}

	log.Println("Running worker...")

	mux := server.NewServeMux()
	mux.HandleFunc(MockTopic, HandleMock)

	if err := srv.ServeMutex(ctx, mux); err != nil {
		panic(err)
	}

	log.Println("Worker cleanup completed...")
}

func HandleMock(ctx context.Context, c *kafka.Client, ev *event.SubmitEvent) error {
	var data MockData
	if err := json.Unmarshal(ev.Payload, &data); err != nil {
		return err
	}

	log.Printf("New task '%s' received with mock data:\n", ev.ID)
	log.Println(data.Content)

	writer := c.NewWriter(MockTopic + ".tasks.status")

	log.Println("Performing work for mock...")

	words := strings.Fields(MockContent)
	for _, word := range words {
		select {
		case <-ctx.Done():
			return writer.WriteStatusEvent(ctx, &event.StatusEvent{
				TaskID: ev.ID,
				Status: event.StatusLost,
			})
		default:
			payload, err := json.Marshal(MockData{
				Content: word + " ",
			})
			if err != nil {
				return err
			}

			err = writer.WriteStatusEvent(ctx, &event.StatusEvent{
				TaskID:  ev.ID,
				Status:  event.StatusRunning,
				Payload: payload,
			})
			if err != nil {
				return err
			}
		}
	}

	log.Println("Work has been completed...")

	return writer.WriteStatusEvent(ctx, &event.StatusEvent{
		TaskID: ev.ID,
		Status: event.StatusComplete,
	})
}
