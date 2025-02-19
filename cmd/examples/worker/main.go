package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/mwantia/asynk/pkg/event"
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

	mux := server.NewServeMux()
	mux.HandleFunc(MockTopic, HandleMock)

	if err := srv.ServeMutex(ctx, mux); err != nil {
		// Ignore errors when the context has been cancelled
		if ctx.Err() == nil {
			panic(err)
		}
	}
}

func HandleMock(ctx context.Context, p *server.Pipeline) error {
	var data MockData
	if err := json.Unmarshal(p.Submit().Payload, &data); err != nil {
		return err
	}

	log.Printf("New task '%s' received with mock data:\n", p.Submit().ID)
	log.Println(data.Content)

	log.Println("Performing work for mock...")

	words := strings.Fields(MockContent)
	for _, word := range words {
		select {
		case <-ctx.Done():
			return p.Done(ctx, event.StatusLost)
		default:
			data := MockData{
				Content: word + " ",
			}

			payload, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal data: %w", err)
			}

			if err := p.Status(ctx, &event.StatusEvent{
				Status:  event.StatusRunning,
				Payload: payload,
			}); err != nil {
				return fmt.Errorf("failed to update status: %w", err)
			}
		}
	}

	log.Println("Work has been completed...")

	return p.Done(ctx, event.StatusComplete)
}
