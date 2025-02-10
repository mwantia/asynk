package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Admin struct {
	conn *kafka.Conn
}

func NewAdmin(brokers []string) (*Admin, error) {
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Kafka: %w", err)
	}

	return &Admin{
		conn: conn,
	}, nil
}

func (a *Admin) CreateTopics(topics ...string) error {
	topicConfigs := make([]kafka.TopicConfig, 0, len(topics))

	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "604800000",
				},
			},
		})
	}

	return a.conn.CreateTopics(topicConfigs...)
}

func (a *Admin) Close() error {
	return a.conn.Close()
}
