package options

import "time"

const (
	DefaultNumPartitions     = -1
	DefaultReplicationFactor = -1
	DefaultRetentionTime     = 86400000
	DefaultRetentionBytes    = 173741824
)

type TopicOptions struct {
	NumPartitions     int           `json:"num_partitions,omitempty"`
	ReplicationFactor int           `json:"replication_factor,omitempty"`
	RetentionTime     time.Duration `json:"retention_time,omitempty"`
	RetentionBytes    int64         `json:"retention_bytes,omitempty"`
}

func DefaultTopicOptions() TopicOptions {
	return TopicOptions{
		NumPartitions:     DefaultNumPartitions,
		ReplicationFactor: DefaultReplicationFactor,
		RetentionTime:     DefaultRetentionTime,
		RetentionBytes:    DefaultRetentionBytes,
	}
}

type TopicOption func(*TopicOptions) error

func WithNumPartitions(numPartitions int) TopicOption {
	return func(o *TopicOptions) error {
		o.NumPartitions = numPartitions
		return nil
	}
}

func WithReplicationFactor(replicationFactor int) TopicOption {
	return func(o *TopicOptions) error {
		o.ReplicationFactor = replicationFactor
		return nil
	}
}

func WithRetentionTime(retentionTime time.Duration) TopicOption {
	return func(o *TopicOptions) error {
		o.RetentionTime = retentionTime
		return nil
	}
}

func WithRetentionBytes(retentionBytes string) TopicOption {
	return func(o *TopicOptions) error {
		bytes, err := parseBytes(retentionBytes)
		if err != nil {
			return err
		}
		o.RetentionBytes = bytes
		return nil
	}
}
