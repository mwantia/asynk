package options

import "time"

type ClientOptionPreset string

const (
	// Optimized for immediate processing
	PresetLowLatency ClientOptionPreset = "low-latency"
	// Optimized for processing large volumes
	PresetHighThroughput ClientOptionPreset = "high-throughput"
	// Optimized for guaranteed delivery
	PresetReliable ClientOptionPreset = "reliable"
	// Default balanced configuration
	PresetBalanced ClientOptionPreset = "balanced"
)

func WithPreset(preset ClientOptionPreset) ClientOption {
	return func(o *ClientOptions) error {
		switch preset {
		case PresetLowLatency:
			o.MaxWait = time.Millisecond * 10
			o.CommitInterval = time.Millisecond * 50
			o.MinBytes = 1
			o.MaxBytes = 1e6 // 1MB
			o.BatchSize = 1
			o.BatchBytes = 1024 // 1KB
			o.BatchTimeout = time.Millisecond * 10
			o.Async = true

		case PresetHighThroughput:
			o.MaxWait = time.Second
			o.CommitInterval = time.Second
			o.MinBytes = 1e6  // 1MB
			o.MaxBytes = 10e6 // 10MB
			o.BatchSize = 100
			o.BatchBytes = 1e6 // 1MB
			o.BatchTimeout = time.Second
			o.Async = true

		case PresetReliable:
			o.MaxWait = time.Millisecond * 100
			o.CommitInterval = time.Millisecond * 100
			o.MinBytes = 1024 // 1KB
			o.MaxBytes = 1e6  // 1MB
			o.BatchSize = 16
			o.BatchBytes = 1e5 // 100KB
			o.BatchTimeout = time.Millisecond * 100
			o.Async = false

		case PresetBalanced:
			o.MaxWait = DefaultMaxWait
			o.CommitInterval = DefaultCommitInterval
			o.MinBytes = DefaultMinBytes
			o.MaxBytes = DefaultMaxBytes
			o.BatchSize = DefaultBatchSize
			o.BatchBytes = DefaultBatchBytes
			o.BatchTimeout = DefaultBatchTimeout
			o.Async = DefaultAsync
		}
		return nil
	}
}
