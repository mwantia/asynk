package kafka

import "time"

type OptionPreset string

const (
	// Optimized for immediate processing
	PresetLowLatency OptionPreset = "low-latency"
	// Optimized for processing large volumes
	PresetHighThroughput OptionPreset = "high-throughput"
	// Optimized for guaranteed delivery
	PresetReliable OptionPreset = "reliable"
	// Default balanced configuration
	PresetBalanced OptionPreset = "balanced"
)

func WithPreset(preset OptionPreset) Option {
	return func(o *Options) error {
		switch preset {
		case PresetLowLatency:
			o.MaxWait = time.Millisecond * 10
			o.CommitInterval = time.Millisecond * 50
			o.MinBytes = 1
			o.MaxBytes = 1e6

		case PresetHighThroughput:
			o.MaxWait = time.Second
			o.CommitInterval = time.Second
			o.MinBytes = 1e6
			o.MaxBytes = 10e6

		case PresetReliable:
			o.MaxWait = time.Millisecond * 100
			o.CommitInterval = time.Millisecond * 100
			o.MinBytes = 1024
			o.MaxBytes = 1e6

		case PresetBalanced:
			o.MaxWait = DefaultMaxWait
			o.CommitInterval = DefaultCommitInterval
			o.MinBytes = DefaultMinBytes
			o.MaxBytes = DefaultMaxBytes

			// o.BatchSize = DefaultBatchSize
			// o.BatchBytes = defaultOpts.BatchBytes
			// o.BatchTimeout = defaultOpts.BatchTimeout
			// o.Async = defaultOpts.Async
		}
		return nil
	}
}
