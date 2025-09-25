package config

type StreamConfig struct {
	Log LogConfig `yaml:"log"`
}

// TODO: add more config as we go
type LogConfig struct {
	SegmentBytes int64 `yaml:"segment_bytes"`

	IndexIntervalBytes int64 `yaml:"index_interval_bytes"`
	IndexMaxBytes      int64 `yaml:"index_max_bytes"`
}

type TopicConfig struct {
	NumPartitions int64
}
