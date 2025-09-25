package broker

import (
	"fmt"
	"sync"

	"github.com/shuv0id/strim/internal/config"
	"github.com/shuv0id/strim/internal/partition"
)

type Broker struct {
	ID          string
	DataDir     string
	Topics      map[string]*Topic
	TopicConfig *config.TopicConfig
	mu          sync.RWMutex
}

func NewBroker(id string, dataDir string, topicCfg *config.TopicConfig) *Broker {
	return &Broker{
		ID:          id,
		DataDir:     dataDir,
		Topics:      make(map[string]*Topic),
		TopicConfig: topicCfg,
	}
}

func (b *Broker) CreateTopic(name string, partitionCount int) error {
	if _, ok := b.Topics[name]; ok {
		return fmt.Errorf("topic with name: %s already exists, name", name)
	}
	if partitionCount <= 0 {
		return fmt.Errorf("invalid number of partition: %d", partitionCount)
	}

	topicCfg := &config.TopicConfig{}

	topic := &Topic{
		Name:       name,
		Partitions: make(map[int]partition.Partition),
		Config:     topicCfg, // NOTE: pass empty config for now
	}

	logCfg := &config.LogConfig{
		SegmentBytes:  1073741824,
		IndexMaxBytes: 10485760,
	}
	for i := range partitionCount {
		p, err := partition.NewPartition(uint32(i), name, logCfg) // NOTE: pass empty config for now
		if err != nil {
			return fmt.Errorf("error creating partition for topic:%s :%v", name, err)
		}
		topic.Partitions[i] = p
	}

	b.Topics[name] = topic

	return nil
}

func (b *Broker) GetTopic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.Topics[name]
	if !exists {
		return nil
	}
	return t
}

func (b *Broker) GetPartition(topicName string, partitionIndex int) partition.Partition {
	if t, ok := b.Topics[topicName]; ok {
		if p, ok := t.Partitions[partitionIndex]; ok {
			return p
		}
	}
	return nil
}

func (b *Broker) GetTopicNames() []string {
	var topicNames []string
	for name := range b.Topics {
		topicNames = append(topicNames, name)
	}
	return topicNames
}
func (b *Broker) GetTopicConfig() *config.TopicConfig {
	return nil
}
