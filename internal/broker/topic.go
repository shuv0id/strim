package broker

import (
	"github.com/shuv0id/strim/internal/config"
	"github.com/shuv0id/strim/internal/partition"
)

type TopicManager interface {
	GetTopicNames() []string
	GetTopic(name string) *Topic
	CreateTopic(name string, partitionCount int) error
	GetTopicConfig() *config.TopicConfig
	GetPartition(topicName string, partitionIndex int) partition.Partition
}

type Topic struct {
	Name       string
	Partitions map[int]partition.Partition
	Config     *config.TopicConfig
}
