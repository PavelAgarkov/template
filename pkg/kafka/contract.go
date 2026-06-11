package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

type TopicMapper struct {
	Topic    string
	OfficeID int64
}

const (
	ShardProducer = "shard"
	MainProducer  = "main"
	MainTopic     = 0
)

type Producer interface {
	WriteMessages(
		ctx context.Context,
		shardHash []byte,
		topicFinder func(mapper []TopicMapper) (string, error),
		messages ...kafka.Message,
	) error
	Close() error
	GetType() string
	GetName() string
	Entity() string
	GetTopicMapper() []TopicMapper
	Pool() *sync.Pool
	Ping(ctx context.Context) error
	GetBatchSize() int
}
