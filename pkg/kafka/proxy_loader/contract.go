package proxy_loader

import (
	"context"
	"fmt"
	kafka2 "github.com/PavelAgarkov/template/pkg/kafka"
	"github.com/PavelAgarkov/template/pkg/metrics"
	"runtime"

	"github.com/rs/xid"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type ProxyLoader interface {
	ProcessShkOnPlaceBytesStreamV1(ctx context.Context, body []byte, version string) error
	ProcessTareMoveBytesStreamV1(ctx context.Context, body []byte, version string) error
}

type shardBatches map[int64][]kafka.Message

type KafkaLoader struct {
	mainProducers  []kafka2.Producer
	shardProducers []kafka2.Producer
	custom         *metrics.Metrics
}

func NewProxyLoader(custom *metrics.Metrics, mainProducers, shardProducers []kafka2.Producer) ProxyLoader {
	return &KafkaLoader{
		custom:         custom,
		mainProducers:  mainProducers,
		shardProducers: shardProducers,
	}
}

// keyBalancer в связке с Hash{} из kafka-go обеспечивает
// равномерное распределение сообщений по партициям топика
// в пределах одного батча все сообщения летят в одну партицию
// т.к. ключ для всех сообщений один и тот же
func keyBalancer() []byte {
	return []byte(xid.New().String())
}

// topicFinder выбирает топик для записи по ключу office_id
func topicFinder(partitionKey int64) func(mapper []kafka2.TopicMapper) (string, error) {
	return func(mapper []kafka2.TopicMapper) (string, error) {
		var topic string
		for _, m := range mapper {
			if m.OfficeID == partitionKey {
				topic = m.Topic
				return topic, nil
			}
		}
		return "", fmt.Errorf("no topic found for partition key: %d", partitionKey)
	}
}

func (kl *KafkaLoader) sendToTopic(
	ctx context.Context,
	producers map[int64]kafka2.Producer,
	groupedByShard shardBatches,
	shardHash []byte,
) error {
	withContext, gctx := errgroup.WithContext(ctx)
	withContext.SetLimit(runtime.GOMAXPROCS(0))

	for shardKey, messages := range groupedByShard {
		sk := shardKey
		batch := messages
		p, ok := producers[sk]
		if !ok {
			return fmt.Errorf("no configured producer for office_id=%d", sk)
		}
		if len(batch) == 0 {
			p.Pool().Put(batch[:0])
			delete(groupedByShard, sk)
			continue
		}
		withContext.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic in writer: %v", r)
				}

				if cap(batch) > kafka2.MaxKeepCap {
					// В пул кладём компактный буфер с базовой capacity
					compact := make([]kafka.Message, 0, kafka2.DoubleMultiplePool(p.GetBatchSize()))
					p.Pool().Put(compact)
				} else {
					p.Pool().Put(batch[:0])
				}

			}()

			if err := p.WriteMessages(gctx, shardHash, topicFinder(sk), batch...); err != nil {
				return fmt.Errorf("failed to write partition messages: %w", err)
			}
			return nil
		})

		delete(groupedByShard, sk)
	}

	if err := withContext.Wait(); err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}

	return nil
}
