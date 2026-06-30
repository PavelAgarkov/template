package kafka

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type WriterWrapper struct {
	name        string
	_type       string
	entity      string
	topicMapper []TopicMapper
	writer      *kafka.Writer

	pool      *sync.Pool
	brokers   []string
	transport *kafka.Transport
}

func NewWriterWrapper(
	writer *kafka.Writer,
	topicMapper []TopicMapper,
	_type string,
	name string,
	entity string,
	pool *sync.Pool,
	brokers []string,
	transport *kafka.Transport,
) Producer {
	return &WriterWrapper{
		writer:      writer,
		topicMapper: topicMapper,
		_type:       _type,
		name:        name,
		entity:      entity,
		pool:        pool,
		brokers:     brokers,
		transport:   transport,
	}
}

func (w *WriterWrapper) WriteMessages(
	ctx context.Context,
	shardHash []byte,
	topicFinder func(mapper []TopicMapper) (string, error),
	messages ...kafka.Message,
) error {
	if topicFinder == nil {
		return fmt.Errorf("topicFinder is nil")
	}

	topic, err := topicFinder(w.topicMapper)
	if err != nil {
		return fmt.Errorf("failed to find topic: %w", err)
	}

	// для всех сообщений батча один и тот же ключ - летят в одну партицию
	for i := range messages {
		messages[i].Key = shardHash
		messages[i].Topic = topic
	}
	err = w.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}

	return nil
}

func (w *WriterWrapper) Ping(ctx context.Context) error {
	if len(w.brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, addr := range w.brokers {
		addr := addr
		g.Go(func() error {
			var conn *kafka.Conn
			var err error
			if w.transport != nil && w.transport.SASL != nil {
				dialer := &kafka.Dialer{
					Timeout:       5 * time.Second,
					SASLMechanism: w.transport.SASL,
					TLS:           w.transport.TLS,
				}
				conn, err = dialer.DialContext(gctx, "tcp", addr)
			} else {
				conn, err = kafka.DialContext(gctx, "tcp", addr)
			}

			if err != nil {
				return fmt.Errorf("failed to dial kafka broker %s: %w", addr, err)
			}

			err = conn.Close()
			if err != nil {
				log.Printf("failed to close connection to kafka broker %s: %v", addr, err)
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	return nil
}

func (w *WriterWrapper) Close() error {
	return w.writer.Close()
}

func (w *WriterWrapper) GetType() string {
	return w._type
}

func (w *WriterWrapper) GetName() string {
	return w.name
}

func (w *WriterWrapper) GetTopicMapper() []TopicMapper {
	return w.topicMapper
}

func (w *WriterWrapper) Pool() *sync.Pool {
	return w.pool
}

func (w *WriterWrapper) Entity() string {
	return w.entity
}

func (w *WriterWrapper) GetBatchSize() int {
	return w.writer.BatchSize
}
