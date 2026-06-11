package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	loggerwrapper "github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/segmentio/kafka-go"
)

type Configs struct {
	Name string

	WorkersInPool int
	Brokers       []string
	Topic         string
	GroupID       string
	BatchSize     int
	BatchDeadline time.Duration

	MinBytes          int
	MaxBytes          int
	CommitInterval    time.Duration
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
	MaxWait           time.Duration
	QueueCapacity     int
	ReaderDownTimeout time.Duration
	Dialer            *kafka.Dialer
}

type Consumer struct {
	configs Configs
	handler func(context.Context, []kafka.Message) error
	reader  *kafka.Reader
}

func NewKafkaConsumer(
	configs Configs,
	handler func(context.Context, []kafka.Message) error,
) *Consumer {
	return &Consumer{
		handler: handler,
		configs: configs,
		reader:  nil,
	}
}

func (kc *Consumer) Run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
				Msg:       "Panic recovered in runConsumer",
				Error:     fmt.Errorf("%v", r),
				Component: "kafka-reader",
				Method:    "runConsumer",
			})
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           kc.configs.Brokers,
		Topic:             kc.configs.Topic,
		GroupID:           kc.configs.GroupID,
		MinBytes:          kc.configs.MinBytes,
		MaxBytes:          kc.configs.MaxBytes,
		CommitInterval:    kc.configs.CommitInterval, // ВАЖНО: ручной коммит (никаких авто-коммитов)
		SessionTimeout:    kc.configs.SessionTimeout,
		HeartbeatInterval: kc.configs.HeartbeatInterval,
		RebalanceTimeout:  kc.configs.RebalanceTimeout,
		MaxWait:           kc.configs.MaxWait,
		QueueCapacity:     kc.configs.QueueCapacity,
		Dialer:            kc.configs.Dialer,
	})

	kc.reader = reader

	defer func() {
		err := kc.rebalance(ctx)
		if err != nil {
			logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
				Msg:       "Failed to rebalance kafka reader on exit",
				Component: "kafka-reader",
				Method:    "runConsumer",
				Error:     err,
			})
		}
	}()

	log.Printf("Consuming topic=%q group=%q brokers=%v ...", kc.configs.Topic, kc.configs.GroupID, kc.configs.Brokers)

	batch := NewBatch(kc.configs.BatchSize, kc.configs.BatchDeadline)

	flush := func(flushCtx context.Context) error {
		messages := batch.MessagesPartition()

		// будем собирать last только для успешно обработанных партиций
		toCommit := make([]kafka.Message, 0, len(messages))

		// обработка сообщений по партициям отдельно
		// чтобы при ошибке обработки одной партиции не блокировать обработку других
		// и не мешать коммитить оффсеты успешно обработанных партиций
		// (иначе может быть ситуация, когда одна проблемная партиция блокирует все остальные)
		for partitionNumber, partitionBatch := range messages {
			if len(partitionBatch) == 0 {
				continue
			}
			err := kc.handler(flushCtx, partitionBatch)
			if err != nil {
				// сделайте свой DLQ-обработчик здесь, если нужно

				// пишем ВСЮ партицию в DLQ, но оффсет НЕ коммитим
				//toDeadLetterBatch := make([]storage.DeadLetterMessage, 0, len(partitionBatch))
				//now := time.Now()
				//for _, m := range partitionBatch {
				//	toDeadLetterBatch = append(toDeadLetterBatch, storage.DeadLetterMessage{
				//		Topic:     kc.configs.Topic,
				//		Partition: m.Partition,
				//		Offset:    m.Offset,
				//		Key:       m.Key,
				//		Value:     m.Value,
				//		Error:     err.Error(),
				//		CreatedAt: now,
				//		Attempts:  0,
				//	})
				//}
				//
				//dlqCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				//if dlqErr := kc.deadLetterQueue.InsertBatch(dlqCtx, toDeadLetterBatch); dlqErr != nil {
				//	logger.WriteErrorLog(flushCtx, &loggerwrapper.LogEntry{
				//		Msg:       "Failed to write messages to DLQ",
				//		Component: "kafka-reader",
				//		Method:    "runConsumer",
				//		Error:     dlqErr,
				//		Args:      map[string]any{"partition": partitionNumber, "batch_size": len(partitionBatch)},
				//	})
				//	cancel()
				//	return dlqErr
				//}
				//cancel()
				// последнее сообщение из набора сообщения для обработки, но упавших в ошибку
				// коммитим, чтобы не застрять на этой партиции и дальше работать через dlq с этими сообщениями

				// комит только если вы сделали DLQ обработку иначе будет потеря сообщений
				//toCommit = append(toCommit, partitionBatch[len(partitionBatch)-1])

				logger.WriteErrorLog(flushCtx, &loggerwrapper.LogEntry{
					Msg:       "Message handling error",
					Component: "kafka-reader",
					Method:    "runConsumer",
					Error:     err,
					Args:      map[string]any{"partition": partitionNumber, "batch_size": len(partitionBatch)},
				})
				continue
			}

			if len(partitionBatch) == 0 {
				continue
			}
			// коммитим последний оффсет этой партиции
			last := partitionBatch[len(partitionBatch)-1]
			toCommit = append(toCommit, last)
		}

		if len(toCommit) > 0 {
			if err := kc.reader.CommitMessages(flushCtx, toCommit...); err != nil {
				logger.WriteErrorLog(flushCtx, &loggerwrapper.LogEntry{
					Msg:       "Failed to commit messages",
					Component: "kafka-reader",
					Method:    "runConsumer",
					Error:     err,
				})
				return err
			}
		}

		batch.Reset()

		return nil
	}

	for {
		readCtx, readCancel := context.WithTimeout(ctx, kc.configs.ReaderDownTimeout)
		message, err := kc.reader.FetchMessage(readCtx)
		readCancel()

		if err != nil {
			// Родительский контекст завершён — выходим
			if ctx.Err() != nil {
				if batch.Size() > 0 {
					localCtx, localCancel := newFlushCtx(ctx, 5*time.Second)
					if flushErr := flush(localCtx); flushErr != nil {
						localCancel()
						log.Printf("fetch error (parent ctx done) with name %s and topic %s and group %s: %v", kc.configs.Name, kc.configs.Topic, kc.configs.GroupID, flushErr)
						return
					}
					localCancel()
				}
				log.Printf("fetch error (parent ctx done) with name %s and topic %s and group %s: %v", kc.configs.Name, kc.configs.Topic, kc.configs.GroupID, err)
				return
			}

			// Локальный таймаут/отмена
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if batch.Size() == 0 {
					continue
				}
				localCtx, localCancel := newFlushCtx(ctx, 5*time.Second)
				if flushErr := flush(localCtx); flushErr != nil {
					localCancel()
					log.Printf("fetch error (deadline exceeded) with name %s and topic %s and group %s: %v", kc.configs.Name, kc.configs.Topic, kc.configs.GroupID, flushErr)
					return
				}
				localCancel()

				continue
			}

			// Любая другая ошибка — ➕ флаш и выход
			if batch.Size() > 0 {
				localCtx, localCancel := newFlushCtx(ctx, 5*time.Second)
				if flushErr := flush(localCtx); flushErr != nil {
					localCancel()
					log.Printf("fetch error (other) with name %s and topic %s and group %s: %v", kc.configs.Name, kc.configs.Topic, kc.configs.GroupID, flushErr)
					return
				}
				localCancel()
			}
			log.Printf("fetch error with name %s and topic %s and group %s: %v", kc.configs.Name, kc.configs.Topic, kc.configs.GroupID, err)
			return
		}

		state := batch.Add(message)
		switch state {
		case BatchFilling:
			continue
		case BatchFull, BatchDeadlineExceeded:
			// батч готов, message уже внутри
		}

		localCtx, localCancel := newFlushCtx(ctx, 5*time.Second)
		err = flush(localCtx)
		if err != nil {
			localCancel()
			return
		}
		localCancel()
	}
}

func newFlushCtx(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return context.WithTimeout(context.Background(), d)
	}
	return context.WithTimeout(parent, d)
}

// вызывается для возврата сообщений в в брокер, когда случаеются ошибки FetchMessage или CommitMessages
// это необходимо, чтобы другой инстанс мог забрать на себя обработку партиций
// так реализован Nack метод в kafkа, других способов "отказаться" от сообщений в kafkа нет
// только отменить консюмер и вернуть в группа партиции брокеру сообщения, которые не были закоммичены
func (kc *Consumer) rebalance(ctx context.Context) error {
	err := kc.reader.Close()
	if err != nil {
		logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
			Msg:       "Failed to close kafka reader on rebalance",
			Component: "kafka-reader",
			Method:    "rebalance",
			Error:     err,
		})
		return err
	}
	logger.WriteInfoLog(ctx, &loggerwrapper.LogEntry{
		Msg:       "Kafka reader closed on rebalance",
		Component: "kafka-reader",
		Method:    "rebalance",
	})

	return nil
}
