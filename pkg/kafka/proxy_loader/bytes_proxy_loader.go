package proxy_loader

import (
	"bytes"
	"context"
	"fmt"
	models "github.com/PavelAgarkov/template/internal/service/kafka/model"
	kafka2 "github.com/PavelAgarkov/template/pkg/kafka"

	"github.com/buger/jsonparser"

	"github.com/segmentio/kafka-go"
)

const (
	jsonKeyData     = "data"
	jsonKeyOfficeID = "office_id"
)

func (kl *KafkaLoader) ProcessShkOnPlaceBytesStreamV1(ctx context.Context, body []byte, version string) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context err: %w", ctx.Err())
	}

	producersByOffice := make(map[int64]kafka2.Producer)
	for _, prod := range kl.shardProducers {
		if prod.Entity() == models.ShkOnPlaceEntity {
			producersByOffice[prod.GetTopicMapper()[0].OfficeID] = prod
		}
	}

	// Группируем сразу "сырыми" JSON-объектами
	batchesByOffice := make(shardBatches)
	versionBytes := []byte(version)
	headers := []kafka.Header{{Key: kafka2.Scheme, Value: versionBytes}}

	// Ленивая инициализация батча из пула конкретного продюсера
	ensureBatch := func(office int64) []kafka.Message {
		if b, ok := batchesByOffice[office]; ok {
			return b
		}
		p, ok := producersByOffice[office]
		if !ok {
			return nil // офис не обслуживается — пропускаем
		}
		b := p.Pool().Get().([]kafka.Message)[:0]
		batchesByOffice[office] = b
		return b
	}

	// ожидаемый формат: {"data":[{...},{...},...]}
	var iterErr error
	_, err := jsonparser.ArrayEach(body, func(itemRaw []byte, itemType jsonparser.ValueType, _ int, cbErr error) {
		// return равно пропуску элемента в итерировании
		if iterErr != nil {
			return
		}
		if cbErr != nil {
			iterErr = cbErr
			return
		}
		if itemType != jsonparser.Object {
			return // интересуют только объекты
		}

		office, err := jsonparser.GetInt(itemRaw, jsonKeyOfficeID)
		if err != nil {
			return
		}

		if office < 1 {
			return
		}

		batch := ensureBatch(office)
		if batch == nil {
			return // офис не из поддерживаемых
		}

		// Кладём байты объекта как Value (без маршала)
		batch = append(batch, kafka.Message{
			Value:   itemRaw,
			Headers: headers,
		})
		batchesByOffice[office] = batch
	}, jsonKeyData)
	if iterErr != nil {
		return fmt.Errorf("iterate data error: %w", iterErr)
	}
	if err != nil {
		return fmt.Errorf("array iterate error: %w", err)
	}

	if len(batchesByOffice) == 0 {
		return nil
	}

	shardHash := keyBalancer()

	if err := kl.sendToTopic(ctx, producersByOffice, batchesByOffice, shardHash); err != nil {
		return fmt.Errorf("failed to send shk messages: %w", err)
	}

	return nil
}

func (kl *KafkaLoader) ProcessTareMoveBytesStreamV1(ctx context.Context, body []byte, version string) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context err: %w", ctx.Err())
	}

	producersByOffice := make(map[int64]kafka2.Producer)
	for _, prod := range kl.shardProducers {
		if prod.Entity() == models.TareMoveEntity {
			producersByOffice[prod.GetTopicMapper()[0].OfficeID] = prod
		}
	}

	for _, prod := range kl.mainProducers {
		if prod.Entity() == models.TareMoveEntity {
			producersByOffice[0] = prod
			break
		}
	}

	// Группируем сразу "сырыми" JSON-объектами (raw bytes элемента массива)
	batchesByOffice := make(shardBatches)
	versionBytes := []byte(version)
	headers := []kafka.Header{{Key: kafka2.Scheme, Value: versionBytes}}

	// Ленивая инициализация батча из пула конкретного продюсера
	ensureBatch := func(office int64) []kafka.Message {
		if b, ok := batchesByOffice[office]; ok {
			return b
		}
		p, ok := producersByOffice[office]
		if !ok {
			return nil // офис не обслуживается — пропускаем
		}
		b := p.Pool().Get().([]kafka.Message)[:0]
		batchesByOffice[office] = b
		return b
	}

	// Ожидаемый формат входа: top-level массив: [{"office_id":...}, {...}, ...]
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return fmt.Errorf("invalid json: expected top-level array")
	}

	// Итерируем массив без полного распарса, берём только office_id
	var iterErr error
	_, err := jsonparser.ArrayEach(trimmed, func(itemRaw []byte, itemType jsonparser.ValueType, _ int, cbErr error) {
		// return == пропуск элемента
		if iterErr != nil {
			return
		}
		if cbErr != nil {
			iterErr = cbErr
			return
		}
		if itemType != jsonparser.Object {
			return // интересуют только объекты
		}

		office, err := jsonparser.GetInt(itemRaw, jsonKeyOfficeID)
		if err != nil {
			return
		}

		if office < 1 {
			return
		}

		batch := ensureBatch(office)
		if batch == nil {
			return // офис не из поддерживаемых
		}

		// Кладём байты объекта как Value
		batch = append(batch, kafka.Message{
			Value:   itemRaw,
			Headers: headers,
		})
		batchesByOffice[office] = batch

		if _, ok := producersByOffice[kafka2.MainTopic]; ok {
			batchesByOffice[kafka2.MainTopic] = append(batchesByOffice[kafka2.MainTopic], kafka.Message{
				Value:   itemRaw,
				Headers: headers,
			})
		}
	})
	if iterErr != nil {
		return fmt.Errorf("iterate array error: %w", iterErr)
	}
	if err != nil {
		return fmt.Errorf("array iterate error: %w", err)
	}

	// Нечего отправлять
	if len(batchesByOffice) == 0 {
		return nil
	}

	shardHash := keyBalancer()

	if err := kl.sendToTopic(ctx, producersByOffice, batchesByOffice, shardHash); err != nil {
		return fmt.Errorf("failed to send tare messages: %w", err)
	}

	return nil
}
