package handler

import (
	"context"
	models "github.com/PavelAgarkov/template/internal/service/kafka/model"

	loggerwrapper "github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"

	"github.com/bytedance/sonic"
	"github.com/segmentio/kafka-go"
)

type TareMoveHandler struct {
}

func NewTareMoveHandler() Contract {
	handler := &TareMoveHandler{}
	return handler
}

func (h *TareMoveHandler) Handle(ctx context.Context, messages []kafka.Message) error {
	events := make(map[string]Version)
	for _, msg := range messages {
		var tareMoveEvent models.WhTare
		err := sonic.Unmarshal(msg.Value, &tareMoveEvent)
		if err != nil {
			logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
				Msg:       "Failed to unmarshal message",
				Component: "kafka-reader",
				Method:    "messageHandler",
				Error:     err,
				Args:      map[string]any{"message_offset": msg.Offset, "message_partition": msg.Partition},
			})
			continue
		}

		switch {
		case IsV1Message(msg.Headers):
			if _, exists := events[SchemeV1]; !exists {
				events[SchemeV1] = NewVersionDto(SchemeV1, make([]interface{}, 0))
			}
			events[SchemeV1].SetMessage(tareMoveEvent)
		default:
			continue
		}

		logger.WriteInfoLog(ctx, &loggerwrapper.LogEntry{
			Msg:       "Processed tare-move event",
			Component: "kafka-reader",
			Method:    "messageHandler",
			Args:      map[string]any{"message_offset": msg.Offset, "message_partition": msg.Partition},
		})
	}

	if len(events) == 0 {
		return nil
	}

	for _, version := range events {
		switch {
		case version.Version() == SchemeV1:
			forProcessMessages := make([]models.WhTare, 0, len(version.GetMessages()))
			for _, msg := range version.GetMessages() {
				if tareMsg, ok := msg.(models.WhTare); ok {
					forProcessMessages = append(forProcessMessages, tareMsg)
				}
			}
			if err := h.processEventsV1(ctx, forProcessMessages); err != nil {
				logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
					Msg:       "Failed to process V1 tare move events",
					Component: "kafka-reader",
					Method:    "messageHandler",
					Error:     err,
				})
				return err
			}
		}
	}

	return nil
}

func (h *TareMoveHandler) processEventsV1(ctx context.Context, tareMoveEvents []models.WhTare) error {
	return nil
}
