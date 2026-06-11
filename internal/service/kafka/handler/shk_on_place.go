package handler

import (
	"context"
	"fmt"
	models "github.com/PavelAgarkov/template/internal/service/kafka/model"

	loggerwrapper "github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/bytedance/sonic"
	"github.com/segmentio/kafka-go"
)

type ShkOnPlaceHandler struct {
}

func NewShkOnPlaceHandler() Contract {
	handler := &ShkOnPlaceHandler{}
	return handler
}

func (h *ShkOnPlaceHandler) Handle(ctx context.Context, messages []kafka.Message) error {
	events := make(map[string]Version)
	for _, msg := range messages {
		var shkEvent models.Shk
		err := sonic.Unmarshal(msg.Value, &shkEvent)
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
			events[SchemeV1].SetMessage(shkEvent)
		default:
			continue
		}

		logger.WriteInfoLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("Processed SHK event: %+v", shkEvent),
			Component: "kafka-reader",
			Method:    "messageHandler",
			Args:      map[string]any{"message_offset": msg.Offset, "message_partition": msg.Partition},
		})
	}

	if len(events) == 0 {
		logger.WriteWarnLog(ctx, &loggerwrapper.LogEntry{
			Msg:       "No valid SHK events to process",
			Component: "kafka-reader",
			Method:    "messageHandler",
		})
		return nil
	}

	for _, version := range events {
		switch {
		case version.Version() == SchemeV1:
			forProcessMessages := make([]models.Shk, 0, len(version.GetMessages()))
			for _, msg := range version.GetMessages() {
				if shkMsg, ok := msg.(models.Shk); ok {
					forProcessMessages = append(forProcessMessages, shkMsg)
				}
			}
			if err := h.processEventsV1(ctx, forProcessMessages); err != nil {
				logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
					Msg:       "Failed to process SHK events",
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

func (h *ShkOnPlaceHandler) processEventsV1(ctx context.Context, shkOnPlaceEvents []models.Shk) error {
	return nil
}
