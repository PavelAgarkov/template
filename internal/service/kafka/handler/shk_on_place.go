package handler

import (
	"context"
	"log"

	models "github.com/PavelAgarkov/template/internal/service/kafka/model"

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
			log.Printf("Failed to unmarshal message: %v, error: %v", string(msg.Value), err)
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

		log.Printf("Processed SHK event: %+v", shkEvent)
	}

	if len(events) == 0 {
		log.Printf("No valid SHK events to process")
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
				log.Printf("Failed to process SHK events: %v", err)
				return err
			}
		}
	}

	return nil
}

func (h *ShkOnPlaceHandler) processEventsV1(ctx context.Context, shkOnPlaceEvents []models.Shk) error {
	return nil
}
