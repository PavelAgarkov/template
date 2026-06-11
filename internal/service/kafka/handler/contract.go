package handler

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Contract interface {
	Handle(ctx context.Context, messages []kafka.Message) error
}

const (
	Scheme   = "scheme"
	SchemeV1 = "v1"
)

func IsV1Message(headers []kafka.Header) bool {
	for _, header := range headers {
		switch {
		case string(header.Key) == Scheme && string(header.Value) == SchemeV1:
			return true
		default:
			continue
		}
	}
	return false
}

type Version interface {
	Version() string
	GetMessages() []interface{}
	SetMessages(messages []interface{})
	SetMessage(message interface{})
}

type ShkV1VersionDto struct {
	v        string
	messages []interface{}
}

func NewVersionDto(version string, messages []interface{}) Version {
	return &ShkV1VersionDto{
		v:        version,
		messages: messages,
	}
}

func (v *ShkV1VersionDto) Version() string {
	return v.v
}

func (v *ShkV1VersionDto) GetMessages() []interface{} {
	return v.messages
}

func (v *ShkV1VersionDto) SetMessages(messages []interface{}) {
	v.messages = messages
}
func (v *ShkV1VersionDto) SetMessage(message interface{}) {
	v.messages = append(v.messages, message)
}
