package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type BatchState int

const (
	BatchFilling BatchState = iota
	BatchFull
	BatchDeadlineExceeded
)

type Batch struct {
	created time.Time
	ttl     time.Duration
	maxSize int
	size    int
	parts   map[int][]kafka.Message
	last    map[int]kafka.Message // обновляем на каждом Add
}

func NewBatch(max int, ttl time.Duration) *Batch {
	return &Batch{
		created: time.Now(),
		ttl:     ttl,
		maxSize: max,
		parts:   make(map[int][]kafka.Message),
		last:    make(map[int]kafka.Message),
	}
}

func (b *Batch) Size() int {
	return b.size
}

func (b *Batch) MessagesPartition() map[int][]kafka.Message {
	return b.parts
}

func (b *Batch) Messages() []kafka.Message {
	out := make([]kafka.Message, 0, b.size)
	for _, messages := range b.parts {
		out = append(out, messages...)
	}
	return out
}

func (b *Batch) Add(m kafka.Message) BatchState {
	b.parts[m.Partition] = append(b.parts[m.Partition], m)
	if lm, ok := b.last[m.Partition]; !ok || m.Offset > lm.Offset {
		b.last[m.Partition] = m
	}
	b.size++
	if b.size >= b.maxSize {
		return BatchFull
	}
	if b.ttl > 0 && time.Since(b.created) >= b.ttl {
		return BatchDeadlineExceeded
	}
	return BatchFilling
}

func (b *Batch) LastPerPartition() []kafka.Message {
	out := make([]kafka.Message, 0, len(b.last))
	for _, m := range b.last {
		out = append(out, m)
	}
	return out
}

func (b *Batch) Reset() {
	*b = *NewBatch(b.maxSize, b.ttl)
}
