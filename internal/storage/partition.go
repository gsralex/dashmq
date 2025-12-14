package storage

import (
	"sync"
	"sync/atomic"
)

// Partition represents a Kafka partition
type Partition struct {
	Topic     string
	Partition int32
	Messages  []*Message
	mu        sync.RWMutex
	offset    int64
}

// Message represents a Kafka message
type Message struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64
}

// NewPartition creates a new partition
func NewPartition(topic string, partitionID int32) *Partition {
	return &Partition{
		Topic:     topic,
		Partition: partitionID,
		Messages:  make([]*Message, 0),
		offset:    0,
	}
}

// Append appends a message to the partition
func (p *Partition) Append(key, value []byte, timestamp int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := atomic.AddInt64(&p.offset, 1) - 1
	msg := &Message{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}
	p.Messages = append(p.Messages, msg)
	return offset
}

// Fetch fetches messages from the partition
func (p *Partition) Fetch(offset int64, maxBytes int32) ([]*Message, int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 {
		offset = 0
	}

	if int64(len(p.Messages)) <= offset {
		return []*Message{}, p.offset
	}

	messages := make([]*Message, 0)
	totalBytes := int32(0)
	nextOffset := offset

	for i := offset; i < int64(len(p.Messages)); i++ {
		msg := p.Messages[i]
		msgSize := int32(len(msg.Key) + len(msg.Value) + 50) // approximate overhead

		if totalBytes+msgSize > maxBytes && len(messages) > 0 {
			break
		}

		messages = append(messages, msg)
		totalBytes += msgSize
		nextOffset = i + 1
	}

	return messages, nextOffset
}

// GetHighWatermark returns the high watermark (last offset + 1)
func (p *Partition) GetHighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.offset
}

// GetLogEndOffset returns the log end offset
func (p *Partition) GetLogEndOffset() int64 {
	return p.GetHighWatermark()
}

