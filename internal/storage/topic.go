package storage

import (
	"sync"
)

// Topic represents a Kafka topic
type Topic struct {
	Name       string
	Partitions map[int32]*Partition
	mu         sync.RWMutex
}

// NewTopic creates a new topic
func NewTopic(name string) *Topic {
	return &Topic{
		Name:       name,
		Partitions: make(map[int32]*Partition),
	}
}

// GetPartition gets or creates a partition
func (t *Topic) GetPartition(partitionID int32) *Partition {
	t.mu.Lock()
	defer t.mu.Unlock()

	if p, ok := t.Partitions[partitionID]; ok {
		return p
	}

	p := NewPartition(t.Name, partitionID)
	t.Partitions[partitionID] = p
	return p
}

// GetPartitionCount returns the number of partitions
func (t *Topic) GetPartitionCount() int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int32(len(t.Partitions))
}
