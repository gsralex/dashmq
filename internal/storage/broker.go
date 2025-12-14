package storage

import (
	"sync"
)

// Broker manages topics and partitions
type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex
	nodeID int32
}

// NewBroker creates a new broker
func NewBroker(nodeID int32) *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
		nodeID: nodeID,
	}
}

// GetOrCreateTopic gets or creates a topic
func (b *Broker) GetOrCreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if topic, ok := b.topics[name]; ok {
		return topic
	}

	topic := NewTopic(name)
	b.topics[name] = topic
	return topic
}

// GetTopic gets a topic
func (b *Broker) GetTopic(name string) (*Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topic, ok := b.topics[name]
	return topic, ok
}

// GetAllTopics returns all topics
func (b *Broker) GetAllTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.topics))
	for name := range b.topics {
		topics = append(topics, name)
	}
	return topics
}

// GetNodeID returns the broker node ID
func (b *Broker) GetNodeID() int32 {
	return b.nodeID
}

