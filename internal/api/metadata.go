package api

import (
	"github.com/gsralex/dashmq/internal/protocol"
	"github.com/gsralex/dashmq/internal/storage"
)

// HandleMetadata handles Metadata API requests
func HandleMetadata(decoder *protocol.Decoder, encoder *protocol.Encoder, header *protocol.RequestHeader, broker *storage.Broker) error {
	// Read request
	var topicNames []string
	if err := decoder.ReadArray(func(d *protocol.Decoder) error {
		name, err := d.ReadString()
		if err != nil {
			return err
		}
		topicNames = append(topicNames, name)
		return nil
	}); err != nil {
		return err
	}

	// Write response header
	if err := encoder.WriteResponseHeader(header.CorrelationID); err != nil {
		return err
	}

	// Write brokers array
	if err := encoder.WriteArray(1, func(e *protocol.Encoder, i int) error {
		nodeID := broker.GetNodeID()
		if err := e.WriteInt32(nodeID); err != nil {
			return err
		}
		if err := e.WriteString("localhost"); err != nil {
			return err
		}
		if err := e.WriteInt32(9092); err != nil {
			return err
		}
		// rack (nullable string)
		return e.WriteString("")
	}); err != nil {
		return err
	}

	// Write cluster ID (nullable string)
	if err := encoder.WriteString("dashmq-cluster"); err != nil {
		return err
	}

	// Write controller ID
	if err := encoder.WriteInt32(broker.GetNodeID()); err != nil {
		return err
	}

	// Write topics array
	allTopics := broker.GetAllTopics()
	topicsToReturn := allTopics
	if len(topicNames) > 0 {
		topicsToReturn = topicNames
	}

	if err := encoder.WriteArray(int32(len(topicsToReturn)), func(e *protocol.Encoder, i int) error {
		topicName := topicsToReturn[i]
		topic, exists := broker.GetTopic(topicName)

		// Error code
		errorCode := int16(protocol.ErrNone)
		if !exists && len(topicNames) > 0 {
			errorCode = int16(protocol.ErrUnknownTopicOrPartition)
		}
		if err := e.WriteInt16(errorCode); err != nil {
			return err
		}

		// Topic name
		if err := e.WriteString(topicName); err != nil {
			return err
		}

		// Is internal
		if err := e.WriteInt8(0); err != nil {
			return err
		}

		// Partitions array
		partitionCount := int32(0)
		if topic != nil {
			partitionCount = topic.GetPartitionCount()
			if partitionCount == 0 {
				partitionCount = 1 // default to 1 partition
			}
		}

		if err := e.WriteArray(partitionCount, func(e *protocol.Encoder, j int) error {
			partitionID := int32(j)
			if err := e.WriteInt16(protocol.ErrNone); err != nil {
				return err
			}
			if err := e.WriteInt32(partitionID); err != nil {
				return err
			}
			// Leader
			if err := e.WriteInt32(broker.GetNodeID()); err != nil {
				return err
			}
			// Replicas array
			if err := e.WriteArray(1, func(e *protocol.Encoder, k int) error {
				return e.WriteInt32(broker.GetNodeID())
			}); err != nil {
				return err
			}
			// ISR array
			if err := e.WriteArray(1, func(e *protocol.Encoder, k int) error {
				return e.WriteInt32(broker.GetNodeID())
			}); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
