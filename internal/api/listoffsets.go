package api

import (
	"github.com/gsralex/dashmq/internal/protocol"
	"github.com/gsralex/dashmq/internal/storage"
)

// HandleListOffsets handles ListOffsets API requests
func HandleListOffsets(decoder *protocol.Decoder, encoder *protocol.Encoder, header *protocol.RequestHeader, broker *storage.Broker) error {
	// Read replica ID
	if _, err := decoder.ReadInt32(); err != nil {
		return err
	}

	// Read isolation level
	if _, err := decoder.ReadInt8(); err != nil {
		return err
	}

	// Read topic array
	var topicResponses []topicListOffsetsResponse
	if err := decoder.ReadArray(func(d *protocol.Decoder) error {
		topicName, err := d.ReadString()
		if err != nil {
			return err
		}

		// Read partition array
		var partitionResponses []partitionListOffsetsResponse
		if err := d.ReadArray(func(d *protocol.Decoder) error {
			partitionID, err := d.ReadInt32()
			if err != nil {
				return err
			}

			// Read timestamp
			timestamp, err := d.ReadInt64()
			if err != nil {
				return err
			}

			// Get offset
			topic, exists := broker.GetTopic(topicName)
			errorCode := int16(protocol.ErrNone)
			offset := int64(0)

			if !exists {
				errorCode = int16(protocol.ErrUnknownTopicOrPartition)
			} else {
				partition := topic.GetPartition(partitionID)
				// timestamp: -1 = earliest, -2 = latest
				if timestamp == -1 {
					offset = 0
				} else if timestamp == -2 {
					offset = partition.GetHighWatermark()
				} else {
					// For specific timestamp, return latest offset (simplified)
					offset = partition.GetHighWatermark()
				}
			}

			partitionResponses = append(partitionResponses, partitionListOffsetsResponse{
				PartitionID: partitionID,
				ErrorCode:   errorCode,
				Timestamp:   timestamp,
				Offset:      offset,
			})

			return nil
		}); err != nil {
			return err
		}

		topicResponses = append(topicResponses, topicListOffsetsResponse{
			Topic:      topicName,
			Partitions: partitionResponses,
		})

		return nil
	}); err != nil {
		return err
	}

	// Write response header
	if err := encoder.WriteResponseHeader(header.CorrelationID); err != nil {
		return err
	}

	// Write responses array
	if err := encoder.WriteArray(int32(len(topicResponses)), func(e *protocol.Encoder, i int) error {
		tr := topicResponses[i]
		if err := e.WriteString(tr.Topic); err != nil {
			return err
		}

		if err := e.WriteArray(int32(len(tr.Partitions)), func(e *protocol.Encoder, j int) error {
			pr := tr.Partitions[j]
			if err := e.WriteInt32(pr.PartitionID); err != nil {
				return err
			}
			if err := e.WriteInt16(pr.ErrorCode); err != nil {
				return err
			}
			if err := e.WriteInt64(pr.Timestamp); err != nil {
				return err
			}
			if err := e.WriteInt64(pr.Offset); err != nil {
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

type topicListOffsetsResponse struct {
	Topic      string
	Partitions []partitionListOffsetsResponse
}

type partitionListOffsetsResponse struct {
	PartitionID int32
	ErrorCode   int16
	Timestamp   int64
	Offset      int64
}
