package api

import (
	"github.com/gsralex/dashmq/internal/protocol"
	"github.com/gsralex/dashmq/internal/storage"
)

// HandleFetch handles Fetch API requests
func HandleFetch(decoder *protocol.Decoder, encoder *protocol.Encoder, header *protocol.RequestHeader, broker *storage.Broker) error {
	// Read replica ID
	if _, err := decoder.ReadInt32(); err != nil {
		return err
	}

	// Read max wait time
	if _, err := decoder.ReadInt32(); err != nil {
		return err
	}

	// Read min bytes
	if _, err := decoder.ReadInt32(); err != nil {
		return err
	}

	// Read max bytes
	maxBytes, err := decoder.ReadInt32()
	if err != nil {
		return err
	}

	// Read isolation level
	if _, err := decoder.ReadInt8(); err != nil {
		return err
	}

	// Read topic array
	var topicResponses []topicFetchResponse
	if err := decoder.ReadArray(func(d *protocol.Decoder) error {
		topicName, err := d.ReadString()
		if err != nil {
			return err
		}

		// Read partition array
		var partitionResponses []partitionFetchResponse
		if err := d.ReadArray(func(d *protocol.Decoder) error {
			partitionID, err := d.ReadInt32()
			if err != nil {
				return err
			}

			// Read fetch offset
			fetchOffset, err := d.ReadInt64()
			if err != nil {
				return err
			}

			// Read partition max bytes
			partitionMaxBytes, err := d.ReadInt32()
			if err != nil {
				return err
			}

			// Fetch messages
			topic, exists := broker.GetTopic(topicName)
			errorCode := int16(protocol.ErrNone)
			var messages []*storage.Message
			highWatermark := int64(0)
			lastStableOffset := int64(0)

			if !exists {
				errorCode = int16(protocol.ErrUnknownTopicOrPartition)
			} else {
				partition := topic.GetPartition(partitionID)
				messages, highWatermark = partition.Fetch(fetchOffset, partitionMaxBytes)
				lastStableOffset = highWatermark
			}

			partitionResponses = append(partitionResponses, partitionFetchResponse{
				PartitionID:      partitionID,
				ErrorCode:        errorCode,
				HighWatermark:    highWatermark,
				LastStableOffset: lastStableOffset,
				Messages:         messages,
			})

			return nil
		}); err != nil {
			return err
		}

		topicResponses = append(topicResponses, topicFetchResponse{
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

	// Write throttle time
	if err := encoder.WriteInt32(0); err != nil {
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
			if err := e.WriteInt64(pr.HighWatermark); err != nil {
				return err
			}
			if err := e.WriteInt64(pr.LastStableOffset); err != nil {
				return err
			}

			// Write message set
			if err := e.WriteArray(int32(len(pr.Messages)), func(e *protocol.Encoder, k int) error {
				msg := pr.Messages[k]
				// Write record batch (simplified)
				// In a full implementation, this would properly encode the record batch
				if err := e.WriteInt64(msg.Offset); err != nil {
					return err
				}
				// Message size placeholder
				msgSize := int32(14 + 4 + len(msg.Key) + 4 + len(msg.Value))
				if err := e.WriteInt32(msgSize); err != nil {
					return err
				}
				// CRC32 (placeholder)
				if err := e.WriteInt32(0); err != nil {
					return err
				}
				// Magic byte (v1)
				if err := e.WriteInt8(1); err != nil {
					return err
				}
				// Attributes
				if err := e.WriteInt8(0); err != nil {
					return err
				}
				// Timestamp
				if err := e.WriteInt64(msg.Timestamp); err != nil {
					return err
				}
				// Key
				if err := e.WriteBytes(msg.Key); err != nil {
					return err
				}
				// Value
				if err := e.WriteBytes(msg.Value); err != nil {
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
	}); err != nil {
		return err
	}

	_ = maxBytes
	return nil
}

type topicFetchResponse struct {
	Topic      string
	Partitions []partitionFetchResponse
}

type partitionFetchResponse struct {
	PartitionID      int32
	ErrorCode        int16
	HighWatermark    int64
	LastStableOffset int64
	Messages         []*storage.Message
}
