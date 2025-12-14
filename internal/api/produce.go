package api

import (
	"time"

	"github.com/gsralex/dashmq/internal/protocol"
	"github.com/gsralex/dashmq/internal/storage"
)

// HandleProduce handles Produce API requests
func HandleProduce(decoder *protocol.Decoder, encoder *protocol.Encoder, header *protocol.RequestHeader, broker *storage.Broker) error {
	// Read transactional ID (nullable string)
	if _, err := decoder.ReadString(); err != nil {
		return err
	}

	// Read acks
	acks, err := decoder.ReadInt16()
	if err != nil {
		return err
	}

	// Read timeout
	timeout, err := decoder.ReadInt32()
	if err != nil {
		return err
	}
	_ = timeout

	// Read topic data array
	var topicResponses []topicProduceResponse
	if err := decoder.ReadArray(func(d *protocol.Decoder) error {
		topicName, err := d.ReadString()
		if err != nil {
			return err
		}

		// Read partition data array
		var partitionResponses []partitionProduceResponse
		if err := d.ReadArray(func(d *protocol.Decoder) error {
			partitionID, err := d.ReadInt32()
			if err != nil {
				return err
			}

			// Read message set
			messageSet, err := d.ReadBytes()
			if err != nil {
				return err
			}

			// Parse and store messages
			topic := broker.GetOrCreateTopic(topicName)
			partition := topic.GetPartition(partitionID)

			offset, err := parseAndStoreMessages(messageSet, partition)
			if err != nil {
				return err
			}

			partitionResponses = append(partitionResponses, partitionProduceResponse{
				PartitionID: partitionID,
				ErrorCode:   protocol.ErrNone,
				Offset:      offset,
			})

			return nil
		}); err != nil {
			return err
		}

		topicResponses = append(topicResponses, topicProduceResponse{
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
			if err := e.WriteInt64(pr.Offset); err != nil {
				return err
			}
			// Timestamp (for log append time)
			if err := e.WriteInt64(time.Now().UnixMilli()); err != nil {
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

	// Write throttle time
	if err := encoder.WriteInt32(0); err != nil {
		return err
	}

	_ = acks
	return nil
}

type topicProduceResponse struct {
	Topic      string
	Partitions []partitionProduceResponse
}

type partitionProduceResponse struct {
	PartitionID int32
	ErrorCode   int16
	Offset      int64
}

// parseAndStoreMessages parses message set and stores messages
func parseAndStoreMessages(data []byte, partition *storage.Partition) (int64, error) {
	if len(data) == 0 {
		return partition.GetHighWatermark(), nil
	}

	// Simple message parsing (for v0/v1 message format)
	// In a full implementation, this would properly parse the message set
	pos := 0

	for pos < len(data) {
		if pos+8 > len(data) {
			break
		}

		// Read offset (int64) - skip, we'll use partition's offset
		_ = int64(data[pos])<<56 | int64(data[pos+1])<<48 | int64(data[pos+2])<<40 | int64(data[pos+3])<<32 |
			int64(data[pos+4])<<24 | int64(data[pos+5])<<16 | int64(data[pos+6])<<8 | int64(data[pos+7])
		pos += 8

		if pos+4 > len(data) {
			break
		}

		// Read message size (int32)
		msgSize := int32(data[pos])<<24 | int32(data[pos+1])<<16 | int32(data[pos+2])<<8 | int32(data[pos+3])
		pos += 4

		if pos+int(msgSize) > len(data) {
			break
		}

		// Parse message
		msgData := data[pos : pos+int(msgSize)]
		pos += int(msgSize)

		// Parse message (simplified - assumes v0/v1 format)
		if len(msgData) < 14 {
			continue
		}

		// CRC32 (4 bytes) - skip
		// Magic byte (1 byte)
		magic := msgData[4]
		// Attributes (1 byte)
		// Timestamp (8 bytes for v1, 0 for v0)
		timestamp := time.Now().UnixMilli()
		if magic >= 1 {
			if len(msgData) >= 14 {
				timestamp = int64(msgData[6])<<56 | int64(msgData[7])<<48 | int64(msgData[8])<<40 | int64(msgData[9])<<32 |
					int64(msgData[10])<<24 | int64(msgData[11])<<16 | int64(msgData[12])<<8 | int64(msgData[13])
			}
		}

		keyOffset := 14
		if magic == 0 {
			keyOffset = 6
		}

		// Read key
		var key []byte
		if keyOffset < len(msgData) {
			keyLen := int32(msgData[keyOffset])<<24 | int32(msgData[keyOffset+1])<<16 | int32(msgData[keyOffset+2])<<8 | int32(msgData[keyOffset+3])
			keyOffset += 4
			if keyLen > 0 && keyOffset+int(keyLen) <= len(msgData) {
				key = msgData[keyOffset : keyOffset+int(keyLen)]
				keyOffset += int(keyLen)
			}
		}

		// Read value
		var value []byte
		if keyOffset < len(msgData) {
			valueLen := int32(msgData[keyOffset])<<24 | int32(msgData[keyOffset+1])<<16 | int32(msgData[keyOffset+2])<<8 | int32(msgData[keyOffset+3])
			keyOffset += 4
			if valueLen > 0 && keyOffset+int(valueLen) <= len(msgData) {
				value = msgData[keyOffset : keyOffset+int(valueLen)]
			}
		}

		// Store message
		partition.Append(key, value, timestamp)
	}

	return partition.GetHighWatermark(), nil
}
