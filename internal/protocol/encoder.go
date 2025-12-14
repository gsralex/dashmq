package protocol

import (
	"encoding/binary"
	"io"
)

// Encoder encodes Kafka binary protocol
type Encoder struct {
	writer io.Writer
	buf    []byte
}

// NewEncoder creates a new encoder
func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		writer: writer,
		buf:    make([]byte, 4096),
	}
}

// WriteInt8 writes an int8
func (e *Encoder) WriteInt8(v int8) error {
	_, err := e.writer.Write([]byte{byte(v)})
	return err
}

// WriteInt16 writes an int16 (big-endian)
func (e *Encoder) WriteInt16(v int16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	_, err := e.writer.Write(b[:])
	return err
}

// WriteInt32 writes an int32 (big-endian)
func (e *Encoder) WriteInt32(v int32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	_, err := e.writer.Write(b[:])
	return err
}

// WriteInt64 writes an int64 (big-endian)
func (e *Encoder) WriteInt64(v int64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	_, err := e.writer.Write(b[:])
	return err
}

// WriteString writes a nullable string
func (e *Encoder) WriteString(s string) error {
	if s == "" {
		return e.WriteInt16(-1) // null string
	}
	if err := e.WriteInt16(int16(len(s))); err != nil {
		return err
	}
	_, err := e.writer.Write([]byte(s))
	return err
}

// WriteBytes writes nullable bytes
func (e *Encoder) WriteBytes(b []byte) error {
	if b == nil {
		return e.WriteInt32(-1) // null bytes
	}
	if err := e.WriteInt32(int32(len(b))); err != nil {
		return err
	}
	_, err := e.writer.Write(b)
	return err
}

// WriteArray writes an array
func (e *Encoder) WriteArray(length int32, fn func(*Encoder, int) error) error {
	if err := e.WriteInt32(length); err != nil {
		return err
	}
	for i := int32(0); i < length; i++ {
		if err := fn(e, int(i)); err != nil {
			return err
		}
	}
	return nil
}

// WriteResponseHeader writes the response header
func (e *Encoder) WriteResponseHeader(correlationID int32) error {
	return e.WriteInt32(correlationID)
}

