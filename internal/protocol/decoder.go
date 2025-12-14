package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Decoder decodes Kafka binary protocol
type Decoder struct {
	reader io.Reader
	buf    []byte
}

// NewDecoder creates a new decoder
func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{
		reader: reader,
		buf:    make([]byte, 4096),
	}
}

// ReadInt8 reads an int8
func (d *Decoder) ReadInt8() (int8, error) {
	var b [1]byte
	if _, err := io.ReadFull(d.reader, b[:]); err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

// ReadInt16 reads an int16 (big-endian)
func (d *Decoder) ReadInt16() (int16, error) {
	var b [2]byte
	if _, err := io.ReadFull(d.reader, b[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b[:])), nil
}

// ReadInt32 reads an int32 (big-endian)
func (d *Decoder) ReadInt32() (int32, error) {
	var b [4]byte
	if _, err := io.ReadFull(d.reader, b[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b[:])), nil
}

// ReadInt64 reads an int64 (big-endian)
func (d *Decoder) ReadInt64() (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(d.reader, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

// ReadString reads a nullable string
func (d *Decoder) ReadString() (string, error) {
	length, err := d.ReadInt16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil // null string
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(d.reader, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// ReadBytes reads nullable bytes
func (d *Decoder) ReadBytes() ([]byte, error) {
	length, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil // null bytes
	}
	if length == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(d.reader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadArray reads an array
func (d *Decoder) ReadArray(fn func(*Decoder) error) error {
	length, err := d.ReadInt32()
	if err != nil {
		return err
	}
	if length < 0 {
		return errors.New("negative array length")
	}
	for i := int32(0); i < length; i++ {
		if err := fn(d); err != nil {
			return err
		}
	}
	return nil
}

// ReadRequestHeader reads the request header
func (d *Decoder) ReadRequestHeader() (*RequestHeader, error) {
	apiKey, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}

	apiVersion, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}

	correlationID, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}

	clientID, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	return &RequestHeader{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, nil
}
