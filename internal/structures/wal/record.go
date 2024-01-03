package wal

import (
	"encoding/binary"
	"errors"
	"time"
)

type Record struct {
	Crc       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func NewRecord(key string, value []byte, tombstone bool) *Record {
	// Bytes for checksum
	bytes := make([]byte, 0)
	bytes = append(bytes, []byte(key)...)
	bytes = append(bytes, value...)

	return &Record{
		Crc:       CRC32(bytes),
		Timestamp: uint64(time.Now().Unix()),
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
}

func (record *Record) Serialize() []byte {
	bytes := make([]byte, RecordHeaderSize+record.KeySize+record.ValueSize)
	// Append the CRC
	binary.BigEndian.PutUint32(bytes[CrcStart:TimestampStart], record.Crc)
	// Append the Timestamp
	binary.BigEndian.PutUint64(bytes[TimestampStart:TombstoneStart], record.Timestamp)
	// Append the Tombstone
	if record.Tombstone {
		bytes[TombstoneStart] = 1
	} else {
		bytes[TombstoneStart] = 0
	}
	// Append the Key Size
	binary.BigEndian.PutUint64(bytes[KeySizeStart:ValueSizeStart], record.KeySize)
	// Append the Value Size
	binary.BigEndian.PutUint64(bytes[ValueSizeStart:KeyStart], record.ValueSize)
	// Append the Key
	copy(bytes[KeyStart:KeyStart+record.KeySize], record.Key)
	// Append the Value
	copy(bytes[KeyStart+record.KeySize:], record.Value)

	return bytes
}

func Deserialize(bytes []byte) (*Record, error) {
	crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:ValueSizeStart])
	valueSize := binary.BigEndian.Uint64(bytes[ValueSizeStart:KeyStart])
	key := string(bytes[KeyStart : KeyStart+keySize])

	var value = make([]byte, valueSize)
	copy(value, bytes[KeyStart+keySize:])

	// Check if the CRC matches
	crcValue := make([]byte, 0)
	crcValue = append(crcValue, []byte(key)...)
	crcValue = append(crcValue, value...)
	if crc != CRC32(crcValue) {
		return nil, errors.New("CRC does not match")
	}

	return &Record{
		Crc:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}, nil
}
