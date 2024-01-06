package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type dataRecord struct {
	Crc       uint32
	Timestamp uint64
	Tombstone bool
	Value     []byte
}

func NewDataRecord(memEntry MemEntry) *dataRecord {

	// Bytes for checksum (9 bytes for timestamp, tombstone)
	bytes := make([]byte, 9)
	binary.BigEndian.PutUint64(bytes, memEntry.Value.Timestamp)
	if memEntry.Value.Tombstone {
		bytes[8] = 1
	} else {
		bytes[8] = 0
	}
	bytes = append(bytes, memEntry.Value.Value...)

	return &dataRecord{
		Crc:       crc32.ChecksumIEEE(bytes),
		Timestamp: memEntry.Value.Timestamp,
		Tombstone: memEntry.Value.Tombstone,
		Value:     memEntry.Value.Value,
	}
}

func (record *dataRecord) SerializeDataRecord() []byte {
	bytes := make([]byte, RecordHeaderSize)
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
	// Append the Value
	copy(bytes[ValueStart:], record.Value)

	return bytes
}

func DeserializeDataRecord(bytes []byte) (*dataRecord, error) {
	crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	value := bytes[ValueStart:]

	// Check if the CRC matches
	if crc != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
		return nil, errors.New("CRC does not match")
	}

	return &dataRecord{
		Crc:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		Value:     value,
	}, nil
}
