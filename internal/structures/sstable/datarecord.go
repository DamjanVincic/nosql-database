package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type DataRecord struct {
	crc       uint32
	timestamp uint64
	tombstone bool
	value     []byte
}

func NewDataRecord(memEntry MemEntry) *DataRecord {

	// Bytes for checksum (9 bytes for timestamp, tombstone)
	bytes := make([]byte, 9)
	binary.BigEndian.PutUint64(bytes, memEntry.Value.Timestamp)
	if memEntry.Value.Tombstone {
		bytes[8] = 1
	} else {
		bytes[8] = 0
	}
	bytes = append(bytes, memEntry.Value.Value...)

	return &DataRecord{
		crc:       crc32.ChecksumIEEE(bytes),
		timestamp: memEntry.Value.Timestamp,
		tombstone: memEntry.Value.Tombstone,
		value:     memEntry.Value.Value,
	}
}

func (record *DataRecord) SerializeDataRecord() []byte {
	bytes := make([]byte, RecordHeaderSize)
	// Append the CRC
	binary.BigEndian.PutUint32(bytes[CrcStart:TimestampStart], record.crc)
	// Append the Timestamp
	binary.BigEndian.PutUint64(bytes[TimestampStart:TombstoneStart], record.timestamp)
	// Append the Tombstone
	if record.tombstone {
		bytes[TombstoneStart] = 1
	} else {
		bytes[TombstoneStart] = 0
	}
	// Append the Value
	copy(bytes[ValueStart:], record.value)

	return bytes
}

func DeserializeDataRecord(bytes []byte) (*DataRecord, error) {
	crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	value := bytes[ValueStart:]

	// Check if the CRC matches
	if crc != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
		return nil, errors.New("CRC does not match")
	}

	return &DataRecord{
		crc:       crc,
		timestamp: timestamp,
		tombstone: tombstone,
		value:     value,
	}, nil
}
