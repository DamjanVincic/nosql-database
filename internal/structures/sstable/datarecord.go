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

	// Bytes for checksum (9 bytes for Timestamp, Tombstone)
	bytes := make([]byte, 9)
	binary.BigEndian.PutUint64(bytes, memEntry.Value.Timestamp)
	if memEntry.Value.Tombstone {
		bytes[8] = 1
	} else {
		bytes[8] = 0
		bytes = append(bytes, memEntry.Value.Value...)
	}

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
		return bytes
	} else {
		bytes[TombstoneStart] = 0
	}
	// Append the Value
	bytes = append(bytes, record.value...)

	return bytes
}

func DeserializeDataRecord(bytes []byte) (*DataRecord, error) {
	Crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	Timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	Tombstone := bytes[TombstoneStart] == 1
	var Value []byte
	if !Tombstone {
		Value = bytes[ValueStart:]
	}

	// Check if the CRC matches
	if Crc != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
		return nil, errors.New("CRC does not match")
	}

	return &DataRecord{
		crc:       Crc,
		timestamp: Timestamp,
		tombstone: Tombstone,
		value:     Value,
	}, nil
}
