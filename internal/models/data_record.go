package models

import (
	"encoding/binary"
)

const (
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	ValueSizeSize = 8

	TimestampStart = 0
	TombstoneStart = TimestampStart + TimestampSize
	KeySizeStart   = TombstoneStart + TombstoneSize
	ValueSizeStart = KeySizeStart + KeySizeSize
	KeyStart       = ValueSizeStart + ValueSizeSize

	RecordHeaderSize = TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize
)

// DataRecord struct for SSTable that wraps Data, KeySize and ValueSize
type DataRecord struct {
	Data      *Data
	KeySize   uint64
	ValueSize uint64
}

func NewDataRecord(data *Data) *DataRecord {
	return &DataRecord{
		Data:      data,
		KeySize:   uint64(len(data.Key)),
		ValueSize: uint64(len(data.Value)),
	}
}

func (dataRecord *DataRecord) Serialize() []byte {
	bytes := make([]byte, RecordHeaderSize+dataRecord.KeySize+dataRecord.ValueSize)

	// Append the Timestamp
	binary.BigEndian.PutUint64(bytes[TimestampStart:TombstoneStart], dataRecord.Data.Timestamp)
	// Append the Tombstone
	if dataRecord.Data.Tombstone {
		bytes[TombstoneStart] = 1
	} else {
		bytes[TombstoneStart] = 0
	}
	// Append the Key Size
	binary.BigEndian.PutUint64(bytes[KeySizeStart:ValueSizeStart], dataRecord.KeySize)
	// Append the Value Size
	binary.BigEndian.PutUint64(bytes[ValueSizeStart:KeyStart], dataRecord.ValueSize)
	// Append the Key
	copy(bytes[KeyStart:KeyStart+dataRecord.KeySize], dataRecord.Data.Key)
	// Append the Value
	copy(bytes[KeyStart+dataRecord.KeySize:], dataRecord.Data.Value)

	return bytes
}

func Deserialize(bytes []byte) *DataRecord {
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:ValueSizeStart])
	valueSize := binary.BigEndian.Uint64(bytes[ValueSizeStart:KeyStart])
	key := string(bytes[KeyStart : KeyStart+keySize])

	var value = make([]byte, valueSize)
	copy(value, bytes[KeyStart+keySize:])

	return &DataRecord{
		Data: &Data{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
			Timestamp: timestamp,
		},
		KeySize:   keySize,
		ValueSize: valueSize,
	}
}
