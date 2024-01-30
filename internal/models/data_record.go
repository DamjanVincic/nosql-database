package models

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	CrcSize       = 4
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	ValueSizeSize = 8

	CrcStart       = 0
	TimestampStart = CrcStart + CrcSize
	TombstoneStart = TimestampStart + TimestampSize
	KeySizeStart   = TombstoneStart + TombstoneSize
	ValueSizeStart = KeySizeStart + KeySizeSize
	KeyStart       = ValueSizeStart + ValueSizeSize

	RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize
)

// DataRecord struct for SSTable that wraps Data, KeySize and ValueSize
type DataRecord struct {
	Data      *Data
	Crc       uint32
	KeySize   uint64
	ValueSize uint64
}

func NewDataRecord(data *Data) *DataRecord {
	var keySize uint64
	var valueSize uint64
	var bytes []byte

	if !data.Tombstone {
		keySize = uint64(len(data.Key))
		valueSize = uint64(len(data.Value))

		// Bytes for checksum (timestamp, tombstone, key size and value size)
		bytes = make([]byte, RecordHeaderSize-CrcSize)
		binary.BigEndian.PutUint64(bytes, data.Timestamp)
		if data.Tombstone {
			bytes[TombstoneStart-CrcSize] = 1
		} else {
			bytes[TombstoneStart-CrcSize] = 0
		}
		binary.BigEndian.PutUint64(bytes[KeySizeStart-CrcSize:], keySize)
		binary.BigEndian.PutUint64(bytes[ValueSizeStart-CrcSize:], valueSize)
		bytes = append(bytes, []byte(data.Key)...)
		bytes = append(bytes, data.Value...)
	} else {
		keySize = uint64(len(data.Key))

		// Bytes for checksum (timestamp, tombstone, and key size)
		bytes = make([]byte, RecordHeaderSize-CrcSize-ValueSizeSize)
		binary.BigEndian.PutUint64(bytes, data.Timestamp)
		if data.Tombstone {
			bytes[TombstoneStart-CrcSize] = 1
		} else {
			bytes[TombstoneStart-CrcSize] = 0
		}
		binary.BigEndian.PutUint64(bytes[KeySizeStart-CrcSize:], keySize)
		bytes = append(bytes, []byte(data.Key)...)
	}

	return &DataRecord{
		Data:      data,
		Crc:       crc32.ChecksumIEEE(bytes),
		KeySize:   keySize,
		ValueSize: valueSize,
	}
}

//rt

func (dataRecord *DataRecord) Serialize(wal bool) []byte {
	var bytes []byte
	if wal || !dataRecord.Data.Tombstone {
		bytes = dataRecord.fullSerialize()
	} else {
		bytes = dataRecord.partialSerialize()
	}
	return bytes
}

// Serialize the whole DataRecord
func (dataRecord *DataRecord) fullSerialize() []byte {
	bytes := make([]byte, RecordHeaderSize+dataRecord.KeySize+dataRecord.ValueSize)
	// Append the CRC
	binary.BigEndian.PutUint32(bytes[CrcStart:TimestampStart], dataRecord.Crc)
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

// Serialize the DataRecord without ValueSize and Value
func (dataRecord *DataRecord) partialSerialize() []byte {
	bytes := make([]byte, RecordHeaderSize-ValueSizeSize+dataRecord.KeySize)
	// Append the CRC
	binary.BigEndian.PutUint32(bytes[CrcStart:TimestampStart], dataRecord.Crc)
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
	// Append the Key - ValueSizeStart is just there to append the Key after the KeySize, since it was where the Value would be if tombstone was false
	copy(bytes[ValueSizeStart:], dataRecord.Data.Key)

	return bytes
}

// Deserialize the whole DataRecord
func Deserialize(bytes []byte, wal bool) (*DataRecord, error) {
	var dataRecord *DataRecord
	var err error
	if wal {
		dataRecord, err = fullDeserialize(bytes)
		if err != nil {
			return nil, err
		}
	} else {
		if bytes[TombstoneStart] == 1 {
			dataRecord, err = partialDeserialize(bytes)
			if err != nil {
				return nil, err
			}
		} else {
			dataRecord, err = fullDeserialize(bytes)
			if err != nil {
				return nil, err
			}
		}
	}

	return dataRecord, nil
}

// Deserialize the DataRecord without it's ValueSize and Value
func fullDeserialize(bytes []byte) (*DataRecord, error) {
	crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:ValueSizeStart])
	valueSize := binary.BigEndian.Uint64(bytes[ValueSizeStart:KeyStart])
	key := string(bytes[KeyStart : KeyStart+keySize])

	var value = make([]byte, valueSize)
	copy(value, bytes[KeyStart+keySize:])

	// Check if the CRC matches
	if crc != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
		return nil, errors.New("CRC does not match")
	}

	return &DataRecord{
		Data: &Data{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
			Timestamp: timestamp,
		},
		Crc:       crc,
		KeySize:   keySize,
		ValueSize: valueSize,
	}, nil
}

func partialDeserialize(bytes []byte) (*DataRecord, error) {
	crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:ValueSizeStart])
	// The bytes positions are moved when the value and value size aren't stored
	key := string(bytes[ValueSizeStart : ValueSizeStart+keySize])

	// Check if the CRC matches
	if crc != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
		return nil, errors.New("CRC does not match")
	}

	return &DataRecord{
		Data: &Data{
			Key:       key,
			Value:     nil,
			Tombstone: tombstone,
			Timestamp: timestamp,
		},
		Crc:       crc,
		KeySize:   keySize,
		ValueSize: 0,
	}, nil
}
