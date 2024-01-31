package models

import (
	"encoding/binary"
	"errors"
	key_encoder "github.com/DamjanVincic/key-value-engine/internal/structures/key-encoder"
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

func (dataRecord *DataRecord) Serialize() []byte {
	var bytes []byte
	if !dataRecord.Data.Tombstone {
		bytes = make([]byte, RecordHeaderSize+dataRecord.KeySize+dataRecord.ValueSize)
	} else {
		bytes = make([]byte, RecordHeaderSize+dataRecord.KeySize+dataRecord.ValueSize-ValueSizeSize)
	}
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

	if !dataRecord.Data.Tombstone {
		// Append the Value Size
		binary.BigEndian.PutUint64(bytes[ValueSizeStart:KeyStart], dataRecord.ValueSize)
		// Append the Key
		copy(bytes[KeyStart:KeyStart+dataRecord.KeySize], dataRecord.Data.Key)
		// Append the Value
		copy(bytes[KeyStart+dataRecord.KeySize:], dataRecord.Data.Value)
	} else {
		// Append the Key
		copy(bytes[ValueSizeStart:ValueSizeStart+dataRecord.KeySize], dataRecord.Data.Key)
	}

	return bytes
}

func (dataRecord *DataRecord) SerializeWithCompression(encoder *key_encoder.KeyEncoder) []byte {
	var bytes []byte

	//temporary storage for 32 and 64 integers and number of used bytes in them
	crcBytes := make([]byte, binary.MaxVarintLen32)
	var crcBytesSize int
	timestampBytes := make([]byte, binary.MaxVarintLen64)
	var timestampBytesSize int
	keyBytes := make([]byte, binary.MaxVarintLen64)
	var keyBytesSize int
	var valueSizeBytes []byte
	var valueSizeBytesSize int

	// serialize CRC
	crcBytesSize = binary.PutUvarint(crcBytes, uint64(dataRecord.Crc))

	// serialize Timestamp
	timestampBytesSize = binary.PutUvarint(timestampBytes, dataRecord.Data.Timestamp)

	// encode Key and serialize encoded value
	keyBytesSize = binary.PutUvarint(keyBytes, encoder.GetEncoded(dataRecord.Data.Key))

	if !dataRecord.Data.Tombstone {
		valueSizeBytes = make([]byte, binary.MaxVarintLen64)

		// serialize ValueSize
		valueSizeBytesSize = binary.PutUvarint(valueSizeBytes, dataRecord.ValueSize)

		//calculate size of serialized  record and make bytes
		bytes = make([]byte, uint64(crcBytesSize+timestampBytesSize+keyBytesSize+1)+dataRecord.ValueSize)
	} else {
		//calculate size of serialized  record and make bytes
		bytes = make([]byte, crcBytesSize+timestampBytesSize+keyBytesSize+1)
	}
	offset := 0

	//append Crc
	copy(bytes[offset:offset+crcBytesSize], crcBytes[:crcBytesSize])
	offset += crcBytesSize

	//append Timestamp
	copy(bytes[offset:offset+timestampBytesSize], timestampBytes[:timestampBytesSize])
	offset += timestampBytesSize

	//append Tombstone
	bytes[offset] = 1
	offset++

	//append Key
	copy(bytes[offset:offset+keyBytesSize], keyBytes[:keyBytesSize])
	offset += keyBytesSize

	if !(dataRecord.Data.Tombstone) {
		//append ValueSize
		copy(bytes[offset:offset+valueSizeBytesSize], valueSizeBytes[:valueSizeBytesSize])
		offset += valueSizeBytesSize

		//append Value
		copy(bytes[offset:], dataRecord.Data.Value)
	}

	return bytes
}

func Deserialize(bytes []byte) (*DataRecord, error) {
	crc := binary.BigEndian.Uint32(bytes[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(bytes[TimestampStart:TombstoneStart])
	tombstone := bytes[TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:ValueSizeStart])

	var key string
	var valueSize uint64
	var value []byte
	if !tombstone {
		valueSize = binary.BigEndian.Uint64(bytes[ValueSizeStart:KeyStart])
		key = string(bytes[KeyStart : KeyStart+keySize])
		value = make([]byte, valueSize)
		copy(value, bytes[KeyStart+keySize:])
	} else {
		key = string(bytes[ValueSizeStart : ValueSizeStart+keySize])
	}

	dataRecord := &DataRecord{
		Data: &Data{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
			Timestamp: timestamp,
		},
		Crc:       crc,
		KeySize:   keySize,
		ValueSize: valueSize,
	}

	// Check if the CRC matches
	if crc != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
		// return dataRecord anyway for merkle
		return dataRecord, errors.New("CRC does not match")
	} else {
		return dataRecord, nil
	}
}

func DeserializeWithCompression(bytes []byte, encoder key_encoder.KeyEncoder) (record *DataRecord, err error) {
	record = nil
	err = nil

	var crc uint64
	var timestamp uint64
	var encodedKey uint64
	var valueSize uint64
	var keySize uint64
	var value []byte

	var offsetStep int //used for storing number of bytes read when reading variant-encoded values
	offset := 0

	crc, offsetStep = binary.Uvarint(bytes[offset:])
	offset += offsetStep

	timestamp, offsetStep = binary.Uvarint(bytes[offset:])
	offset += offsetStep

	tombstone := bytes[offset] == 1
	offset++

	encodedKey, offsetStep = binary.Uvarint(bytes[offset:])
	offset += offsetStep

	key, err := encoder.GetKey(encodedKey)
	if err != nil {
		return
	}

	if !tombstone {
		valueSize, offsetStep = binary.Uvarint(bytes[offset:])
		offset += offsetStep

		value = make([]byte, valueSize)
		copy(value, bytes[offset:])
	}

	record = &DataRecord{
		Data: &Data{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
			Timestamp: timestamp,
		},
		Crc:       uint32(crc),
		KeySize:   keySize,
		ValueSize: valueSize,
	}
	//check implementation of crc creation!

	//// Check if the CRC matches
	//if uint32(crc) != crc32.ChecksumIEEE(bytes[TimestampStart:]) {
	//	// return dataRecord anyway for merkle
	//	return dataRecord, errors.New("CRC does not match")
	//} else {
	//	return dataRecord, nil
	//}
}
