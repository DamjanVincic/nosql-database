package models

import (
	"encoding/binary"
	"errors"
	"github.com/DamjanVincic/key-value-engine/internal/structures/keyencoder"
	"github.com/edsrzf/mmap-go"
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

// Data struct that wraps bytes, tombstone and timestamp
// to be used in structures that require those fields
type Data struct {
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp uint64
}

func (data *Data) Serialize(compression bool, encoder *keyencoder.KeyEncoder) []byte {
	if compression {
		return data.serializeWithCompression(encoder)
	} else {
		return data.serializeWithoutCompression()
	}
}

func (data *Data) serializeWithoutCompression() []byte {
	var bytes []byte
	keySize := uint64(len([]byte(data.Key)))
	valueSize := uint64(len(data.Value))
	if !data.Tombstone {
		bytes = make([]byte, RecordHeaderSize+keySize+valueSize)
		bytes[TombstoneStart] = 0
		// Append the Value Size
		binary.BigEndian.PutUint64(bytes[ValueSizeStart:KeyStart], valueSize)
		// Append the Key
		copy(bytes[KeyStart:KeyStart+keySize], data.Key)
		// Append the Value
		copy(bytes[KeyStart+keySize:], data.Value)
	} else {
		bytes = make([]byte, RecordHeaderSize+keySize-ValueSizeSize)
		bytes[TombstoneStart] = 1
		// Append the Key
		copy(bytes[ValueSizeStart:ValueSizeStart+keySize], data.Key)
	}
	// Append the Timestamp
	binary.BigEndian.PutUint64(bytes[TimestampStart:TombstoneStart], data.Timestamp)
	// Append the Tombstone
	//if data.Tombstone {
	//	bytes[TombstoneStart] = 1
	//} else {
	//	bytes[TombstoneStart] = 0
	//}
	// Append the Key Size
	binary.BigEndian.PutUint64(bytes[KeySizeStart:ValueSizeStart], keySize)

	crc := crc32.ChecksumIEEE(bytes[CrcSize:])
	// Append the CRC
	binary.BigEndian.PutUint32(bytes[CrcStart:TimestampStart], crc)

	return bytes
}

func (data *Data) serializeWithCompression(encoder *keyencoder.KeyEncoder) []byte {
	var bytes []byte
	valueSize := uint64(len(data.Value))

	//temporary storage for 32 and 64 integers and number of used bytes in them
	crcBytes := make([]byte, binary.MaxVarintLen32)
	var crcBytesSize int
	timestampBytes := make([]byte, binary.MaxVarintLen64)
	var timestampBytesSize int
	keyBytes := make([]byte, binary.MaxVarintLen64)
	var keyBytesSize int
	var valueSizeBytes []byte
	var valueSizeBytesSize int

	// serialize Timestamp
	timestampBytesSize = binary.PutUvarint(timestampBytes, data.Timestamp)

	// encode Key and serialize encoded value
	keyBytesSize = binary.PutUvarint(keyBytes, encoder.GetEncoded(data.Key))

	if !data.Tombstone {
		valueSizeBytes = make([]byte, binary.MaxVarintLen64)

		// serialize ValueSize
		valueSizeBytesSize = binary.PutUvarint(valueSizeBytes, valueSize)

		//calculate size of serialized  record and make bytes
		bytes = make([]byte, uint64(timestampBytesSize+keyBytesSize+TombstoneSize+valueSizeBytesSize)+valueSize)
	} else {
		//calculate size of serialized  record and make bytes
		bytes = make([]byte, timestampBytesSize+keyBytesSize+TombstoneSize)
	}
	offset := 0

	//append Timestamp
	copy(bytes[offset:offset+timestampBytesSize], timestampBytes[:timestampBytesSize])
	offset += timestampBytesSize

	//append Tombstone
	if data.Tombstone {
		bytes[offset] = 1
	} else {
		bytes[offset] = 0
	}
	offset++

	//append Key
	copy(bytes[offset:offset+keyBytesSize], keyBytes[:keyBytesSize])
	offset += keyBytesSize

	if !data.Tombstone {
		//append ValueSize
		copy(bytes[offset:offset+valueSizeBytesSize], valueSizeBytes[:valueSizeBytesSize])
		offset += valueSizeBytesSize

		//append Value
		copy(bytes[offset:], data.Value)
	}

	//calculate crc
	crc := crc32.ChecksumIEEE(bytes)
	// serialize CRC
	crcBytesSize = binary.PutUvarint(crcBytes, uint64(crc))

	//append Crc
	bytes = append(crcBytes[:crcBytesSize], bytes...)

	return bytes
}

func Deserialize(mmapFile mmap.MMap, compression bool, encoder *keyencoder.KeyEncoder) (*Data, uint64, error) {
	if compression {
		return deserializeWithCompression(mmapFile, encoder)
	} else {
		return deserializeWithoutCompression(mmapFile)
	}
}

// returns pointer to deserialized data and number of deserialized bytes
func deserializeWithoutCompression(mmapFile mmap.MMap) (*Data, uint64, error) {
	crc := binary.BigEndian.Uint32(mmapFile[CrcStart:TimestampStart])
	timestamp := binary.BigEndian.Uint64(mmapFile[TimestampStart:TombstoneStart])
	tombstone := mmapFile[TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(mmapFile[KeySizeStart:ValueSizeStart])

	var bytesRead uint64
	var key string
	var valueSize uint64
	var value []byte
	if !tombstone {
		valueSize = binary.BigEndian.Uint64(mmapFile[ValueSizeStart:KeyStart])
		key = string(mmapFile[KeyStart : KeyStart+keySize])
		value = make([]byte, valueSize)
		copy(value, mmapFile[KeyStart+keySize:KeyStart+keySize+valueSize])
		bytesRead = KeyStart + keySize + valueSize
	} else {
		key = string(mmapFile[ValueSizeStart : ValueSizeStart+keySize])
		bytesRead = ValueSizeStart + keySize
	}

	data := &Data{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}

	// Check if the CRC matches
	if crc != crc32.ChecksumIEEE(mmapFile[TimestampStart:bytesRead]) {
		// return dataRecord anyway for merkle
		return data, bytesRead, errors.New("CRC does not match")
	} else {
		return data, bytesRead, nil
	}
}

func deserializeWithCompression(mmapFile mmap.MMap, encoder *keyencoder.KeyEncoder) (data *Data, bytesRead uint64, err error) {
	data = nil
	err = nil
	bytesRead = 0

	var crc uint64
	var crcSize int
	var timestamp uint64
	var encodedKey uint64
	var valueSize uint64
	var value []byte

	var offsetStep int //used for storing number of bytes read when reading variant-encoded values
	offset := 0

	crc, crcSize = binary.Uvarint(mmapFile[offset:])
	offset += crcSize

	timestamp, offsetStep = binary.Uvarint(mmapFile[offset:])
	offset += offsetStep

	tombstone := mmapFile[offset] == 1
	offset++

	encodedKey, offsetStep = binary.Uvarint(mmapFile[offset:])
	offset += offsetStep

	key, err := encoder.GetKey(encodedKey)
	if err != nil {
		return
	}

	if !tombstone {
		valueSize, offsetStep = binary.Uvarint(mmapFile[offset:])
		offset += offsetStep

		value = make([]byte, valueSize)
		copy(value, mmapFile[offset:uint64(offset)+valueSize])
		offset += int(valueSize)
	}

	newCrc := crc32.ChecksumIEEE(mmapFile[crcSize:offset])

	if newCrc != uint32(crc) {
		err = errors.New("CRC does not match")
	}

	data = &Data{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}

	bytesRead = uint64(offset)
	return
}
