package sstable

import (
	"encoding/binary"
	key_encoder "github.com/DamjanVincic/key-value-engine/internal/structures/key-encoder"

	"github.com/DamjanVincic/key-value-engine/internal/models"
)

type IndexRecord struct {
	Key    string
	Offset uint64
}

func NewIndexRecord(memEntry *models.Data, offset uint64) *IndexRecord {
	return &IndexRecord{
		Key:    memEntry.Key,
		Offset: offset,
	}
}

func (indexRecord *IndexRecord) SerializeIndexRecord(compression bool, encoder *key_encoder.KeyEncoder) []byte {
	var bytes []byte

	if compression {
		//temporary storage for 64-bit integer and used bytes in them
		keyBytes := make([]byte, binary.MaxVarintLen64)
		var keyBytesSize int
		offsetBytes := make([]byte, binary.MaxVarintLen64)
		var offsetBytesSize int

		//get encoded uint64 value of key
		encodedKey := encoder.GetEncoded(indexRecord.Key)

		//serialize key and offset
		keyBytesSize = binary.PutUvarint(keyBytes, encodedKey)
		offsetBytesSize = binary.PutUvarint(offsetBytes, indexRecord.Offset)

		//make bytes and append serialized key and offset
		bytes = make([]byte, keyBytesSize+offsetBytesSize)
		copy(bytes[:keyBytesSize], keyBytes[:keyBytesSize])
		copy(bytes[keyBytesSize:], offsetBytes[:offsetBytesSize])

	} else {
		keySize := uint64(len([]byte(indexRecord.Key)))

		// 8 bytes for Offset and Keysize
		bytes = make([]byte, KeySizeSize+keySize+OffsetSize)

		// Serialize KeySize (8 bytes, BigEndian)
		binary.BigEndian.PutUint64(bytes, keySize)

		// Serialize Key (variable size)
		copy(bytes[KeyStart:], []byte(indexRecord.Key))

		// Serialize Offset (8 bytes, BigEndian)
		binary.BigEndian.PutUint64(bytes[KeyStart+keySize:], indexRecord.Offset)
	}

	return bytes
}

func DeserializeIndexRecord(bytes []byte, compression bool, encoder *key_encoder.KeyEncoder) (indexRecord *IndexRecord, recordLength uint64, err error) {
	indexRecord = nil
	err = nil
	recordLength = 0

	var key string
	var offset uint64

	if compression {
		//deserialize encoded uint64 value of key and get number of bytes read
		encodedKey, bytesRead := binary.Uvarint(bytes)
		recordLength += uint64(bytesRead)

		//get string key from encoded value
		key, err = encoder.GetKey(encodedKey)
		if err != nil {
			return
		}

		//deserialize offset
		offset, bytesRead = binary.Uvarint(bytes[bytesRead:])
		recordLength += uint64(bytesRead)

	} else {
		// Deserialize KeySize (8 bytes, BigEndian)
		keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:KeySizeSize])

		// Deserialize Key (variable size)
		key = string(bytes[KeyStart : KeyStart+int(keySize)])

		// Deserialize Offset (8 bytes, BigEndian)
		offset = binary.BigEndian.Uint64(bytes[KeyStart+int(keySize):])
	}

	indexRecord = &IndexRecord{Key: key, Offset: offset}
	return
}
