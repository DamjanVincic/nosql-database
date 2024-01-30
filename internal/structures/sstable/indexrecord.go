package sstable

import (
	"encoding/binary"

	"github.com/DamjanVincic/key-value-engine/internal/models"
)

type IndexRecord struct {
	keySize uint64
	Key     string
	Offset  uint64
}

func NewIndexRecord(memEntry *models.Data, offset uint64) *IndexRecord {
	return &IndexRecord{
		keySize: uint64(len([]byte(memEntry.Key))),
		Key:     memEntry.Key,
		Offset:  offset,
	}
}

func (indexRecord *IndexRecord) SerializeIndexRecord() []byte {

	// 8 bytes for Offset and Keysize
	bytes := make([]byte, KeySizeSize+indexRecord.keySize+OffsetSize)

	// Serialize KeySize (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes, indexRecord.keySize)

	// Serialize Key (variable size)
	copy(bytes[KeyStart:], []byte(indexRecord.Key))

	// Serialize Offset (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes[KeyStart+indexRecord.keySize:], indexRecord.Offset)

	return bytes
}

func DeserializeIndexRecord(bytes []byte) *IndexRecord {
	// Deserialize KeySize (8 bytes, BigEndian)
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:KeySizeSize])

	// Deserialize Key (variable size)
	key := string(bytes[KeyStart : KeyStart+int(keySize)])

	// Deserialize Offset (8 bytes, BigEndian)
	offset := binary.BigEndian.Uint64(bytes[KeyStart+int(keySize):])

	return &IndexRecord{
		Key:     key,
		keySize: keySize,
		Offset:  offset,
	}
}
