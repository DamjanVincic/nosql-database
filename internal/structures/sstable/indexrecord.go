package sstable

import (
	"encoding/binary"
)

type indexRecord struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

func NewIndexRecord(memEntry MemEntry, offset uint64) *indexRecord {
	return &indexRecord{
		KeySize: uint64(len([]byte(memEntry.Key))),
		Key:     memEntry.Key,
		Offset:  offset,
	}
}

func (indexRecord *indexRecord) SerializeIndexRecord() []byte {

	// 8 bytes for Offset and Keysize
	bytes := make([]byte, indexRecord.KeySize+16)

	// Serialize KeySize (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes, indexRecord.KeySize)

	// Serialize Key (variable size)
	copy(bytes[8:], []byte(indexRecord.Key))

	// Serialize Offset (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes[8+len([]byte(indexRecord.Key)):], indexRecord.Offset)

	return bytes
}

func DeserializeIndexRecord(bytes []byte) (*indexRecord, error) {
	// Deserialize KeySize (8 bytes, BigEndian)
	keySize := binary.BigEndian.Uint64(bytes[:8])

	// Deserialize Key (variable size)
	key := string(bytes[8 : keySize+8])

	// Deserialize Offset (8 bytes, BigEndian)
	offset := binary.BigEndian.Uint64(bytes[keySize+8:])

	return &indexRecord{
		Key:     key,
		KeySize: keySize,
		Offset:  offset,
	}, nil
}
