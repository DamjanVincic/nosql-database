package sstable

import (
	"encoding/binary"
)

type IndexRecord struct {
	keySize uint64
	key     string
	offset  uint64
}

func NewIndexRecord(memEntry *MemEntry, offset uint64) *IndexRecord {
	return &IndexRecord{
		keySize: uint64(len([]byte(memEntry.Key))),
		key:     memEntry.Key,
		offset:  offset,
	}
}

func (indexRecord *IndexRecord) SerializeIndexRecord() []byte {

	// 8 bytes for Offset and Keysize
	bytes := make([]byte, indexRecord.keySize+16)

	// Serialize KeySize (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes, indexRecord.keySize)

	// Serialize Key (variable size)
	copy(bytes[KeyStart:], []byte(indexRecord.key))

	// Serialize Offset (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes[KeyStart+len([]byte(indexRecord.key)):], indexRecord.offset)

	return bytes
}

func DeserializeIndexRecord(bytes []byte) (*IndexRecord, error) {
	// Deserialize KeySize (8 bytes, BigEndian)
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:KeySizeSize])

	// Deserialize Key (variable size)
	key := string(bytes[KeyStart : KeyStart+int(keySize)])

	// Deserialize Offset (8 bytes, BigEndian)
	offset := binary.BigEndian.Uint64(bytes[KeyStart+int(keySize):])

	return &IndexRecord{
		key:     key,
		keySize: keySize,
		offset:  offset,
	}, nil
}
