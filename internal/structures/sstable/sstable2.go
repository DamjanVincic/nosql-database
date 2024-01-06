package sstable

import (
	skipList "github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

/*
+---------------+-----------------+---------------+--...--+
|    CRC (4B)   | Timestamp (8B) | Tombstone(1B) |  Value |
+---------------+-----------------+---------------+--...--+
CRC = 32bit hash computed over the payload using CRC
Tombstone = If this record was deleted and has a value
Value = Value data
Timestamp = Timestamp of the operation in seconds

In order to optimize memory, we will not store the Key, KeySize and ValueSize found in Index.
*/
const (
	CrcSize       = 4
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	OffsetSize    = 8
	//for dataRecord
	CrcStart         = 0
	TimestampStart   = CrcStart + CrcSize
	TombstoneStart   = TimestampStart + TimestampSize
	ValueStart       = TombstoneStart + TombstoneSize
	RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize

	//for indexRecord
	KeySizeStart = 0
	KeyStart     = KeySizeStart + KeySizeSize

	// Path to store the SSTable files
	Path = "sstable"
)

type MemEntry struct {
	Key   string
	Value *skipList.SkipListValue
}

type SSTable struct {
	tableSize            uint64
	dataFilename         string
	indexFilename        string
	summaryIndexFilename string
	filterFilename       string
	metadataFilename     string
	tocFilename          string
}
