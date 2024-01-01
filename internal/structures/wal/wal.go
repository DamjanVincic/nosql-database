package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

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

	//RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize
	path   = "wal"
	prefix = "wal_"
)

type Record struct {
	Crc       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

type WAL struct {
	segmentSize     uint64
	currentFilename string
}

func NewWAL(segmentSize uint64) *WAL {
	dirEntries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	var filename string
	// If there are no files in the directory, create the first one
	if len(dirEntries) == 0 {
		filename = fmt.Sprintf("%s%05d.log", prefix, 1)
		_, err := os.Create(filepath.Join(path, filename))
		if err != nil {
			panic(err)
		}
	} else {
		// Get the last file
		filename = dirEntries[len(dirEntries)-1].Name()
	}
	return &WAL{
		segmentSize:     segmentSize,
		currentFilename: filename,
	}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
