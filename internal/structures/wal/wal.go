package wal

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
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

	RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize

	path   = "wal"
	prefix = "wal_"
)

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

func (wal *WAL) AddRecord(record *Record) {
	// Check if the current file is full
	fileInfo, err := os.Stat(filepath.Join(path, wal.currentFilename))
	if err != nil {
		panic(err)
	}

	dirEntries, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	// If the current file is full, create a new one
	if uint64(fileInfo.Size())+RecordHeaderSize+record.KeySize+record.ValueSize > wal.segmentSize {
		wal.currentFilename = fmt.Sprintf("%s%05d.log", prefix, len(dirEntries)+1)
		_, err := os.Create(filepath.Join(path, wal.currentFilename))
		if err != nil {
			panic(err)
		}
	}

	// Append the record to the current file
	f, err := os.OpenFile(filepath.Join(path, wal.currentFilename), os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			panic(err)
		}
	}(f)

	fileInfo, err = f.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fileInfo.Size()

	err = f.Truncate(fileSize + int64(RecordHeaderSize+record.KeySize+record.ValueSize))
	if err != nil {
		return
	}

	mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer func(mmapFile *mmap.MMap) {
		err := mmapFile.Unmap()
		if err != nil {
			panic(err)
		}
	}(&mmapFile)

	// Append the record to the file
	copy(mmapFile[fileSize:], record.Serialize())
}

func (wal *WAL) GetRecords() []*Record {
	files, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	records := make([]*Record, 0)
	for _, file := range files {
		// Open the file
		f, err := os.Open(filepath.Join(path, file.Name()))
		if err != nil {
			panic(err)
		}

		mmapFile, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			panic(err)
		}

		// Read the records from the file
		for i := 0; i < len(mmapFile); {
			keySize := binary.BigEndian.Uint64(mmapFile[i+KeySizeStart : i+ValueSizeStart])
			valueSize := binary.BigEndian.Uint64(mmapFile[i+ValueSizeStart : i+KeyStart])
			record := Deserialize(mmapFile[i : uint64(i)+RecordHeaderSize+keySize+valueSize])
			records = append(records, record)
			i += RecordHeaderSize + int(record.KeySize) + int(record.ValueSize)
		}
		err = mmapFile.Unmap()
		if err != nil {
			panic(err)
		}

		err = f.Close()
		if err != nil {
			panic(err)
		}
	}

	return records
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
