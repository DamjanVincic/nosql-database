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
	HeaderSize       = 8

	path   = "wal"
	prefix = "wal_"
)

type WAL struct {
	segmentSize     uint64
	currentFilename string
}

func NewWAL(segmentSize uint64) (*WAL, error) {
	dirEntries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	var filename string
	// If there are no files in the directory, create the first one
	if len(dirEntries) == 0 {
		filename = fmt.Sprintf("%s%05d.log", prefix, 1)
		file, err := os.OpenFile(filepath.Join(path, filename), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		err = file.Truncate(HeaderSize)
		if err != nil {
			return nil, err
		}
	} else {
		// Get the last file
		filename = dirEntries[len(dirEntries)-1].Name()
	}
	return &WAL{
		segmentSize:     segmentSize,
		currentFilename: filename,
	}, nil
}

func (wal *WAL) CreateNewSegment() error {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	wal.currentFilename = fmt.Sprintf("%s%05d.log", prefix, len(dirEntries)+1)

	file, err := os.OpenFile(filepath.Join(path, wal.currentFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer func(file *os.File, err *error) {
		*err = file.Close()
	}(file, &err)
	if err != nil {
		return err
	}

	err = file.Truncate(HeaderSize)
	if err != nil {
		return err
	}

	return nil
}

func (wal *WAL) AddRecord(key string, value []byte, tombstone bool) error {
	var record = NewRecord(key, value, tombstone)
	var recordBytes = record.Serialize()
	var recordSize = RecordHeaderSize + record.KeySize + record.ValueSize // len(recordBytes) ?

	var idx uint64 = 0
	for idx < uint64(len(recordBytes)) {
		file, err := os.OpenFile(filepath.Join(path, wal.currentFilename), os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}

		fileSize := uint64(fileInfo.Size())
		remainingBytes := wal.segmentSize - (fileSize - HeaderSize)

		var mmapFile mmap.MMap
		if idx+remainingBytes < recordSize {
			err = file.Truncate(int64(fileSize + remainingBytes))
			if err != nil {
				return err
			}

			mmapFile, err = mmap.Map(file, mmap.RDWR, 0)
			if err != nil {
				return err
			}

			// Copy the bytes that can fit in the current file
			copy(mmapFile[fileSize:], recordBytes[idx:idx+remainingBytes])

			err = wal.CreateNewSegment()
			if err != nil {
				return err
			}

			newSegment, err := os.OpenFile(filepath.Join(path, wal.currentFilename), os.O_RDWR, 0644)
			if err != nil {
				return err
			}

			newSegmentMmap, err := mmap.Map(newSegment, mmap.RDWR, 0)
			if err != nil {
				return err
			}

			// If the record is bigger than the segment size, it'll take up the entire new segment
			binary.BigEndian.PutUint64(newSegmentMmap[:HeaderSize], min(recordSize-(idx+remainingBytes), wal.segmentSize))
			fmt.Println(recordSize-(idx+remainingBytes), wal.segmentSize)
			err = newSegmentMmap.Unmap()
			if err != nil {
				return err
			}

			err = newSegment.Close()
			if err != nil {
				return err
			}
		} else {
			err = file.Truncate(int64(fileSize + recordSize - idx))
			if err != nil {
				return err
			}

			mmapFile, err = mmap.Map(file, mmap.RDWR, 0)
			if err != nil {
				return err
			}

			// Copy the remaining bytes to the new file
			copy(mmapFile[fileSize:], recordBytes[idx:])
		}

		err = mmapFile.Unmap()
		if err != nil {
			return err
		}

		err = file.Close()
		if err != nil {
			return err
		}

		idx += remainingBytes
	}

	return nil
}

func (wal *WAL) GetRecords() ([]*Record, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	records := make([]*Record, 0)
	var recordBytes = make([]byte, 0)
	for _, file := range files {
		f, err := os.OpenFile(filepath.Join(path, file.Name()), os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		mmapFile, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			return nil, err
		}

		var offset = int(binary.BigEndian.Uint64(mmapFile[:HeaderSize])) + HeaderSize
		recordBytes = append(recordBytes, mmapFile[HeaderSize:offset]...)
		if len(recordBytes) != 0 {
			record, err := Deserialize(recordBytes)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			recordBytes = []byte{}
		}

		var i = offset
		for i < len(mmapFile) {
			if i+RecordHeaderSize > len(mmapFile) {
				break
			}
			keySize := binary.BigEndian.Uint64(mmapFile[i+KeySizeStart : i+ValueSizeStart])
			valueSize := binary.BigEndian.Uint64(mmapFile[i+ValueSizeStart : i+KeyStart])

			if i+RecordHeaderSize+int(keySize)+int(valueSize) > len(mmapFile) {
				break
			}

			record, err := Deserialize(mmapFile[i : uint64(i)+RecordHeaderSize+keySize+valueSize])
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			i += RecordHeaderSize + int(record.KeySize+record.ValueSize)
		}

		recordBytes = append(recordBytes, mmapFile[i:]...)

		err = mmapFile.Unmap()
		if err != nil {
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}

	return records, nil
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
