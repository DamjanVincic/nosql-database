package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
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

	Path         = "wal"
	Prefix       = "wal_"
	LowWaterMark = "lwm"
)

type WAL struct {
	segmentSize     uint64
	currentFilename string
}

func NewWAL(segmentSize uint64) (*WAL, error) {
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	var filename string
	// If there are no files in the directory, create the first one
	if len(dirEntries) == 0 {
		filename = fmt.Sprintf("%s%05d.log", Prefix, 1)
		file, err := os.OpenFile(filepath.Join(Path, filename), os.O_CREATE|os.O_RDWR, 0644)
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
	dirEntries, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	wal.currentFilename = fmt.Sprintf("%s%05d.log", Prefix, len(dirEntries)+1)

	file, err := os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_CREATE|os.O_RDWR, 0644)
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
		file, err := os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}

		fileSize := uint64(fileInfo.Size())
		var remainingBytes uint64
		if fileSize-HeaderSize < wal.segmentSize {
			remainingBytes = wal.segmentSize - (fileSize - HeaderSize)
		} else {
			// If segment size got changed in between application runs, and it's smaller than the current file size, we need to create a new segment
			err = file.Close()
			if err != nil {
				return err
			}

			err = wal.CreateNewSegment()
			if err != nil {
				return err
			}

			file, err = os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
			if err != nil {
				return err
			}

			fileInfo, err = file.Stat()
			if err != nil {
				return err
			}

			fileSize = uint64(fileInfo.Size())
			remainingBytes = wal.segmentSize - (fileSize - HeaderSize)
		}

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

			newSegment, err := os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
			if err != nil {
				return err
			}

			newSegmentMmap, err := mmap.Map(newSegment, mmap.RDWR, 0)
			if err != nil {
				return err
			}

			// If the record is bigger than the segment size, it'll take up the entire new segment
			binary.BigEndian.PutUint64(newSegmentMmap[:HeaderSize], min(recordSize-(idx+remainingBytes), wal.segmentSize))
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
	files, err := os.ReadDir(Path)
	if err != nil {
		return nil, err
	}

	records := make([]*Record, 0)
	var recordBytes = make([]byte, 0)
	for _, file := range files {
		f, err := os.OpenFile(filepath.Join(Path, file.Name()), os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		mmapFile, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			return nil, err
		}

		var offset = int(binary.BigEndian.Uint64(mmapFile[:HeaderSize])) + HeaderSize
		// If the last record got deleted based on low watermark, we will ignore the record's remaining bytes
		if len(recordBytes) != 0 && binary.BigEndian.Uint64(mmapFile[:HeaderSize]) != wal.segmentSize {
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

// FindTimestampSegment Find a segment that has a record with the given timestamp, or a newer one (for low watermark)
func (wal *WAL) FindTimestampSegment(rec *Record) (uint64, error) {
	files, err := os.ReadDir(Path)
	if err != nil {
		return 0, err
	}

	var recordBytes = make([]byte, 0)
	for idx, file := range files {
		f, err := os.OpenFile(filepath.Join(Path, file.Name()), os.O_RDONLY, 0644)
		if err != nil {
			return 0, err
		}

		mmapFile, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			return 0, err
		}

		var offset = int(binary.BigEndian.Uint64(mmapFile[:HeaderSize])) + HeaderSize
		recordBytes = append(recordBytes, mmapFile[HeaderSize:offset]...)
		if len(recordBytes) != 0 && binary.BigEndian.Uint64(mmapFile[:HeaderSize]) != wal.segmentSize {
			record, err := Deserialize(recordBytes)
			if err != nil {
				return 0, err
			}
			if record.Timestamp >= rec.Timestamp {
				return uint64(idx), nil
			}
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
				return 0, err
			}
			if record.Timestamp >= rec.Timestamp {
				return uint64(idx), nil
			}
			i += RecordHeaderSize + int(record.KeySize+record.ValueSize)
		}

		recordBytes = append(recordBytes, mmapFile[i:]...)

		err = mmapFile.Unmap()
		if err != nil {
			return 0, err
		}
		err = f.Close()
		if err != nil {
			return 0, err
		}
	}

	return 0, errors.New("no segment found")
}

func (wal *WAL) MoveLowWatermark(record *Record) error {
	idx, err := wal.FindTimestampSegment(record)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	for _, file := range files {
		parts := strings.SplitN(file.Name(), fmt.Sprintf("_%s", LowWaterMark), 2)
		if len(parts) != 2 {
			continue
		}

		fileName := file.Name()
		err := os.Rename(filepath.Join(Path, fileName), filepath.Join(Path, strings.Replace(fileName, fmt.Sprintf("_%s", LowWaterMark), "", 1)))
		if err != nil {
			return err
		}
		break
	}

	fileName := files[idx].Name()
	extension := filepath.Ext(fileName)
	//fileName = strings.Replace(fileName, extension, fmt.Sprintf("_%s%s", LowWaterMark, extension), 1)
	err = os.Rename(filepath.Join(Path, fileName), filepath.Join(Path, strings.Replace(fileName, extension, fmt.Sprintf("_%s%s", LowWaterMark, extension), 1)))
	if err != nil {
		return err
	}

	return nil
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
