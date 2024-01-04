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

	// RecordHeaderSize The size of the record header
	RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize
	// HeaderSize File header size, to store the amount of bytes overflowed from the previous file
	HeaderSize = 8

	// Path to store the Write Ahead Log files
	Path = "wal"
	// Prefix for the Write Ahead Log files
	Prefix = "wal_"
	// LowWaterMark is used to mark the file up until which the logs can be deleted
	LowWaterMark = "lwm"
)

type WAL struct {
	segmentSize     uint64
	currentFilename string
}

// NewWAL Create a new Write Ahead Log, segmentSize is the maximum size of a segment in bytes
func NewWAL(segmentSize uint64) (*WAL, error) {
	// Create the directory if it doesn't exist
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
		// Filename format: wal_00001_lwm.log
		filename = fmt.Sprintf("%s%05d_%s.log", Prefix, 1, LowWaterMark)
		file, err := os.OpenFile(filepath.Join(Path, filename), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		// Leave the first HeaderSize bytes for the file header
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

// CreateNewSegment Create a new segment file
func (wal *WAL) CreateNewSegment() error {
	dirEntries, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	// Increment the current filename
	wal.currentFilename = fmt.Sprintf("%s%05d.log", Prefix, len(dirEntries)+1)

	// Create the new file
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

	// Leave the first HeaderSize bytes for the file header
	err = file.Truncate(HeaderSize)
	if err != nil {
		return err
	}

	return nil
}

// AddRecord Add a new record to the Write Ahead Log
func (wal *WAL) AddRecord(key string, value []byte, tombstone bool) error {
	var record = NewRecord(key, value, tombstone)
	var recordBytes = record.Serialize()
	var recordSize = uint64(len(recordBytes))

	// Current byte index in the record
	var idx uint64 = 0
	for idx < recordSize {
		file, err := os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}

		fileSize := uint64(fileInfo.Size())

		// Remaining bytes in the current file
		var remainingBytes uint64

		// If segment size got changed in between application runs, and it's smaller than the current file size, we need to create a new segment
		if wal.segmentSize < fileSize-HeaderSize {
			// Close the current file
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
		}
		remainingBytes = wal.segmentSize - (fileSize - HeaderSize)

		var mmapFile mmap.MMap
		// If the record can't fit in the current file
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

			// If the record bytes are bigger than the segment size, they will take up the entire new segment
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
			// If the record can fit in the current file

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

// GetRecords Get all records from the Write Ahead Log
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

		// Offset where the record from previous file ends
		var offset = int(binary.BigEndian.Uint64(mmapFile[:HeaderSize])) + HeaderSize

		// If the last record got deleted based on low watermark, we will ignore the record's remaining bytes
		if len(recordBytes) != 0 {
			recordBytes = append(recordBytes, mmapFile[HeaderSize:offset]...)

			// If the record bytes overflow the current file, we won't deserialize them yet
			if binary.BigEndian.Uint64(mmapFile[:HeaderSize]) != wal.segmentSize {
				record, err := Deserialize(recordBytes)
				if err != nil {
					return nil, err
				}
				records = append(records, record)
				recordBytes = []byte{}
			}
		}

		// Current byte index in the file
		var i = offset
		for i < len(mmapFile) {
			// If the current record header size isn't in the same file, we will move on to the next one
			if i+RecordHeaderSize > len(mmapFile) {
				break
			}
			keySize := binary.BigEndian.Uint64(mmapFile[i+KeySizeStart : i+ValueSizeStart])
			valueSize := binary.BigEndian.Uint64(mmapFile[i+ValueSizeStart : i+KeyStart])

			// If the record header size fit in this file, but the key or value didn't, we also move on to the next file
			if i+RecordHeaderSize+int(keySize)+int(valueSize) > len(mmapFile) {
				break
			}

			// If the whole record is in this file, we deserialize it
			record, err := Deserialize(mmapFile[i : uint64(i)+RecordHeaderSize+keySize+valueSize])
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			i += RecordHeaderSize + int(record.KeySize+record.ValueSize)
		}

		// If the record didn't fit in the current file, we store the remaining bytes and move on to the next one
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

		// Offset where the record from previous file ends
		var offset = int(binary.BigEndian.Uint64(mmapFile[:HeaderSize])) + HeaderSize

		// If the last record got deleted based on low watermark, we will ignore the record's remaining bytes
		if len(recordBytes) != 0 {
			recordBytes = append(recordBytes, mmapFile[HeaderSize:offset]...)
		}

		// If the record bytes overflow the current file, we won't deserialize them yet
		if len(recordBytes) != 0 && binary.BigEndian.Uint64(mmapFile[:HeaderSize]) != wal.segmentSize {
			record, err := Deserialize(recordBytes)
			if err != nil {
				return 0, err
			}

			// If we found a record with the same, or newer timestamp, return the index of the segment
			if record.Timestamp >= rec.Timestamp {
				return uint64(idx), nil
			}
			recordBytes = []byte{}
		}

		// Current byte index in the file
		var i = offset
		for i < len(mmapFile) {
			// If the current record header size isn't in the same file, move on to the next one
			if i+RecordHeaderSize > len(mmapFile) {
				break
			}
			keySize := binary.BigEndian.Uint64(mmapFile[i+KeySizeStart : i+ValueSizeStart])
			valueSize := binary.BigEndian.Uint64(mmapFile[i+ValueSizeStart : i+KeyStart])

			// If the record header size fit in this file, but the key or value didn't, move on to the next file
			if i+RecordHeaderSize+int(keySize)+int(valueSize) > len(mmapFile) {
				break
			}

			record, err := Deserialize(mmapFile[i : uint64(i)+RecordHeaderSize+keySize+valueSize])
			if err != nil {
				return 0, err
			}

			// If we found a record with the same, or newer timestamp, return the index of the segment
			if record.Timestamp >= rec.Timestamp {
				return uint64(idx), nil
			}
			i += RecordHeaderSize + int(record.KeySize+record.ValueSize)
		}

		// If the record didn't fit in the current file, we store the remaining bytes and move on to the next one
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

// MoveLowWatermark Move the low watermark to the segment that has a record with the given timestamp
func (wal *WAL) MoveLowWatermark(record *Record) error {
	// Find the segment that has a record with the given or a newer timestamp
	idx, err := wal.FindTimestampSegment(record)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	// Find the previous low watermark file and remove the low watermark from it
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

	// Rename the segment with the given index to have the low watermark
	fileName := files[idx].Name()
	extension := filepath.Ext(fileName)
	err = os.Rename(filepath.Join(Path, fileName), filepath.Join(Path, strings.Replace(fileName, extension, fmt.Sprintf("_%s%s", LowWaterMark, extension), 1)))
	if err != nil {
		return err
	}

	return nil
}

// DeleteSegments Delete all segments up until the low watermark
func (wal *WAL) DeleteSegments() error {
	files, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	// Delete segments up until the low watermark
	for _, file := range files {
		if strings.Contains(file.Name(), LowWaterMark) {
			break
		}
		err := os.Remove(filepath.Join(Path, file.Name()))
		if err != nil {
			return err
		}
	}

	files, err = os.ReadDir(Path)
	if err != nil {
		return err
	}

	// Rename the remaining segments to start from 1
	for idx, file := range files {
		// Different logic to rename the low watermark file
		if strings.Contains(file.Name(), LowWaterMark) {
			parts := strings.Split(file.Name(), "_")
			err := os.Rename(filepath.Join(Path, file.Name()), filepath.Join(Path, fmt.Sprintf("%s_%05d_%s", parts[0], idx+1, parts[2])))
			if err != nil {
				return err
			}
		} else {
			extension := filepath.Ext(file.Name())
			parts := strings.Split(file.Name(), extension)
			parts2 := strings.Split(parts[0], "_")
			err := os.Rename(filepath.Join(Path, file.Name()), filepath.Join(Path, fmt.Sprintf("%s_%05d%s", parts2[0], idx+1, extension)))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
