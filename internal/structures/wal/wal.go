package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/memtable"
	"github.com/edsrzf/mmap-go"
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

	// The size of the record header
	RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize

	// The size of bytes in the header to store where the records start from after deleting previous segments
	BeginningOffsetSize = 8
	// The size of bytes in the header to store the amount of bytes overflowed from the previous file
	BytesOverflowOffsetSize = 8

	BeginningOffsetStart     = 0
	BytesOverflowOffsetStart = BeginningOffsetStart + BeginningOffsetSize

	// HeaderSize File header size, to store the amount of bytes overflowed from the previous file
	HeaderSize = BeginningOffsetSize + BytesOverflowOffsetSize

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

// createNewSegment Create a new segment file
func (wal *WAL) createNewSegment() error {
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

	// Leave the first HeaderSize bytes for the file header
	err = file.Truncate(HeaderSize)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

/*
Get the remaining bytes in the file and the file size,
if there's not enough space due to changing segment size,
create a new segment and open it in the forwarded file pointer.
*/
func (wal *WAL) getRemainingBytesAndFileSize(file *os.File) (uint64, uint64, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}

	fileSize := uint64(fileInfo.Size())

	// Remaining bytes in the current file
	var remainingBytes uint64

	// If segment size got changed in between application runs, and it's smaller than the current file size, we need to create a new segment
	if wal.segmentSize < fileSize-HeaderSize {
		// Close the current file
		err = file.Close()
		if err != nil {
			return 0, 0, err
		}

		err = wal.createNewSegment()
		if err != nil {
			return 0, 0, err
		}

		file, err = os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
		if err != nil {
			return 0, 0, err
		}

		fileInfo, err = file.Stat()
		if err != nil {
			return 0, 0, err
		}

		fileSize = uint64(fileInfo.Size())
	}
	remainingBytes = wal.segmentSize - (fileSize - HeaderSize)

	// We return the file size to avoid getting it in multiple other functions
	return remainingBytes, fileSize, nil
}

// Write the forwarded bytes to the file.
func (wal *WAL) writeBytes(file *os.File, bytes []byte, fileSize uint64) error {
	err := file.Truncate(int64(fileSize + uint64(len(bytes))))
	if err != nil {
		return err
	}

	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	// Copy the bytes that can fit in the current file
	copy(mmapFile[fileSize:], bytes)

	err = mmapFile.Unmap()
	if err != nil {
		return err
	}

	return nil
}

/*
Write the record bytes to the file, starting from the given index.
If the record bytes overflow the current file, create a new segment and write the remaining bytes to it.
Return the number of bytes that can fit in the file (the amount of bytes written).
*/
func (wal *WAL) writeRecordBytes(file *os.File, recordBytes []byte, idx uint64) (uint64, error) {
	recordSize := uint64(len(recordBytes))

	remainingBytes, fileSize, err := wal.getRemainingBytesAndFileSize(file)
	if err != nil {
		return 0, err
	}

	// If the record can't fit in the current file
	if idx+remainingBytes < recordSize {
		err := wal.writeBytes(file, recordBytes[idx:idx+remainingBytes], fileSize)
		if err != nil {
			return 0, err
		}

		err = wal.createNewSegment()
		if err != nil {
			return 0, err
		}

		newSegment, err := os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
		if err != nil {
			return 0, err
		}

		newSegmentMmap, err := mmap.Map(newSegment, mmap.RDWR, 0)
		if err != nil {
			return 0, err
		}

		// If the record bytes are bigger than the segment size, they will take up the entire new segment
		binary.BigEndian.PutUint64(newSegmentMmap[BytesOverflowOffsetStart:HeaderSize], min(recordSize-(idx+remainingBytes), wal.segmentSize))
		err = newSegmentMmap.Unmap()
		if err != nil {
			return 0, err
		}

		err = newSegment.Close()
		if err != nil {
			return 0, err
		}
	} else {
		// If the record can fit in the current file
		remainingBytes = recordSize - idx // To return the amount of bytes written in the last part of the byte array

		err := wal.writeBytes(file, recordBytes[idx:], fileSize)
		if err != nil {
			return 0, err
		}
	}

	return remainingBytes, nil
}

// AddRecord Add a new record to the Write Ahead Log
func (wal *WAL) AddRecord(key string, value []byte, tombstone bool) (*models.Data, error) {
	var record = NewRecord(key, value, tombstone)
	var recordBytes = record.Serialize()
	var recordSize = uint64(len(recordBytes))

	// Current byte index in the record
	var idx uint64 = 0
	for idx < recordSize {
		file, err := os.OpenFile(filepath.Join(Path, wal.currentFilename), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		bytesWritten, err := wal.writeRecordBytes(file, recordBytes, idx)
		if err != nil {
			return nil, err
		}

		err = file.Close()
		if err != nil {
			return nil, err
		}

		idx += bytesWritten
	}

	return &models.Data{
		Key:       record.Key,
		Value:     record.Value,
		Tombstone: record.Tombstone,
		Timestamp: record.Timestamp,
	}, nil
}

/*
Read records from a file, insert them into memtable or return the offset where the record with higher timestamp starts from.
Return offset in a file, a buffer with the remaining bytes whose other part is in another file, boolean if the record bytes overflowed from the previous file, and error
*/
func (wal *WAL) readRecordsFromFile(fileName string, buffer []byte, memtable *memtable.Memtable, timestamp uint64, previousOffset int) (int, []byte, bool, error) {
	file, err := os.OpenFile(filepath.Join(Path, fileName), os.O_RDONLY, 0644)
	if err != nil {
		return 0, nil, false, err
	}

	mmapFile, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return 0, nil, false, err
	}

	// Offset from where to start reading in case segments are deleted because of memtable flush
	var offset = int(binary.BigEndian.Uint64(mmapFile[BeginningOffsetStart:BytesOverflowOffsetStart]))
	if offset == 0 {
		// Offset where the record from previous file ends
		offset = int(binary.BigEndian.Uint64(mmapFile[BytesOverflowOffsetStart:HeaderSize])) + HeaderSize
	} else {
		// If the offset is not 0, it means that the previous file got deleted because of memtable flush
		buffer = []byte{}
	}

	// If the last record got deleted based on low watermark, we will ignore the record's remaining bytes
	if len(buffer) != 0 {
		buffer = append(buffer, mmapFile[HeaderSize:offset]...)

		// If the record bytes overflow the current file, we won't deserialize them yet
		if offset != len(mmapFile) {
			record, err := Deserialize(buffer)

			if err != nil {
				return 0, nil, false, err
			}
			if memtable != nil {
				memtable.Put(record.Key, record.Value, record.Timestamp, record.Tombstone)
			} else {
				if record.Timestamp > timestamp {
					return previousOffset, nil, true, nil
				}
			}
			buffer = []byte{}
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
			return 0, nil, false, err
		}

		if memtable != nil {
			memtable.Put(record.Key, record.Value, record.Timestamp, record.Tombstone)
		} else {
			if record.Timestamp > timestamp {
				return i, nil, false, nil
			}
		}

		i += RecordHeaderSize + int(record.KeySize+record.ValueSize)
	}

	// If the record didn't fit in the current file, we store the remaining bytes and move on to the next one
	buffer = append(buffer, mmapFile[i:]...)

	err = mmapFile.Unmap()
	if err != nil {
		return 0, nil, false, err
	}

	err = file.Close()
	if err != nil {
		return 0, nil, false, err
	}

	if len(buffer) != 0 {
		return i, buffer, true, nil
	}
	return 0, buffer, false, nil
}

// ReadRecords Read all records from the Write Ahead Log
func (wal *WAL) ReadRecords(memtable *memtable.Memtable) error {
	files, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	// Buffer for the remaining bytes from the previous file
	var buffer = make([]byte, 0)
	for idx, file := range files {
		_, newBuffer, _, err := wal.readRecordsFromFile(file.Name(), buffer, memtable, 0, 0)
		if err != nil {
			return err
		}

		buffer = newBuffer

		// If there are no more files and the buffer isn't empty, deserialize the last record
		// This can happen if the last file only contains bytes from one record.
		if len(buffer) != 0 && idx == len(files)-1 {
			record, err := Deserialize(buffer)
			if err != nil {
				return err
			}

			memtable.Put(record.Key, record.Value, record.Timestamp, record.Tombstone)
		}
	}

	return nil
}

// findTimestampSegment Find a segment that has a record with the given timestamp, or a newer one (for low watermark)
// Returns file index, and offset inside the file where the record with the higher timestamp starts from
func (wal *WAL) findTimestampSegment(timestamp uint64) (int, int, error) {
	files, err := os.ReadDir(Path)
	if err != nil {
		return 0, 0, err
	}

	// Buffer for the remaining bytes from the previous file
	var buffer = make([]byte, 0)
	var offset int
	for idx, file := range files {
		offset, newBuffer, previousFile, err := wal.readRecordsFromFile(file.Name(), buffer, nil, timestamp, offset)
		if err != nil {
			return 0, 0, err
		}

		buffer = newBuffer

		// If there are no more files and the buffer isn't empty, deserialize the last record
		// This can happen if the last file only contains bytes from one record.
		if len(buffer) != 0 && idx == len(files)-1 {
			record, err := Deserialize(buffer)
			if err != nil {
				return 0, 0, err
			}

			if record.Timestamp > timestamp {
				return idx, offset, nil
			}
		}

		if offset != 0 && len(buffer) == 0 {
			if previousFile {
				return idx - 1, offset, nil
			} else {
				return idx, offset, nil
			}
		}
	}

	return 0, 0, errors.New("no segment found")
}

// MoveLowWatermark Move the low watermark to the segment that has a record with the given timestamp
func (wal *WAL) MoveLowWatermark(timestamp uint64) error {
	// Find the segment that has a record with the given or a newer timestamp
	idx, offset, err := wal.findTimestampSegment(timestamp)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(Path)
	if err != nil {
		return err
	}

	// Put the offset in the file header
	file, err := os.OpenFile(filepath.Join(Path, files[idx].Name()), os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint64(mmapFile[BeginningOffsetStart:BytesOverflowOffsetStart], uint64(offset))

	err = mmapFile.Unmap()
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	// Find the previous low watermark file and remove the low watermark from it
	for fileIndex, file := range files {
		parts := strings.SplitN(file.Name(), fmt.Sprintf("_%s", LowWaterMark), 2)
		if len(parts) != 2 {
			continue
		}

		// If the new file is the same as the old one, don't do anything
		if fileIndex == idx {
			return nil
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
