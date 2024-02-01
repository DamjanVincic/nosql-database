package lsm

import (
	"encoding/binary"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/edsrzf/mmap-go"
	"os"
)

func Compact(sstables []string) ([]*models.Data, error) {
	var maps []mmap.MMap
	for _, sstable := range sstables {
		currentFile, err := os.OpenFile(sstable, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		m, err := mmap.Map(currentFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}
		maps = append(maps, m)
	}
	return combineSSTables(maps)
}

// sstables - list of serialized data records
func combineSSTables(sstables []mmap.MMap) ([]*models.Data, error) {
	var result []*models.Data
	numberOfSSTables := len(sstables)
	// we keep where we left of in each sstable and last key
	offsets := make([]uint64, numberOfSSTables)
	current := make([]*models.DataRecord, numberOfSSTables)

	// read first record from each sstable
	for i := 0; i < numberOfSSTables; i++ {
		dataRecord, err := readDataFromFile(sstables[i], offsets[i])
		if err != nil {
			return nil, err
		}
		if dataRecord != nil {
			// set offsets for next
			offsets[i] = uint64(len(dataRecord.Data.Key) + KeySizeSize + ValueSizeSize + len(dataRecord.Data.Value) + TombstoneSize + TimestampSize + CrcSize)
		}
		current[i] = dataRecord
	}
	// find the smallest key
	// we know that the index we return is the sstable we read next (or multiple)
	for len(sstables) > 0 {
		smallestKey := findSmallestKey(current)
		if smallestKey == nil {
			break
		}
		var minRecord *models.Data
		for _, keyIndex := range smallestKey {
			if minRecord == nil {
				minRecord = current[keyIndex].Data
			}
			if minRecord.Timestamp < current[keyIndex].Data.Timestamp {
				minRecord = current[keyIndex].Data
			}
			dataRecord, err := readDataFromFile(sstables[keyIndex], offsets[keyIndex])
			if err != nil {
				return nil, err
			}
			if dataRecord != nil {
				// set offsets for next
				offsets[keyIndex] += uint64(len(dataRecord.Data.Key) + CrcSize + KeySizeSize + ValueSizeSize + len(dataRecord.Data.Value) + TombstoneSize + TimestampSize)
			}
			current[keyIndex] = dataRecord
		}
		result = append(result, minRecord)
	}

	return result, nil
}
func findSmallestKey(current []*models.DataRecord) []int {
	var result []int
	for i := 0; i < len(current); i++ {
		if current[i] == nil {
			continue
		}
		if len(result) == 0 {
			// we have nothing in result and add the first one
			result = append(result, i)
		} else
		// if we found another one with this key add it
		// we need to check timestamp
		if current[i].Data.Key == current[result[0]].Data.Key {
			result = append(result, i)
		} else
		// new min
		if current[i].Data.Key < current[result[0]].Data.Key {
			result = make([]int, 0)
			result = append(result, i)
		}
	}
	return result
}
func removeAtIndex(slice []uint64, index int) []uint64 {
	// Ensure that the index is within bounds
	if index < 0 || index >= len(slice) {
		return slice
	}

	// Remove the element at the specified index
	return append(slice[:index], slice[index+1:]...)
}

const (
	CrcSize       = 4
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	OffsetSize    = 8
	ValueSizeSize = 8

	//for dataRecord
	CrcStart           = 0
	TimestampStart     = CrcStart + CrcSize
	TombstoneStart     = TimestampStart + TimestampSize
	DataKeySizeStart   = TombstoneStart + TombstoneSize
	DataValueSizeStart = DataKeySizeStart + KeySizeSize
	DataKeyStart       = DataValueSizeStart + ValueSizeSize
	RecordHeaderSize   = CrcSize + TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize

	//for indexRecord
	KeySizeStart = 0
	KeyStart     = KeySizeStart + KeySizeSize

	//sizes of each block in file for single file SSTable and size of header which we will use for reading and positioning in file
	//reason why we store offsets in uint64 (8 bytes) is because max value od unit32 is 0.0.00429497 TB
	DataBlockSizeSize    = 8
	IndexBlockSizeSize   = 8
	FilterBlockSizeSize  = 8
	SummaryBlockSizeSize = 8
	HeaderSize           = DataBlockSizeSize + IndexBlockSizeSize + FilterBlockSizeSize + SummaryBlockSizeSize
	DataBlockStart       = 0
	IndexBlockStart      = DataBlockStart + DataBlockSizeSize
	SummaryBlockStart    = IndexBlockStart + IndexBlockSizeSize
	FilterBlockStart     = SummaryBlockStart + SummaryBlockSizeSize
	MetaBlockStart       = FilterBlockStart + FilterBlockSizeSize

	//summary header sizes
	SummaryConstSize    = 2
	SummaryMinSizeSize  = 8
	SummaryMaxSizeSize  = 8
	SummaryConstStart   = 0
	SummaryMinSizeStart = SummaryConstStart + SummaryConstSize
	SummaryMaxSizeStart = SummaryMinSizeStart + SummaryMinSizeSize

	//index header for index thinning
	IndexConstSize = 2

	// Path to store SSTable files
	Path = "sstable"

	// Path to store the SStable single file
	// File naming constants for SSTable
	DataFileName    = "data"
	IndexFileName   = "index"
	SummaryFileName = "summary"
	FilterFileName  = "filter"
	MetaFileName    = "meta"
	SingleFileName  = "single"
)

func readDataFromFile(mmapFile mmap.MMap, offset uint64) (*models.DataRecord, error) {
	dataRecordSize := uint64(0)
	//read IndexConst number of data records
	if offset >= uint64(len(mmapFile)) {
		return nil, nil
	}
	tombstone := mmapFile[offset+TombstoneStart] == 1
	keySize := binary.BigEndian.Uint64(mmapFile[offset+DataKeySizeStart : offset+DataValueSizeStart])
	var valueSize uint64
	if !tombstone {
		valueSize = binary.BigEndian.Uint64(mmapFile[offset+DataValueSizeStart : offset+DataKeyStart])
	}

	// make sure to read complete data rec
	dataRecordSize = RecordHeaderSize + keySize + valueSize
	if tombstone {
		dataRecordSize -= ValueSizeSize
	}
	dataRecord, err := models.Deserialize(mmapFile[offset : offset+dataRecordSize])
	if err != nil {
		return nil, err
	}
	return dataRecord, nil
}
