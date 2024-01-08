package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/structures/bloomfilter"
	"github.com/edsrzf/mmap-go"
	"os"
	"path/filepath"
	"strconv"
)

func NewSSTable2(memEntries []MemEntry, tableSize uint64) (*SSTable, error) {
	// Create the directory if it doesn't exist
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	var dataFilename string
	var indexFilename string
	var summaryFilename string
	var filterFilename string
	var metadataFilename string
	var tocFilename string

	lsmIndex := uint8(1)
	var index uint8
	var subdirPath string
	var subdirName string

	// If there are no subdirectoriums in the directory, create the first one
	if len(dirEntries) == 0 {
		// subdirName : sstableN (N - index)
		index = 1
		subdirName = fmt.Sprintf("sstable%d", index)
		subdirPath = filepath.Join(Path, subdirName)
		err := os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	} else {
		// Get the last file
		subdirName = dirEntries[len(dirEntries)-1].Name()
		fmt.Println(subdirName)
		n, err := strconv.ParseUint(subdirName[7:], 10, 8)
		index = uint8(n)
		if err != nil {
			return nil, err
		}
		fmt.Println(index)
		index++
		subdirName = fmt.Sprintf("sstable%d", index)
		subdirPath = filepath.Join(Path, subdirName)
		err = os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	// Filename format: sst_00001_lsmi_PART.db
	dataFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, DataSufix)
	indexFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, IndexSufix)
	summaryFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, SummarySufix)
	filterFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, FilterSufix)
	metadataFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, MetaSufix)
	tocFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, TocSufix)

	dataFile, err := os.OpenFile(filepath.Join(subdirPath, dataFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(filepath.Join(subdirPath, indexFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	summaryFile, err := os.OpenFile(filepath.Join(subdirPath, summaryFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	filterFile, err := os.OpenFile(filepath.Join(subdirPath, filterFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	metadataFile, err := os.OpenFile(filepath.Join(subdirPath, metadataFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	tocFile, err := os.OpenFile(filepath.Join(subdirPath, tocFilename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	createFiles(memEntries, dataFile, indexFile, summaryFile, filterFile)

	dataFile.Close()
	indexFile.Close()
	summaryFile.Close()
	filterFile.Close()
	metadataFile.Close()
	tocFile.Close()

	return &SSTable{
		summaryConst:         SummaryConst,
		dataFilename:         dataFilename,
		indexFilename:        indexFilename,
		summaryIndexFilename: summaryFilename,
		filterFilename:       filterFilename,
		metadataFilename:     metadataFilename,
		tocFilename:          tocFilename,
	}, nil
}
func addToDataSegment(dataFile *os.File, entry MemEntry) (uint64, error) {
	dataRecord := NewDataRecord(entry)
	serializedRecord := dataRecord.SerializeDataRecord()
	//*******************************************************
	flatData := make([]byte, 8)
	flatData = append(flatData, serializedRecord...)
	totalBytes := int64(len(serializedRecord))
	//*******************************************************

	fileStat, err := dataFile.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := fileStat.Size()
	if err = dataFile.Truncate(fileSize + totalBytes); err != nil {
		return 0, err
	}
	mmapFile, err := mmap.Map(dataFile, mmap.RDWR, 0)
	if err != nil {
		return 0, err
	}
	copy(mmapFile[fileSize:], flatData)
	err = mmapFile.Unmap()
	if err != nil {
		return 0, err
	}
	err = dataFile.Close()
	if err != nil {
		return 0, err
	}
	return uint64(len(serializedRecord)), nil
}
func ReadFromFile(file *os.File, offsetStart, offsetEnd int) (*DataRecord, error) {
	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileStat.Size()
	fmt.Println(fileSize)

	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	serializedDataRecord := mmapFile[offsetStart:offsetEnd]
	dataRecord, err := DeserializeDataRecord(serializedDataRecord)
	if err != nil {
		return nil, err
	}
	err = mmapFile.Unmap()
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	return dataRecord, nil
}
func writeToFile(dataFile *os.File, binaryData []byte) error {
	_, err := dataFile.Write(binaryData)
	if err != nil {
		return fmt.Errorf("error writing to file: %v", err)
	}
	return nil
}

func addToSparseIndex(indexFile *os.File, entry MemEntry, offset uint64) (uint64, error) {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	//****************************************************************
	totalBytes := int64(len(serializedIndexRecord))
	fileStat, err := indexFile.Stat()
	if err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	fileSize := fileStat.Size()
	flatData := make([]byte, 8)
	flatData = append(flatData, serializedIndexRecord...)

	if err = indexFile.Truncate(totalBytes + fileSize); err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	mmapFile, err := mmap.Map(indexFile, mmap.RDWR, 0)
	if err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	copy(mmapFile[fileSize:], flatData)
	err = mmapFile.Unmap()
	if err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	err = indexFile.Close()
	if err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	return uint64(len(serializedIndexRecord)), nil
}
func ReadIndexFromFile(indexFile *os.File, offsetStart, offsetEnd uint64) ([]*IndexRecord, error) {
	var result []*IndexRecord
	mmapFile, err := mmap.Map(indexFile, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	// 8 for size of key size
	// key size for key
	// 8 for offset
	mmapFileSize := uint64(len(mmapFile))
	mmapFile = mmapFile[offsetStart:]
	offset := uint64(0)
	for offset < mmapFileSize {
		keySize := binary.BigEndian.Uint64(mmapFile[KeySizeStart:KeySizeSize])
		key := string(mmapFile[KeyStart : KeyStart+int(keySize)])
		keyUint64, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return nil, errors.New("error converting")
		}
		indexOffset := binary.BigEndian.Uint64(mmapFile[KeyStart+int(keySize):])
		offset = keySize + keyUint64 + indexOffset
		indexRecord, err := DeserializeIndexRecord(mmapFile[:offset])
		if err != nil {
			return nil, errors.New("error deserializing index record")
		}
		result = append(result, indexRecord)
		mmapFile = mmapFile[offset:]
	}
	return result, nil
}
func addToSummaryIndex(summaryFile *os.File, entry MemEntry, indexOffset uint64) (uint64, error) {
	indexRecord := NewIndexRecord(entry, indexOffset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()

	if err := writeToFile(summaryFile, serializedIndexRecord); err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	return uint64(len(serializedIndexRecord)), nil
}
func saveBloomFilter(filter bloomfilter.BloomFilter, filterFile *os.File) error {
	serializedFilter := filter.Serialize()
	err := writeToFile(filterFile, serializedFilter)
	if err != nil {
		return err
	}
	return nil
}
func createFiles(memEntries []MemEntry, dataFile, indexFile, summaryFile, filterFile *os.File) {
	// offset that points to begining of file
	offset := uint64(0)
	countKeysBetween := 0
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.2)
	for _, entry := range memEntries {
		offset, _ = addToDataSegment(dataFile, entry)
		// handle errors ?
		indexOffset, _ := addToSparseIndex(indexFile, entry, offset)
		if countKeysBetween == 0 || countKeysBetween%SummaryConst == 0 {
			addToSummaryIndex(summaryFile, entry, indexOffset)
		}
		filter.AddElement([]byte(entry.Key))
		countKeysBetween++
		break
	}
	saveBloomFilter(filter, filterFile)
}
