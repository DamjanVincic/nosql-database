package sstable

import (
	"fmt"
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

	// offset that points to begining of file
	offset := uint64(0)
	countKeysBetween := 0
	for _, entry := range memEntries {
		offset, _ = addToDataSegment(dataFile, entry)
		// handle errors ?
		indexOffset, _ := addToSparseIndex(indexFile, entry, offset)
		if countKeysBetween == 0 || countKeysBetween%SummaryConst == 0 {
			addToSummaryIndex(summaryFile, entry, indexOffset)
		}
		countKeysBetween++
		break
	}

	dataFile.Close()
	indexFile.Close()
	summaryFile.Close()
	filterFile.Close()
	metadataFile.Close()
	tocFile.Close()

	return &SSTable{
		summaryConst:         SummaryConst,
		tableSize:            tableSize,
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

	if err := writeToFile(dataFile, serializedRecord); err != nil {
		return uint64(len(serializedRecord)), err
	}
	return uint64(len(serializedRecord)), nil
}

func writeToFile(dataFile *os.File, binaryData []byte) error {
	_, err := dataFile.Write(binaryData)
	if err != nil {
		return fmt.Errorf("error writing to file: %v", err)
	}
	return nil
}

/*
	func readFromFile(size int, dataFile *os.File) *IndexRecord {
		data, err := ioutil.ReadFile("sstable/sstable1/sst_00001_1_index.db")
		fmt.Println(data)
		if err != nil {
			return nil
		}
		n := data[:size]
		fmt.Println(data)
		dataRecord, err := DeserializeIndexRecord(n)
		fmt.Println(dataRecord)
		return dataRecord
	}
*/
func addToSparseIndex(indexFile *os.File, entry MemEntry, offset uint64) (uint64, error) {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()

	if err := writeToFile(indexFile, serializedIndexRecord); err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	return uint64(len(serializedIndexRecord)), nil
}
func addToSummaryIndex(summaryFile *os.File, entry MemEntry, indexOffset uint64) (uint64, error) {
	indexRecord := NewIndexRecord(entry, indexOffset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()

	if err := writeToFile(summaryFile, serializedIndexRecord); err != nil {
		return uint64(len(serializedIndexRecord)), err
	}
	return uint64(len(serializedIndexRecord)), nil
}
