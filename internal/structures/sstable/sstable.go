package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/bloomfilter"
	"github.com/edsrzf/mmap-go"
)

func NewSSTable2(memEntries []MemEntry) (*SSTable, error) {
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
		dirEntries, err := os.ReadDir(subdirPath)
		fmt.Println(dirEntries)
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

	createFiles(memEntries, dataFilename, indexFilename, summaryFilename, filterFile)

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

func WriteToDataFile(file string, data []byte) error {
	dataFile, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	flatData := make([]byte, 8)
	flatData = append(flatData, data...)
	totalBytes := int64(len(data))
	//*******************************************************

	fileStat, err := dataFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()
	if err = dataFile.Truncate(fileSize + totalBytes); err != nil {
		return err
	}
	mmapFile, err := mmap.Map(dataFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], flatData)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	err = dataFile.Close()
	if err != nil {
		return err
	}
	return nil
}
func ReadDataFromFile(file *os.File, offsetStart, offsetEnd uint64) (*DataRecord, error) {
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

func WriteToIndexFile(file string, data []byte) error {
	indexFile, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	flatData := make([]byte, 8)
	flatData = append(flatData, data...)
	totalBytes := int64(len(data))
	fileStat, err := indexFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()

	if err = indexFile.Truncate(totalBytes + fileSize); err != nil {
		return err
	}
	mmapFile, err := mmap.Map(indexFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], flatData)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	err = indexFile.Close()
	if err != nil {
		return err
	}
	return nil
}

// it returns data offset
func ReadIndexFromFile(indexFile *os.File, offsetStart uint64) ([]*IndexRecord, error) {
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
func WriteSummaryToFile(file string, data, summaryMin, summaryMax []byte) error {
	summaryFile, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	summaryMinSize := int64(len(summaryMin))
	summaryMaxSize := int64(len(summaryMax))
	totalBytes := int64(len(data))
	totalBytes += summaryMinSize
	totalBytes += summaryMaxSize
	flatData := make([]byte, 8)
	flatData = append(flatData, summaryMin...)
	flatData = append(flatData, summaryMax...)
	flatData = append(flatData, data...)

	fileStat, err := summaryFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()
	if err = summaryFile.Truncate(totalBytes + fileSize); err != nil {
		return err
	}
	mmapFile, err := mmap.Map(summaryFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], flatData)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	err = summaryFile.Close()
	if err != nil {
		return err
	}
	return nil
}

// it returns index offset
func ReadSummaryFromFile(file *os.File, key string) (uint64, error) {
	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return 0, err
	}
	mmapFileSize := uint64(len(mmapFile))
	summaryMinSize := binary.BigEndian.Uint64(mmapFile[SummaryMinSizestart:SummaryMaxSizeStart])
	summaryMaxSize := binary.BigEndian.Uint64(mmapFile[SummaryMaxSizeStart : SummaryMaxSizeStart+SummaryMaxSizeSize])

	keysStart := uint64(SummaryMaxSizeStart + SummaryMaxSizeSize)
	serializedSummaryMin := mmapFile[keysStart : keysStart+summaryMinSize]
	serializedSummaryMax := mmapFile[keysStart+summaryMinSize : keysStart+summaryMinSize+summaryMaxSize]
	mmapFile = mmapFile[keysStart+summaryMinSize+summaryMaxSize:]
	summaryMin, err := DeserializeIndexRecord(serializedSummaryMin)
	if err != nil {
		return 0, err
	}
	summaryMax, err := DeserializeIndexRecord(serializedSummaryMax)
	if err != nil {
		return 0, err
	}
	// check if key is in range of summary indexes
	if key < summaryMin.key || key > summaryMax.key {
		return 0, errors.New("key not in range of summary index table")
	}
	var summaryRecords []*IndexRecord
	offset := uint64(0)
	for offset < mmapFileSize {
		keySize := binary.BigEndian.Uint64(mmapFile[KeySizeStart:KeySizeSize])

		indexOffset := binary.BigEndian.Uint64(mmapFile[KeyStart+int(keySize):])
		offset = KeySizeSize + keySize + indexOffset
		indexRecord, err := DeserializeIndexRecord(mmapFile[:offset])
		if err != nil {
			return 0, errors.New("error deserializing index record")
		}
		summaryRecords = append(summaryRecords, indexRecord)
		if len(summaryRecords) >= 2 && summaryRecords[len(summaryRecords)-1].key > key && summaryRecords[len(summaryRecords)-2].key < key {
			return summaryRecords[len(summaryRecords)-1].offset, nil
		}
		mmapFile = mmapFile[offset:]
	}
	return 0, errors.New("key not found")
}
func WriteBloomFilter(filter bloomfilter.BloomFilter, filterFile *os.File) error {
	// First 4 bytes are the number of bytes in the byte array
	// Next 4 bytes are the number of hash functions

	data := filter.Serialize()
	totalBytes := int64(len(data))
	//var flatData []byte
	//flatData = append(flatData, data...)
	fileStat, err := filterFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()

	if err = filterFile.Truncate(int64(fileSize + totalBytes)); err != nil {
		return err
	}
	mmapFile, err := mmap.Map(filterFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], data)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	err = filterFile.Close()
	if err != nil {
		return err
	}
	return nil
}
func ReadBloomFilterFromFile(key string, file *os.File) (bool, error) {
	mmapFile, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return false, err
	}
	filter := bloomfilter.Deserialize(mmapFile)
	err = mmapFile.Unmap()
	if err != nil {
		return false, err
	}
	return filter.ContainsElement([]byte(key))
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// get serialized data
func addToDataSegment(entry MemEntry, result []byte) (uint64, []byte) {
	dataRecord := NewDataRecord(entry)
	serializedRecord := dataRecord.SerializeDataRecord()
	return uint64(len(serializedRecord)), append(result, serializedRecord...)
}
func addToIndex(offset uint64, entry MemEntry, result []byte) (uint64, []byte) {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	return uint64(len(serializedIndexRecord)), append(result, serializedIndexRecord...)
}
func addToSummaryIndex(entry MemEntry, indexOffset uint64, result []byte) ([]byte, []byte) {
	indexRecord := NewIndexRecord(entry, indexOffset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	return serializedIndexRecord, append(result, serializedIndexRecord...)
}
func (sstable SSTable) Get(key string) (*models.Data, error) {
	fmt.Println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	//open last added subdir in sstable dir
	//sstable\sstableN
	lastSubDirName := dirEntries[len(dirEntries)-1].Name()
	index, err := strconv.ParseUint(lastSubDirName[7:8], 10, 64)
	if err != nil {
		return nil, err
	}
	subDirPath := filepath.Join(Path, lastSubDirName)
	subDirEntries, err := os.ReadDir(subDirPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(subDirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	// paths : sstable\\sstableN\\sst_00001_1_part.db
	dataFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
	filterFilePath := filepath.Join(subDirPath, subDirEntries[1].Name())
	indexFilePath := filepath.Join(subDirPath, subDirEntries[2].Name())
	metaFilePath := filepath.Join(subDirPath, subDirEntries[3].Name())
	summaryFilePath := filepath.Join(subDirPath, subDirEntries[4].Name())
	tocFilePath := filepath.Join(subDirPath, subDirEntries[5].Name())

	dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR, 0644)
	filterFile, err := os.OpenFile(filterFilePath, os.O_RDWR, 0644)
	indexFile, err := os.OpenFile(indexFilePath, os.O_RDWR, 0644)
	metaFile, err := os.OpenFile(metaFilePath, os.O_RDWR, 0644)
	summaryFile, err := os.OpenFile(summaryFilePath, os.O_RDWR, 0644)
	tocFile, err := os.OpenFile(tocFilePath, os.O_RDWR, 0644)
	fmt.Println(metaFile, tocFile)
	found, err := ReadBloomFilterFromFile(key, filterFile)
	if !found || err != nil {
		return nil, err
	}
	indexOffset, err := ReadSummaryFromFile(summaryFile, key)
	if err != nil {
		return nil, err
	}
	indexRecords, err := ReadIndexFromFile(indexFile, indexOffset)
	for i := 0; i < len(indexRecords); i++ {
		if indexRecords[i].key == key {
			dataRecord, err := ReadDataFromFile(dataFile, indexRecords[i].offset, indexRecords[i+1].offset)
			if err != nil {
				return nil, err
			}
			return &models.Data{Value: dataRecord.value, Tombstone: dataRecord.tombstone, Timestamp: dataRecord.timestamp}, nil
		}
	}
	index--
	return nil, nil
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func createFiles(memEntries []MemEntry, dataFile, indexFile, summaryFile string, filterFile *os.File) {
	dataRecords := []byte{}
	indexRecords := []byte{}
	summaryRecords := []byte{}
	var indexOffset uint64
	var summaryMin []byte
	var summaryMax []byte
	var serializedIndexRecord []byte
	offset := uint64(0)
	countKeysBetween := 0
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.2)
	for _, entry := range memEntries {
		offset, dataRecords = addToDataSegment(entry, dataRecords)
		indexOffset, indexRecords = addToIndex(offset, entry, indexRecords)
		if countKeysBetween == 0 || countKeysBetween%SummaryConst == 0 {
			serializedIndexRecord, summaryRecords = addToSummaryIndex(entry, indexOffset, summaryRecords)
			if summaryMin == nil {
				summaryMin = serializedIndexRecord
			}
		}
		filter.AddElement([]byte(entry.Key))
		summaryMax = serializedIndexRecord
		countKeysBetween++
		break
	}

	summaryHeader := make([]byte, 16)
	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizestart:SummaryMaxSizeStart], uint64(len(summaryMin)))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len(summaryMax)))
	summaryHeader = append(summaryHeader, summaryMin...)
	summaryHeader = append(summaryHeader, summaryMax...)

	WriteToDataFile(dataFile, dataRecords)
	WriteToIndexFile(indexFile, indexRecords)
	WriteSummaryToFile(summaryFile, summaryRecords, summaryMin, summaryMax)
	WriteBloomFilter(filter, filterFile)
}
