package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/bloomfilter"
	"github.com/edsrzf/mmap-go"
)

const (
	CrcSize       = 4
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	OffsetSize    = 8
	//for dataRecord
	CrcStart         = 0
	TimestampStart   = CrcStart + CrcSize
	TombstoneStart   = TimestampStart + TimestampSize
	ValueStart       = TombstoneStart + TombstoneSize
	RecordHeaderSize = CrcSize + TimestampSize + TombstoneSize
	SummaryConst     = 5 //from config file
	//for indexRecord
	KeySizeStart = 0
	KeyStart     = KeySizeStart + KeySizeSize
	//sizes of each block in file for SimpleSSTable and size of header which we will use for reading and positioning in file
	//reason why we store offsets in uint64 (8 bytes) is because max value od unit32 is 0.0.00429497 TB
	DataBlockSizeSize    = 8
	IndexBlockSizeSize   = 8
	FilterBlockSizeSize  = 8
	SummaryBlockSizeSize = 8
	HeaderSize           = DataBlockSizeSize + IndexBlockSizeSize + FilterBlockSizeSize + SummaryBlockSizeSize
	DataBlockStart       = 0
	FilterBlockStart     = DataBlockStart + DataBlockSizeSize
	IndexBlockStart      = FilterBlockStart + FilterBlockSizeSize
	SummaryBlockStart    = IndexBlockStart + IndexBlockSizeSize
	MetaBlockStart       = SummaryBlockStart + SummaryBlockSizeSize

	//summary header sizes
	SummaryMinSizeSize  = 8
	SummaryMaxSizeSize  = 8
	SummaryMinSizestart = 0
	SummaryMaxSizeStart = SummaryMinSizestart + SummaryMinSizeSize
	// Path to store SSTable files
	Path = "sstable"
	// Path to store the SimpleSStable file
	SimplePath = "simplesstable"
	// File naming constants for SSTable
	Prefix       = "sst_"
	DataSufix    = "_data"
	IndexSufix   = "_index"
	SummarySufix = "_summary"
	FilterSufix  = "_filter"
	MetaSufix    = "_meta"
	TocSufix     = "_toc"
	//for toc file header (contains lengths of filenames sizes)
	FileNamesSizeSize = 8
	// File naming constants for simpleSSTable
	SimplePrefix = "ssst_"
	SimpleSufix  = "_sss"
)

type MemEntry struct {
	Key   string
	Value *models.Data
}

type SSTable struct {
	summaryConst uint16
	filename     string
}

func NewSSTable(memEntries []*MemEntry, singleFile bool) (*SSTable, error) {
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
	//var filename string
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
		// sort the entries in numerical order, because they are returned in lexicographical
		sort.Slice(dirEntries, func(i, j int) bool {
			numI, _ := strconv.Atoi(dirEntries[i].Name()[7:])
			numJ, _ := strconv.Atoi(dirEntries[j].Name()[7:])
			return numI < numJ
		})

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
	// creates files and save the data
	if singleFile {
		// creating file where everything will be held
		filename := fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, SimpleSufix)
		file, err := os.OpenFile(filepath.Join(subdirPath, filename), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		createFiles(memEntries, file, true)

		err = file.Close()
		if err != nil {
			return nil, err
		}

		return &SSTable{
			summaryConst: SummaryConst,
			filename:     filename,
		}, nil

	} else {
		// Filename format: sst_00001_lsmi_PART.db
		// create names of new files
		dataFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, DataSufix)
		indexFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, IndexSufix)
		summaryFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, SummarySufix)
		filterFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, FilterSufix)
		metadataFilename = fmt.Sprintf("%s%05d_%d%s.db", Prefix, index, lsmIndex, MetaSufix)
		tocFilename = fmt.Sprintf("%s%05d_%d%s.txt", Prefix, index, lsmIndex, TocSufix)

		//create files
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

		err = dataFile.Close()
		if err != nil {
			return nil, err
		}
		err = indexFile.Close()
		if err != nil {
			return nil, err
		}
		err = summaryFile.Close()
		if err != nil {
			return nil, err
		}
		err = filterFile.Close()
		if err != nil {
			return nil, err
		}
		err = metadataFile.Close()
		if err != nil {
			return nil, err
		}

		// write to toc to access later
		writeToTocFile(dataFilename, indexFilename, summaryFilename, filterFilename, metadataFilename, tocFilename)

		createFiles(memEntries, tocFile, false)

		err = tocFile.Close()
		if err != nil {
			return nil, err
		}

		return &SSTable{
			summaryConst: SummaryConst,
			filename:     tocFilename,
		}, nil
	}
}
func writeToTocFile(dataFilename, indexFilename, summaryFilename, filterFilename, metadataFilename, tocFilename string) error {
	tocFile, err := os.OpenFile(tocFilename, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	totalBytes := int64(8 * 5) // for sizes of strings
	totalBytes += int64(len(dataFilename) + len(indexFilename) + len(summaryFilename) + len(metadataFilename))
	data := []byte(strconv.FormatUint(uint64(len(dataFilename)), 10))
	data = append(data, []byte(dataFilename)...)
	data = append(data, []byte(strconv.FormatUint(uint64(len(indexFilename)), 10))...)
	data = append(data, []byte(indexFilename)...)
	data = append(data, []byte(strconv.FormatUint(uint64(len(summaryFilename)), 10))...)
	data = append(data, []byte(summaryFilename)...)
	data = append(data, []byte(strconv.FormatUint(uint64(len(filterFilename)), 10))...)
	data = append(data, []byte(filterFilename)...)
	data = append(data, []byte(strconv.FormatUint(uint64(len(metadataFilename)), 10))...)
	data = append(data, []byte(metadataFilename)...)
	fileStat, err := tocFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()

	if err = tocFile.Truncate(fileSize + totalBytes); err != nil {
		return err
	}
	mmapFile, err := mmap.Map(tocFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], data)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	return nil
}
func readFromToc(tocfile string) ([]string, error) {
	tocFile, err := os.OpenFile(tocfile, os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}
	defer tocFile.Close()

	mmapFile, err := mmap.Map(tocFile, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer mmapFile.Unmap()

	// Read data section
	dataSize := binary.BigEndian.Uint64(mmapFile[:8])
	dataFile := string(mmapFile[8 : 8+dataSize])
	mmapFile = mmapFile[8+dataSize:]

	// Read index section
	indexSize := binary.BigEndian.Uint64(mmapFile[:8])
	indexFile := string(mmapFile[8 : 8+indexSize])
	mmapFile = mmapFile[8+indexSize:]

	// Read summary section
	summarySize := binary.BigEndian.Uint64(mmapFile[:8])
	summaryFile := string(mmapFile[8 : 8+summarySize])
	mmapFile = mmapFile[8+summarySize:]

	// Read metadata section
	metadataSize := binary.BigEndian.Uint64(mmapFile[:8])
	metadataFile := string(mmapFile[8 : 8+metadataSize])

	return []string{dataFile, indexFile, summaryFile, metadataFile}, nil
}

func WriteToFile(file *os.File, data []byte) error {
	totalBytes := int64(len(data))
	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()

	if err = file.Truncate(fileSize + totalBytes); err != nil {
		return err
	}
	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], data)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	return nil
}

func ReadDataFromFile(mmapFile mmap.MMap, offsetStart, offsetEnd uint64) (*DataRecord, error) {
	/*
		mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}
	*/
	serializedDataRecord := mmapFile[offsetStart:offsetEnd]
	dataRecord, err := DeserializeDataRecord(serializedDataRecord)
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
func ReadIndexFromFile(mmapFile mmap.MMap, offsetStart uint64) ([]*IndexRecord, error) {
	var result []*IndexRecord
	/*
		mmapFile, err := mmap.Map(indexFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}
	*/
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
func ReadSummaryFromFile(mmapFile mmap.MMap, key string) (uint64, error) {
	/*
		mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
		if err != nil {
			return 0, err
		}
	*/
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

// mmapFile in case of multi file sstable with be the hole file
// in the case of single file sstable it will only be part that is bloom filter
func ReadBloomFilterFromFile(key string, mmapFile mmap.MMap) (bool, error) {
	/*
		mmapFile, err := mmap.Map(file, mmap.RDONLY, 0)
		if err != nil {
			return false, err
		}
	*/
	filter := bloomfilter.Deserialize(mmapFile)
	err := mmapFile.Unmap()
	if err != nil {
		return false, err
	}
	found, err := filter.ContainsElement([]byte(key))
	if err != nil {
		return false, err
	}
	return found, err
}
func ReadMerkle(mmapFile mmap.MMap) {}
func WriteMerkle()                  {}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// get serialized data
func addToDataSegment(entry *MemEntry, result []byte) (uint64, []byte) {
	dataRecord := *NewDataRecord(entry)
	serializedRecord := dataRecord.SerializeDataRecord()
	return uint64(len(serializedRecord)), append(result, serializedRecord...)
}
func addToIndex(offset uint64, entry *MemEntry, result []byte) (uint64, []byte) {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	return uint64(len(serializedIndexRecord)), append(result, serializedIndexRecord...)
}

// func addToSummaryIndex(entry *MemEntry, indexOffset uint64, result []byte) ([]byte, []byte) {
// 	indexRecord := NewIndexRecord(entry, indexOffset)
// 	serializedIndexRecord := indexRecord.SerializeIndexRecord()
// 	return serializedIndexRecord, append(result, serializedIndexRecord...)
// }

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Get(key string) (*models.Data, error) {
	var mmapFileData mmap.MMap
	var mmapFileFilter mmap.MMap
	var mmapFileSummary mmap.MMap
	var mmapFileIndex mmap.MMap
	var mmapFileMeta mmap.MMap
	fmt.Println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	dirEntries, err := os.ReadDir(Path)
	// if there is no dir to read from return nil
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	//open last added subdir in sstable dir
	//sstable\sstableN
	//sort in numerical order, it is returned in lexicographical
	sort.Slice(dirEntries, func(i, j int) bool {
		numI, _ := strconv.Atoi(dirEntries[i].Name()[7:])
		numJ, _ := strconv.Atoi(dirEntries[j].Name()[7:])
		return numI < numJ
	})
	lastSubDirName := dirEntries[len(dirEntries)-1].Name()
	index, err := strconv.ParseUint(lastSubDirName[7:8], 10, 64)
	lastSubDirNameWithoutIndex := lastSubDirName[0:7]
	if err != nil {
		return nil, err
	}
	for {
		subDirName := lastSubDirNameWithoutIndex + fmt.Sprintf("%d", index)
		subDirPath := filepath.Join(Path, subDirName)
		subDirEntries, err := os.ReadDir(subDirPath)
		if os.IsNotExist(err) {
			err := os.Mkdir(subDirPath, os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
		if len(subDirEntries) == 1 {
			// we get data from single file sstable
			simpleFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
			// open simple sstable file
			simpleFile, err := os.OpenFile(simpleFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			mmapFileSimple, err := mmap.Map(simpleFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			//get sizes of each part of SimpleSSTable
			header := mmapFileSimple[:HeaderSize]
			datasize := uint64(binary.BigEndian.Uint64(header[:8]))
			filtersize := uint64(binary.BigEndian.Uint64(header[8:16]))
			indexsize := uint64(binary.BigEndian.Uint64(header[16:24]))
			summarysize := uint64(binary.BigEndian.Uint64(header[24:]))

			dataStart := HeaderSize
			filterStart := dataStart + int(datasize)
			indexStart := filterStart + int(filtersize)
			summaryStart := indexStart + int(indexsize)
			metaStart := summaryStart + int(summarysize)

			// same as hole mmap file in multi file sstable
			mmapFileDataPart := mmapFileSimple[dataStart:filterStart]
			mmapFileData = make([]byte, len(mmapFileDataPart))
			copy(mmapFileData, mmapFileDataPart)

			mmapFileFilterPart := mmapFileSimple[filterStart:indexStart]
			mmapFileFilter = make([]byte, len(mmapFileFilterPart))
			copy(mmapFileFilter, mmapFileFilterPart)

			mmapFileIndexPart := mmapFileSimple[indexStart:summaryStart]
			mmapFileIndex = make([]byte, len(mmapFileIndexPart))
			copy(mmapFileIndex, mmapFileIndexPart)

			mmapFileSummaryPart := mmapFileSimple[summaryStart:metaStart]
			mmapFileSummary = make([]byte, len(mmapFileSummaryPart))
			copy(mmapFileSummary, mmapFileSummaryPart)

			mmapFileMetaPart := mmapFileSimple[metaStart:]
			mmapFileMeta = make([]byte, len(mmapFileMetaPart))
			copy(mmapFileMeta, mmapFileMetaPart)

			// close the file
			err = mmapFileSimple.Unmap()
			if err != nil {
				return nil, err
			}
			err = simpleFile.Close()
			if err != nil {
				return nil, err
			}
			return nil, nil
		} else {
			// paths : sstable\\sstableN\\sst_00001_1_part.db
			dataFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
			filterFilePath := filepath.Join(subDirPath, subDirEntries[1].Name())
			indexFilePath := filepath.Join(subDirPath, subDirEntries[2].Name())
			metaFilePath := filepath.Join(subDirPath, subDirEntries[3].Name())
			summaryFilePath := filepath.Join(subDirPath, subDirEntries[4].Name())
			tocFilePath := filepath.Join(subDirPath, subDirEntries[5].Name())

			// get data file to read
			dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR, 0644)
			mmapFileDataPart, err := mmap.Map(dataFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileData = make([]byte, len(mmapFileDataPart))
			copy(mmapFileData, mmapFileDataPart)

			// get filter to read
			filterFile, err := os.OpenFile(filterFilePath, os.O_RDWR, 0644)
			mmapFileFilterPart, err := mmap.Map(filterFile, mmap.RDONLY, 0)
			if err != nil {
				return nil, err
			}
			mmapFileFilter = make([]byte, len(mmapFileFilterPart))
			copy(mmapFileFilter, mmapFileFilterPart)

			// get index file to read
			indexFile, err := os.OpenFile(indexFilePath, os.O_RDWR, 0644)
			mmapFileIndexPart, err := mmap.Map(indexFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileIndexPart = make([]byte, len(mmapFileIndexPart))
			copy(mmapFileIndex, mmapFileIndexPart)
			metaFile, err := os.OpenFile(metaFilePath, os.O_RDWR, 0644)

			mmapFileMetaPart, err := mmap.Map(metaFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileMeta = make([]byte, len(mmapFileMetaPart))
			copy(mmapFileMeta, mmapFileMetaPart)

			summaryFile, err := os.OpenFile(summaryFilePath, os.O_RDWR, 0644)
			// get summary
			mmapFileSummaryPart, err := mmap.Map(summaryFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileSummary = make([]byte, len(mmapFileSummaryPart))
			copy(mmapFileSummary, mmapFileSummaryPart)
			tocFile, err := os.OpenFile(tocFilePath, os.O_RDWR, 0644)

			err = mmapFileDataPart.Unmap()
			if err != nil {
				return nil, err
			}
			err = mmapFileFilterPart.Unmap()
			if err != nil {
				return nil, err
			}
			err = mmapFileSummaryPart.Unmap()
			if err != nil {
				return nil, err
			}
			err = mmapFileIndexPart.Unmap()
			if err != nil {
				return nil, err
			}
			err = mmapFileMetaPart.Unmap()
			if err != nil {
				return nil, err
			}
			err = dataFile.Close()
			if err != nil {
				return nil, err
			}
			err = filterFile.Close()
			if err != nil {
				return nil, err
			}
			err = indexFile.Close()
			if err != nil {
				return nil, err
			}
			err = metaFile.Close()
			if err != nil {
				return nil, err
			}
			err = tocFile.Close()
			if err != nil {
				return nil, err
			}
		}
		// start process for getting the element
		// first we need to check if its in bloom filter
		found, err := ReadBloomFilterFromFile(key, mmapFileFilter)
		if !found {
			index--
			if index == 0 {
				return nil, nil
			}
			continue
		}
		indexOffset, err := ReadSummaryFromFile(mmapFileSummary, key)
		if err != nil {
			return nil, err
		}
		indexRecords, err := ReadIndexFromFile(mmapFileIndex, indexOffset)
		for i := 0; i < len(indexRecords); i++ {
			if indexRecords[i].key == key {
				dataRecord, err := ReadDataFromFile(mmapFileData, indexRecords[i].offset, indexRecords[i+1].offset)
				if err != nil {
					return nil, err
				}
				return &models.Data{Value: dataRecord.value, Tombstone: dataRecord.tombstone, Timestamp: dataRecord.timestamp}, nil
			}
		}
		break

	}
	return nil, nil
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// file name will be toc file for multi file sstable
// and file for single file sstable
func createFiles(memEntries []*MemEntry, file *os.File, singleFile bool) error {
	var data []byte
	dataSize := int64(0)
	var dataRecords []byte
	var indexRecords []byte
	var summaryRecords []byte
	summaryHeader := make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize)

	var summaryMin []byte
	var summaryMax []byte
	var serializedIndexRecord []byte

	// for single file header
	dataBlockSize := uint64(0)
	indexBlockSize := uint64(0)
	summaryBlockSize := uint64(0)
	metaBlockSize := uint64(0)
	filterBlockSize := uint64(0)

	sizeOfDR := uint64(0)
	sizeOfIR := uint64(0)
	sizeOfSR := uint64(0)

	// set offset to 0 if its multi file
	// set it right after header if its single file, after header are data records
	offset := uint64(0)
	indexOffset := uint64(0) // index block size from ssstable
	if singleFile {
		offset = uint64(HeaderSize)
		indexOffset = uint64(HeaderSize) // index block size from ssstable

	}
	// counter for index summary, for every n index records add one to summary
	countKeysBetween := 0
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.2)
	merkle, err := CreateMerkleTree(memEntries)
	if err != nil {
		return err
	}
	// proccess of adding entries
	for _, entry := range memEntries {
		sizeOfDR, dataRecords = addToDataSegment(entry, dataRecords)
		sizeOfIR, indexRecords = addToIndex(offset, entry, indexRecords)
		// keep track of size of the block for single file sstable
		dataBlockSize += sizeOfDR
		indexBlockSize += sizeOfIR
		if countKeysBetween%SummaryConst == 0 {
			sizeOfSR, summaryRecords = addToIndex(indexOffset, entry, summaryRecords)
			summaryBlockSize += sizeOfSR
			if summaryMin == nil {
				summaryMin = serializedIndexRecord
			}
		}
		filter.AddElement([]byte(entry.Key))
		summaryMax = serializedIndexRecord
		countKeysBetween++
		offset += sizeOfDR
		indexOffset += sizeOfIR
	}
	filterData := filter.Serialize()
	filterBlockSize = uint64(len(filterData))
	merkleData := merkle.Serialize()

	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizestart:SummaryMaxSizeStart], uint64(len(summaryMin)))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len(summaryMax)))
	summaryHeader = append(summaryHeader, summaryMin...)
	summaryHeader = append(summaryHeader, summaryMax...)
	summaryHeader = append(summaryHeader, summaryRecords...)
	summaryBlockSize += uint64(len(summaryHeader))

	if singleFile {
		header := make([]byte, HeaderSize)
		binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
		binary.BigEndian.PutUint64(header[FilterBlockStart:IndexBlockStart], filterBlockSize)
		binary.BigEndian.PutUint64(header[IndexBlockStart:SummaryBlockStart], indexBlockSize)
		binary.BigEndian.PutUint64(header[SummaryBlockStart:MetaBlockStart], summaryBlockSize)
		binary.BigEndian.PutUint64(header[MetaBlockStart:], metaBlockSize)

		//size of all data that needs to be written to mmap
		dataSize = dataSize + int64(dataBlockSize) + int64(filterBlockSize) + int64(indexBlockSize) + int64(summaryBlockSize) + int64(metaBlockSize) + HeaderSize
		data = append(data, header...)
		data = append(data, dataRecords...)
		data = append(data, filterData...)
		data = append(data, indexRecords...)
		data = append(data, summaryHeader...)
		data = append(data, summaryRecords...)
		data = append(data, merkleData...)
	}

	tocData := serializeTocData(fileNames)

	// upis u fajl i izmedju cuvaj sve velicine i to sto treba
	err = WriteToFile(dataFile, dataRecords)
	if err != nil {
		return err
	}
	err = WriteToFile(indexFile, indexRecords)
	if err != nil {
		return err
	}
	err = WriteToFile(summaryFile, summaryRecords)
	if err != nil {
		return err
	}
	err = WriteToFile(filterFile, filterData)
	if err != nil {
		return err
	}
	err = WriteToFile(metadataFile, merkleData)
	if err != nil {
		return err
	}
	err = WriteToFile(tocFile, tocData)
	if err != nil {
		return err
	}
	return nil
}

func serializeTocData(fileNames []*string) []byte {
	filenamesSize := make([]byte, 40)
	filenames := make([]byte, 0)
	i := 0
	for fileName := range fileNames {
		filenames = append(filenames, byte(fileName))
		binary.BigEndian.PutUint64(filenamesSize[FileNamesSizeSize*i:FileNamesSizeSize*i+FileNamesSizeSize], uint64(len(filenames)))
		i++
	}
	filenames = append(filenamesSize, filenames...)
	return filenames
}
