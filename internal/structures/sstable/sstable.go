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
	SimpleSufix = "_sss"
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
		subdirName = fmt.Sprintf("%02d_sstable_%05d", lsmIndex, index)
		subdirPath = filepath.Join(Path, subdirName)
		err := os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	} else {
		// Get the last file
		// sort the entries in numerical order, because they are returned in lexicographical
		sort.Slice(dirEntries, func(i, j int) bool {
			numI, _ := strconv.Atoi(dirEntries[i].Name()[:])
			numJ, _ := strconv.Atoi(dirEntries[j].Name()[:])
			return numI < numJ
		})

		subdirName = dirEntries[len(dirEntries)-1].Name()
		fmt.Println(subdirName)
		n, err := strconv.ParseUint(subdirName[11:], 10, 8)
		index = uint8(n)
		if err != nil {
			return nil, err
		}
		fmt.Println(index)
		index++
		subdirName = fmt.Sprintf("%02d_sstable_%05d", lsmIndex, index)
		subdirPath = filepath.Join(Path, subdirName)
		err = os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
		dirEntries, err := os.ReadDir(subdirPath)
		if err != nil {
			return nil, err
		}
		fmt.Println(dirEntries)
		// err = os.Mkdir(subdirPath, os.ModePerm)
		// if err != nil {
		// 	return nil, err
		// }
	}
	// creates files and save the data
	if singleFile {
		// creating file where everything will be held
		// create the name of the file and open it
		// in said file then write all data
		filename := fmt.Sprintf("%s%05d%s.db", Prefix, index, SimpleSufix)
		file, err := os.OpenFile(filepath.Join(subdirPath, filename), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		err = createFiles(memEntries, true, file)
		if err != nil {
			return nil, err
		}

		err = file.Close()
		if err != nil {
			return nil, err
		}

		return &SSTable{
			summaryConst: SummaryConst,
			filename:     filename,
		}, nil

	} else {
		// Filename format: sst_00001_PART.db
		// create names of new files
		dataFilename = fmt.Sprintf("%s%05d%s.db", Prefix, index, DataSufix)
		indexFilename = fmt.Sprintf("%s%05d%s.db", Prefix, index, IndexSufix)
		summaryFilename = fmt.Sprintf("%s%05d%s.db", Prefix, index, SummarySufix)
		filterFilename = fmt.Sprintf("%s%05d%s.db", Prefix, index, FilterSufix)
		metadataFilename = fmt.Sprintf("%s%05d%s.db", Prefix, index, MetaSufix)
		tocFilename = fmt.Sprintf("%s%05d%s.db", Prefix, index, TocSufix)

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

		// write to toc to access later

		err = createFiles(memEntries, false, dataFile, indexFile, summaryFile, filterFile, metadataFile, tocFile)
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
func createTocData(dataFilename, indexFilename, summaryFilename, filterFilename, metadataFilename string) []byte {
	// there is 5 files and each size of the file is of size 8
	totalBytes := int64(8 * 5) // for sizes of strings

	// reserve places for sizes of filenames
	dataLength := make([]byte, 8)
	indexLength := make([]byte, 8)
	summaryLength := make([]byte, 8)
	filterLength := make([]byte, 8)
	metadataLength := make([]byte, 8)

	// put sizes in created byte array
	binary.BigEndian.PutUint64(dataLength, uint64(len(dataFilename)))
	binary.BigEndian.PutUint64(indexLength, uint64(len(indexFilename)))
	binary.BigEndian.PutUint64(summaryLength, uint64(len(summaryFilename)))
	binary.BigEndian.PutUint64(filterLength, uint64(len(filterFilename)))
	binary.BigEndian.PutUint64(metadataLength, uint64(len(metadataFilename)))

	totalBytes += int64(len(dataFilename) + len(indexFilename) + len(summaryFilename) + len(filterFilename) + len(metadataFilename))

	// add all data together and reserve totalBytes so it can be written
	data := append(dataLength, []byte(dataFilename)...)
	data = append(data, indexLength...)
	data = append(data, []byte(indexFilename)...)
	data = append(data, summaryLength...)
	data = append(data, []byte(summaryFilename)...)
	data = append(data, filterLength...)
	data = append(data, []byte(filterFilename)...)
	data = append(data, metadataLength...)
	data = append(data, []byte(metadataFilename)...)

	return data
}
func ReadFromToc(tocfile string) ([]string, error) {
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

	filterSize := binary.BigEndian.Uint64(mmapFile[:8])
	filterFile := string(mmapFile[8 : 8+filterSize])
	mmapFile = mmapFile[8+summarySize:]
	// Read metadata section
	metadataFile := string(mmapFile[7:])

	return []string{dataFile, indexFile, summaryFile, filterFile, metadataFile}, nil
}

// write to data file
// params - filename in case of single file it will be the sstable file itself,
// in case of multifile sstable this will be data file
//   - data to be written
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

// we need to know where datarecord is placed to after looking for key in
// index file we have offset of data we are looking for and for the one that goes after it
// deserialization bytes between these to locations gives us the said record we are looking for
// param mmapFile - we do this instead passing the file or filename itself so we can use function for
// both multi and single sile sstable, we do this for all reads
func ReadDataFromFile(mmapFile mmap.MMap, offsetStart, offsetEnd uint64) (*DataRecord, error) {
	var serializedDataRecord []byte
	if offsetStart == offsetEnd {
		serializedDataRecord = mmapFile[offsetStart:]
	} else {
		serializedDataRecord = mmapFile[offsetStart:offsetEnd]
	}
	dataRecord, err := DeserializeDataRecord(serializedDataRecord)
	if err != nil {
		return nil, err
	}
	return dataRecord, nil
}

// it returns data offset
func ReadIndexFromFile(mmapFile mmap.MMap, offsetStart uint64) ([]*IndexRecord, error) {
	var result []*IndexRecord
	// 8 for size of key size
	// key size for key
	// 8 for offset
	// make sure that the next thing we read is indeed index (/key size of the key of index)
	mmapFile = mmapFile[offsetStart:]
	offset := uint64(0)
	// <= because later on when we read data file we need two offsets, so in case we are looking for the last one
	// its end is the start of new summary index
	// ex. look for 4, we have 0 1 2 3 4 5 6 7 ... written
	// return 0 1 2 3 4 5, if we dont return the 5 we dont have the end offset
	for i := 0; i <= SummaryConst; i++ {
		keySize := binary.BigEndian.Uint64(mmapFile[KeySizeStart:KeySizeSize])
		// this could be const as well, we know all offsets are uint64
		offset = keySize + KeySizeSize + 8
		indexRecord, err := DeserializeIndexRecord(mmapFile[:offset])
		if err != nil {
			return nil, errors.New("error deserializing index record")
		}
		result = append(result, indexRecord)
		mmapFile = mmapFile[offset:]
		if len(mmapFile) == 0 {
			result = append(result, indexRecord)
			return result, nil
		}
	}

	return result, nil
}

// same as index file just add summary min and max at the top

// it returns index offset
func ReadSummaryFromFile(mmapFile mmap.MMap, key string) (uint64, error) {
	mmapFileSize := uint64(len(mmapFile))
	// first, we get sizes of summary min and max
	summaryMinSize := binary.BigEndian.Uint64(mmapFile[SummaryMinSizestart:SummaryMaxSizeStart])
	summaryMaxSize := binary.BigEndian.Uint64(mmapFile[SummaryMaxSizeStart : SummaryMaxSizeStart+SummaryMaxSizeSize])

	// then read them and deserialize to get index records
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
	if key < summaryMin.Key || key > summaryMax.Key {
		return 0, errors.New("key not in range of summary index table")
	}
	var summaryRecords []*IndexRecord
	offset := uint64(0)
	for offset < mmapFileSize {
		keySize := binary.BigEndian.Uint64(mmapFile[KeySizeStart:KeySizeSize])

		// indexOffset := binary.BigEndian.Uint64(mmapFile[KeyStart+int(keySize):])
		offset = KeySizeSize + keySize + 8
		indexRecord, err := DeserializeIndexRecord(mmapFile[:offset])
		if err != nil {
			return 0, errors.New("error deserializing index record")
		}
		summaryRecords = append(summaryRecords, indexRecord)
		// return second to last if we found the place of the key
		if len(summaryRecords) >= 2 && summaryRecords[len(summaryRecords)-1].Key > key && summaryRecords[len(summaryRecords)-2].Key <= key {
			return summaryRecords[len(summaryRecords)-2].Offset, nil
		}
		mmapFile = mmapFile[offset:]
		// if we came to the end of the file return last one
		// because if it passed all the way to here and it didnt return nil when we checked if its in the range in keys
		// then it has to be somewhere near the end after the last summary index
		if len(mmapFile) == 0 {
			return indexRecord.Offset, nil
		}
	}
	return 0, errors.New("key not found")
}

// mmapFile in case of multi file sstable with be the hole file
// in the case of single file sstable it will only be part that is bloom filter
func ReadBloomFilterFromFile(key string, mmapFile mmap.MMap) (bool, error) {
	filter := bloomfilter.Deserialize(mmapFile)
	found, err := filter.ContainsElement([]byte(key))
	if err != nil {
		return false, err
	}
	return found, err
}
func ReadMerkle(mmapFile mmap.MMap) (*MerkleTree, error) {
	merkle, err := DeserializeMerkle(mmapFile)
	if err != nil {
		return nil, err
	}
	err = mmapFile.Unmap()
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return merkle, err
}

// get serialized data
func addToDataSegment(entry *MemEntry, result []byte) (uint64, []byte) {
	dataRecord := *NewDataRecord(entry)
	serializedRecord := dataRecord.SerializeDataRecord()
	return uint64(len(serializedRecord)), append(result, serializedRecord...)
}
func addToIndex(offset uint64, entry *MemEntry, result []byte) ([]byte, []byte) {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	return serializedIndexRecord, append(result, serializedIndexRecord...)
}

func Get(key string) (*models.Data, error) {
	var mmapFileData mmap.MMap
	var mmapFileFilter mmap.MMap
	var mmapFileSummary mmap.MMap
	var mmapFileIndex mmap.MMap
	var mmapFileMeta mmap.MMap
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
	index, err := strconv.ParseUint(lastSubDirName[11:], 10, 64)
	lastSubDirNameWithoutIndex := lastSubDirName[:10]
	if err != nil {
		return nil, err
	}
	for {
		subDirName := lastSubDirNameWithoutIndex + fmt.Sprintf("%05d", index)
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
		} else {
			// paths : sstable\\sstableN\\sst_00001_1_part.db
			dataFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
			filterFilePath := filepath.Join(subDirPath, subDirEntries[1].Name())
			indexFilePath := filepath.Join(subDirPath, subDirEntries[2].Name())
			metaFilePath := filepath.Join(subDirPath, subDirEntries[3].Name())
			summaryFilePath := filepath.Join(subDirPath, subDirEntries[4].Name())
			tocFilePath := filepath.Join(subDirPath, subDirEntries[5].Name())

			// in the next part we are doing the copying in case mmap acts unexpectedly after file is closed

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
			if err != nil {
				return nil, err
			}
			mmapFileIndexPart, err := mmap.Map(indexFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileIndex = make([]byte, len(mmapFileIndexPart))
			copy(mmapFileIndex, mmapFileIndexPart)

			summaryFile, err := os.OpenFile(summaryFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			mmapFileSummaryPart, err := mmap.Map(summaryFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileSummary = make([]byte, len(mmapFileSummaryPart))
			copy(mmapFileSummary, mmapFileSummaryPart)

			metaFile, err := os.OpenFile(metaFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			mmapFileMetaPart, err := mmap.Map(metaFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileMeta = make([]byte, len(mmapFileMetaPart))
			copy(mmapFileMeta, mmapFileMetaPart)

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
		if err != nil {
			return nil, err
		}
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
			if indexRecords[i].Key == key {
				dataRecord, err := ReadDataFromFile(mmapFileData, indexRecords[i].Offset, indexRecords[i+1].Offset)
				if err != nil {
					return nil, err
				}
				// this could be changed since it is the same
				// dataRecord to models.Data
				return &models.Data{Value: dataRecord.Value, Tombstone: dataRecord.Tombstone, Timestamp: dataRecord.Timestamp}, nil
			}
		}
		break

	}
	return nil, nil
}

// file name will be toc file for multi file sstable
// and file for single file sstable
func createFiles(memEntries []*MemEntry, singleFile bool, files ...*os.File) error {
	var data []byte
	dataSize := uint64(0)
	var dataRecords []byte
	var indexRecords []byte
	var summaryRecords []byte
	var tocData []byte
	summaryHeader := make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize)

	// for single file, container for summary records - we need it bc we dont know the size of data segment wich affects offsets
	singleSummaryRecords := make([]*IndexRecord, 0)
	var summaryMin []byte
	var summaryMax []byte
	var serializedIndexRecord []byte
	var serializedSummaryRecord []byte
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
	}

	// counter for index summary, for every n index records add one to summary
	countKeysBetween := 0
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.001)
	merkle, err := CreateMerkleTree(memEntries)
	if err != nil {
		return err
	}
	// proccess of adding entries
	for _, entry := range memEntries {
		sizeOfDR, dataRecords = addToDataSegment(entry, dataRecords)
		serializedIndexRecord, indexRecords = addToIndex(offset, entry, indexRecords)
		sizeOfIR = uint64(len(serializedIndexRecord))
		// keep track of size of the block for single file sstable
		dataBlockSize += sizeOfDR
		indexBlockSize += sizeOfIR
		if countKeysBetween%SummaryConst == 0 {
			if singleFile {
				singleSummaryRecords = append(singleSummaryRecords, NewIndexRecord(entry, indexOffset))
			} else {
				serializedSummaryRecord, summaryRecords = addToIndex(indexOffset, entry, summaryRecords)
				sizeOfSR = uint64(len(serializedSummaryRecord))
				summaryBlockSize += sizeOfSR
				if summaryMin == nil {
					summaryMin = serializedSummaryRecord
				}
			}
		}
		err = filter.AddElement([]byte(entry.Key))
		if err != nil {
			return err
		}
		summaryMax = serializedIndexRecord
		countKeysBetween++
		offset += sizeOfDR
		indexOffset += sizeOfIR
	}
	filterData := filter.Serialize()
	filterBlockSize = uint64(len(filterData))
	merkleData := merkle.Serialize()
	metaBlockSize += uint64(len(merkleData))

	if singleFile {
		for _, summRecord := range singleSummaryRecords {
			summRecord.Offset += dataBlockSize
			serializedSummaryRecord = summRecord.SerializeIndexRecord()
			if summaryMin == nil {
				summaryMin = serializedSummaryRecord
			}
			summaryRecords = append(summaryRecords, serializedIndexRecord...)
			summaryMax = serializedSummaryRecord
			sizeOfSR = uint64(len(serializedSummaryRecord))
			summaryBlockSize += sizeOfSR
		}

	}
	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizestart:SummaryMaxSizeStart], uint64(len(summaryMin)))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len(summaryMax)))
	summaryHeaderSize := SummaryMinSizeSize + SummaryMaxSizeSize + uint64(len(summaryMin)) + uint64(len(summaryMax))
	summaryHeader = append(summaryHeader, summaryMin...)
	summaryHeader = append(summaryHeader, summaryMax...)
	summaryHeader = append(summaryHeader, summaryRecords...)
	summaryBlockSize += uint64(summaryHeaderSize)

	if singleFile {
		header := make([]byte, HeaderSize)
		binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
		binary.BigEndian.PutUint64(header[FilterBlockStart:IndexBlockStart], filterBlockSize)
		binary.BigEndian.PutUint64(header[IndexBlockStart:SummaryBlockStart], indexBlockSize)
		binary.BigEndian.PutUint64(header[SummaryBlockStart:MetaBlockStart], summaryBlockSize)

		//size of all data that needs to be written to mmap
		dataSize = dataSize + dataBlockSize + filterBlockSize + indexBlockSize + summaryBlockSize + metaBlockSize + HeaderSize
		data = append(data, header...)
		data = append(data, dataRecords...)
		data = append(data, filterData...)
		data = append(data, indexRecords...)
		data = append(data, summaryHeader...)
		data = append(data, merkleData...)

		err = WriteToFile(files[0], data)
		if err != nil {
			return err
		}

		return nil
	}
	tocData = createTocData(files[0].Name(), files[1].Name(), files[2].Name(), files[3].Name(), files[4].Name())
	err = WriteToFile(files[0], dataRecords)
	if err != nil {
		return err
	}
	err = WriteToFile(files[1], indexRecords)
	if err != nil {
		return err
	}
	err = WriteToFile(files[2], summaryHeader)
	if err != nil {
		return err
	}
	err = WriteToFile(files[3], filterData)
	if err != nil {
		return err
	}
	err = WriteToFile(files[4], merkleData)
	if err != nil {
		return err
	}
	err = WriteToFile(files[5], tocData)
	if err != nil {
		return err
	}
	return nil
}
