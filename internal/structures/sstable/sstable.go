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
	"github.com/DamjanVincic/key-value-engine/internal/structures/merkle"
	"github.com/edsrzf/mmap-go"
)

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

/*
same struct for SSTable single and multi-file implementation
  - indexConst and summaryConst are index and summary thinning constants
  - single file is bool value from configuration file which tells which implementation user has picked
    if singleFile==true then we create single SSTable file, otherwise multi
*/
type SSTable struct {
	indexConst   uint16
	summaryConst uint16
	singleFile   bool
}

func NewSSTable(indexSparseConst uint16, summarySparseConst uint16, singleFile bool) (*SSTable, error) {
	//check if sstable dir exists, if not create it
	// _ = dirEntries, now its like this bc we dont use it anywhere (Mijat)
	_, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	return &SSTable{
		indexConst:   indexSparseConst,
		summaryConst: summarySparseConst,
		singleFile:   singleFile,
	}, nil
}

// we know which SSTable to create based on the singleFile variable, set in the configuration file
func (sstable *SSTable) Write(memEntries []*models.Data) error {
	// Create the ssTable directory (with all ssTable files) if it doesn't exist
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return err
		}
	}
	var dataFilename string
	var indexFilename string
	var summaryFilename string
	var filterFilename string
	var metadataFilename string

	// temporarly the lsm index is set to 1
	lsmIndex := uint8(1)
	//index - sequence number of the sstable
	var index uint8
	/* sstable dir contains subdirs for each ssTable
	one subdir = one sstable
	same names for both implementations, no difference
	the difference is in the number of files in subdir
	for multi - 5, for single - 1 */
	var subdirPath string
	var subdirName string

	// If there are no subdirectoriums in the directory, create the first one
	if len(dirEntries) == 0 {
		// subdirName : 00_sstable_00000 (a two-digit num for the lsm index, five-digit num for the index)
		index = 1
		subdirName = fmt.Sprintf("%02d_sstable_%05d", lsmIndex, index)
		subdirPath = filepath.Join(Path, subdirName)
		err := os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return err
		}
	} else {
		subdirName = dirEntries[len(dirEntries)-1].Name()
		n, err := strconv.ParseUint(subdirName[11:], 10, 8)
		if err != nil {
			return err
		}
		index = uint8(n)
		if err != nil {
			return err
		}
		index++
		subdirName = fmt.Sprintf("%02d_sstable_%05d", lsmIndex, index)
		subdirPath = filepath.Join(Path, subdirName)
		err = os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	// creates files and save the data
	if sstable.singleFile {
		// create single file for ssTable
		// name - single.db
		filename := fmt.Sprintf("%s.db", SingleFileName)

		err = sstable.createFiles(memEntries, sstable.singleFile, []string{filepath.Join(subdirPath, filename)})
		if err != nil {
			return err
		}

		return nil
	} else {
		// Filename format: PART.db, part = sstable element
		// create names of new files
		dataFilename = fmt.Sprintf("%s.db", DataFileName)
		indexFilename = fmt.Sprintf("%s.db", IndexFileName)
		summaryFilename = fmt.Sprintf("%s.db", SummaryFileName)
		filterFilename = fmt.Sprintf("%s.db", FilterFileName)
		metadataFilename = fmt.Sprintf("%s.db", MetaFileName)

		fileNames := []string{dataFilename, indexFilename, summaryFilename, filterFilename, metadataFilename}
		var filePaths []string
		for _, fileName := range fileNames {
			filePaths = append(filePaths, filepath.Join(subdirPath, fileName))
		}

		//create files
		err = sstable.createFiles(memEntries, sstable.singleFile, filePaths)
		if err != nil {
			return err
		}

		return nil
	}
}

// we distinguish implementations by the singleFile value (and the num of params, 1 for single, 5 for multi)
func (sstable *SSTable) createFiles(memEntries []*models.Data, singleFile bool, filePaths []string) error {
	// Just a wrapper to store the pointers to a few byte arrays to make the code more readable
	var groupedData = make([]*[]byte, 5)

	// variables for storing serialized data
	var data []byte
	var dataRecords []byte
	groupedData[0] = &dataRecords
	var indexRecords = make([]byte, IndexConstSize)
	groupedData[1] = &indexRecords
	var summaryRecords []byte
	// append index thinning const to indexRecords data
	binary.BigEndian.PutUint16(indexRecords[:IndexConstSize], sstable.indexConst)
	// in summary header we have min and max index record (ranked by key)
	var summaryHeader = make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize+SummaryConstSize)
	groupedData[2] = &summaryHeader
	var summaryMin string
	var summaryMax string
	var serializedIndexRecord []byte
	var serializedSummaryRecord []byte
	// for single file header
	var dataBlockSize uint64
	var indexBlockSize uint64 = IndexConstSize
	var summaryBlockSize uint64
	var filterBlockSize uint64

	// needed for offsets and single file header
	var sizeOfDR uint64
	var sizeOfIR uint64
	var sizeOfSR uint64

	// same start offset for both implementations
	//if its single file implementation we will take blocks from the single file and consider them as multi files
	var offset uint64
	//for summary index
	var indexOffset uint64 = IndexConstSize

	// counter for index and index summary, for every n index records add one to summary
	var countRecords uint16
	var countIndexRecords uint16

	var merkleDataRecords []*models.DataRecord
	//create an empty bloom filter
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.001)
	// proccess of adding entries
	for _, entry := range memEntries {
		//every entry is saved in data segment

		dataRecord := *models.NewDataRecord(entry)
		merkleDataRecords = append(merkleDataRecords, &dataRecord)
		serializedRecord := dataRecord.Serialize(false)
		sizeOfDR = uint64(len(serializedRecord))
		dataRecords = append(dataRecords, serializedRecord...)

		dataBlockSize += sizeOfDR
		// every Nth one is saved in the index (key, offset of dataRec)
		if countRecords%sstable.indexConst == 0 {
			serializedIndexRecord = addToIndex(offset, entry, &indexRecords)
			sizeOfIR = uint64(len(serializedIndexRecord))
			indexBlockSize += sizeOfIR
			// every Nth one is saved in the summary index (key, offset of indexRec)
			if countIndexRecords%sstable.summaryConst == 0 {
				serializedSummaryRecord = addToIndex(indexOffset, entry, &summaryRecords)
				sizeOfSR = uint64(len(serializedSummaryRecord))
				summaryBlockSize += sizeOfSR
				if summaryMin == "" {
					summaryMin = entry.Key
				}
			}
			indexOffset += sizeOfIR
			countIndexRecords++
		}
		summaryMax = entry.Key
		//add key to bf
		err := filter.AddElement([]byte(entry.Key))
		if err != nil {
			return err
		}
		offset += sizeOfDR
		countRecords++
	}
	merkleTree, err := merkle.CreateMerkleTree(merkleDataRecords, nil)
	if err != nil {
		return err
	}
	//serialize filter and data
	filterData := filter.Serialize()
	groupedData[3] = &filterData
	filterBlockSize = uint64(len(filterData))
	merkleData := merkleTree.Serialize()
	groupedData[4] = &merkleData

	//creating summary index header
	binary.BigEndian.PutUint16(summaryHeader[:SummaryMinSizeStart], sstable.summaryConst)
	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizeStart:SummaryMaxSizeStart], uint64(len([]byte(summaryMin))))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len([]byte(summaryMax))))
	summaryHeaderSize := SummaryConstSize + SummaryMinSizeSize + SummaryMaxSizeSize + uint64(len([]byte(summaryMin))) + uint64(len([]byte(summaryMax)))
	//append all summary index records
	summaryHeader = append(summaryHeader, []byte(summaryMin)...)
	summaryHeader = append(summaryHeader, []byte(summaryMax)...)
	summaryHeader = append(summaryHeader, summaryRecords...)
	summaryBlockSize += summaryHeaderSize

	/*
		for single file implementation
		blocks : header - sizes of data, index, summary index and filter blocks
			     data, index, summary index, filter, metaData
	*/
	if singleFile {
		header := make([]byte, HeaderSize)
		binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
		binary.BigEndian.PutUint64(header[IndexBlockStart:SummaryBlockStart], indexBlockSize)
		binary.BigEndian.PutUint64(header[SummaryBlockStart:FilterBlockStart], summaryBlockSize)
		binary.BigEndian.PutUint64(header[FilterBlockStart:MetaBlockStart], filterBlockSize)

		data = append(data, header...)
		for _, fileData := range groupedData {
			data = append(data, *fileData...)
		}

		file, err := os.OpenFile(filePaths[0], os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		err = writeToFile(file, data)
		if err != nil {
			return err
		}

		err = file.Close()
		if err != nil {
			return err

		}
	} else {
		/*
			for multi file implementation
			write data to dataFile, indexData to indexFile... each block in a separate file
		*/
		for idx, fileName := range filePaths {
			file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				return err
			}

			err = writeToFile(file, *groupedData[idx])
			if err != nil {
				return err
			}

			err = file.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func addToIndex(offset uint64, entry *models.Data, result *[]byte) []byte {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	*result = append(*result, serializedIndexRecord...)
	return serializedIndexRecord
}

// write data to file using mmap
func writeToFile(file *os.File, data []byte) error {
	// make sure it has enough space
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

// go through every sstable until you find the key, when its found, return its value
func (sstable *SSTable) Get(key string) (*models.Data, error) {
	var data mmap.MMap
	var filter mmap.MMap
	var index mmap.MMap
	//var meta mmap.MMap
	var summary mmap.MMap

	// Storage for offsets in case of single file
	var dataStart uint64
	var indexStart uint64
	var summaryStart uint64
	var filterStart uint64
	var metaStart uint64

	var currentFile *os.File
	var mmapSingleFile mmap.MMap

	// if there is no dir to read from return nil
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		return nil, err
	}

	// search for the key from the newest to the oldest sstable
	// i variable will be increment each time as long as its not equal to the len of dirEntries
	dirSize := len(dirEntries) - 1
	i := dirSize
	for {
		if i < 0 {
			return nil, errors.New("key not found")
		}
		subDirName := dirEntries[i].Name()
		subDirPath := filepath.Join(Path, subDirName)
		subDirEntries, err := os.ReadDir(subDirPath)
		if os.IsNotExist(err) {
			return nil, err
		}
		subDirSize := len(subDirEntries)

		// search the single file if len == 1, otherwise multi files
		if subDirSize == 1 {
			// get the data from single file sstable
			singleFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
			currentFile, err = os.OpenFile(singleFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}

			mmapSingleFile, err = mmap.Map(currentFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}

			//get sizes of each part of SSTable single file
			header := mmapSingleFile[:HeaderSize]

			dataSize := binary.BigEndian.Uint64(header[:IndexBlockStart])                      // dataSize
			indexSize := binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart])    // indexSize
			summarySize := binary.BigEndian.Uint64(header[SummaryBlockStart:FilterBlockStart]) // summarySize
			filterSize := binary.BigEndian.Uint64(header[FilterBlockStart:])                   // filterSize

			dataStart = uint64(HeaderSize)
			indexStart = dataStart + dataSize
			summaryStart = indexStart + indexSize
			filterStart = summaryStart + summarySize
			metaStart = filterStart + filterSize
		}

		// start process for getting the element
		//if its not singleFile we need to read multifiles one by one
		// first we need to check if its in bloom filter
		if subDirSize == 1 {
			filter = mmapSingleFile[filterStart:metaStart]
		} else {
			currentFile, err = os.OpenFile(filepath.Join(subDirPath, subDirEntries[1].Name()), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			filter, err = mmap.Map(currentFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
		}
		found, err := readBloomFilterFromFile(key, filter)
		if err != nil {
			return nil, err
		}

		// Unmap the memory and close the file
		if subDirSize != 1 {
			err = filter.Unmap()
			if err != nil {
				return nil, err
			}
			err = currentFile.Close()
			if err != nil {
				return nil, err
			}
		}

		// if its not found, check next sstable
		if !found {
			i--
			if subDirSize == 1 {
				err = mmapSingleFile.Unmap()
				if err != nil {
					return nil, err
				}
				err = currentFile.Close()
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		//check if its in summary range (between min and max index)
		if subDirSize == 1 {
			summary = mmapSingleFile[summaryStart:filterStart]
		} else {
			currentFile, err = os.OpenFile(filepath.Join(subDirPath, subDirEntries[4].Name()), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			summary, err = mmap.Map(currentFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
		}

		indexOffset, summaryThinningConst, err := readSummaryFromFile(summary, key)

		if subDirSize != 1 {
			err = summary.Unmap()
			if err != nil {
				return nil, err
			}
			err = currentFile.Close()
			if err != nil {
				return nil, err
			}
		}

		if err != nil {
			i--
			if subDirSize == 1 {
				err = mmapSingleFile.Unmap()
				if err != nil {
					return nil, err
				}
				err = currentFile.Close()
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		//check if its in index
		if subDirSize == 1 {
			index = mmapSingleFile[indexStart:summaryStart]
		} else {
			currentFile, err = os.OpenFile(filepath.Join(subDirPath, subDirEntries[2].Name()), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			index, err = mmap.Map(currentFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
		}

		dataOffset, indexThinningConst := readIndexFromFile(index, summaryThinningConst, key, indexOffset)

		if subDirSize != 1 {
			err = index.Unmap()
			if err != nil {
				return nil, err
			}
			err = currentFile.Close()
			if err != nil {
				return nil, err
			}
		}

		//find it in data
		if subDirSize == 1 {
			data = mmapSingleFile[dataStart:indexStart]
		} else {
			currentFile, err = os.OpenFile(filepath.Join(subDirPath, subDirEntries[0].Name()), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			data, err = mmap.Map(currentFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
		}
		dataRecord, err := readDataFromFile(data, indexThinningConst, key, dataOffset)
		if err != nil {
			return nil, err
		}

		if subDirSize != 1 {
			err = data.Unmap()
			if err != nil {
				return nil, err
			}
			err = currentFile.Close()
			if err != nil {
				return nil, err
			}
		}

		if dataRecord != nil {
			return dataRecord.Data, nil
		}
		i--

		if subDirSize == 1 {
			err = mmapSingleFile.Unmap()
			if err != nil {
				return nil, err
			}
			err = currentFile.Close()
			if err != nil {
				return nil, err
			}
		}
	}
}

// mmapFile in case of multi file sstable will be the hole file
// in the case of single file sstable it will only be part that is bloom filter
func readBloomFilterFromFile(key string, mmapFile mmap.MMap) (bool, error) {
	filter := bloomfilter.Deserialize(mmapFile)
	found, err := filter.ContainsElement([]byte(key))
	if err != nil {
		return false, err
	}
	return found, err
}

// check if key is in summary range, if it is return index record offset, if it is not return 0
func readSummaryFromFile(mmapFile mmap.MMap, key string) (uint64, uint16, error) {

	// first, we get sizes of summary min and max and summary thinning const
	summaryConst := binary.BigEndian.Uint16(mmapFile[SummaryConstStart:SummaryMinSizeStart])

	summaryMinSize := binary.BigEndian.Uint64(mmapFile[SummaryMinSizeStart:SummaryMaxSizeStart])
	summaryMaxSize := binary.BigEndian.Uint64(mmapFile[SummaryMaxSizeStart : SummaryMaxSizeStart+SummaryMaxSizeSize])

	// then read them and deserialize to get index records
	keysStart := uint64(SummaryMaxSizeStart + SummaryMaxSizeSize)
	serializedSummaryMin := mmapFile[keysStart : keysStart+summaryMinSize]
	serializedSummaryMax := mmapFile[keysStart+summaryMinSize : keysStart+summaryMinSize+summaryMaxSize]

	summaryMin := string(serializedSummaryMin)
	summaryMax := string(serializedSummaryMax)

	// check if key is in range of summary indexes
	if key < summaryMin || key > summaryMax {
		return 0, 0, errors.New("key not in range of summary index table")
	}

	// mmapFile = only summary records
	mmapFile = mmapFile[keysStart+summaryMinSize+summaryMaxSize:]
	var summaryRecords []*IndexRecord
	var offset uint64
	for {
		// each summary record has keySize, key and offset
		keySize := binary.BigEndian.Uint64(mmapFile[offset+KeySizeStart : offset+KeySizeSize])
		summaryRecordSize := KeySizeSize + keySize + OffsetSize
		summaryRecord := DeserializeIndexRecord(mmapFile[offset : offset+summaryRecordSize])
		summaryRecords = append(summaryRecords, summaryRecord)

		// return second to last if we found the place of the key
		if len(summaryRecords) >= 2 {
			// no need for the part after && ?
			if summaryRecords[len(summaryRecords)-1].Key > key && summaryRecords[len(summaryRecords)-2].Key <= key {
				return summaryRecords[len(summaryRecords)-2].Offset, summaryConst, nil
			}
			summaryRecords = summaryRecords[1:]
		}
		offset += summaryRecordSize
		// if we came to the end of the file return last one
		// because if it passed all the way to here and it didnt return nil when we checked if its in the range in keys
		// then it has to be somewhere near the end after the last summary index
		if uint64(len(mmapFile)) == offset {
			return summaryRecord.Offset, summaryConst, nil
		}

	}
}

// check if key is in index range, if it is return data record offset, if it is not return 0
// we start reading summaryThinningConst number of index records from offset in index file
func readIndexFromFile(mmapFile mmap.MMap, summaryConst uint16, key string, offset uint64) (uint64, uint16) {
	var indexRecords []*IndexRecord
	// make sure that the next thing we read is indeed index (/key size of the key of index)
	indexThinningConst := binary.BigEndian.Uint16(mmapFile[:IndexConstSize])
	//bytesSize := uint64(len(mmapFile))
	indexRecordSize := uint64(0)
	//read SummaryConst number of index records
	for i := uint16(0); i < summaryConst; i++ {
		keySize := binary.BigEndian.Uint64(mmapFile[offset+KeySizeStart : offset+KeySizeSize])
		// this could be const as well, we know all offsets are uint64
		indexRecordSize = keySize + KeySizeSize + OffsetSize
		indexRecord := DeserializeIndexRecord(mmapFile[offset : offset+indexRecordSize])
		indexRecords = append(indexRecords, indexRecord)

		// when you find the record which key is bigger, the result is the previous record which key is smaller
		if len(indexRecords) >= 2 {
			// part after the && can be removed? since it's sorted
			if indexRecords[len(indexRecords)-1].Key > key && indexRecords[len(indexRecords)-2].Key <= key {
				return indexRecords[len(indexRecords)-2].Offset, indexThinningConst
			}
			indexRecords = indexRecords[1:]
		}
		offset += indexRecordSize
		// when you get to the end it means the result is the last one read
		if uint64(len(mmapFile)) == offset {
			return indexRecord.Offset, indexThinningConst
		}
	}
	//read all SummaryConst rec it means the result is the last one read
	return indexRecords[len(indexRecords)-1].Offset, indexThinningConst
}

// we need to know where datarecord is placed to after looking for key in
// index file we have offset of data we are looking for and for the one that goes after it
// deserialization bytes between these to locations gives us the said record we are looking for
// param mmapFile - we do this instead passing the file or filename itself so we can use function for
// both multi and single sile sstable, we do this for all reads
func readDataFromFile(mmapFile mmap.MMap, indexThinningConst uint16, key string, offset uint64) (*models.DataRecord, error) {
	dataRecordSize := uint64(0)
	//read IndexConst number of data records
	for i := uint16(0); i < indexThinningConst; i++ {
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
		dataRecord, err := models.Deserialize(mmapFile[offset:offset+dataRecordSize], false)
		if err != nil {
			return nil, err
		}

		// keys must be equal
		if dataRecord.Data.Key == key {
			return dataRecord, nil
		}
		offset += dataRecordSize
		// when you get to the end it means there is no match
		if len(mmapFile) == int(offset) {
			return nil, nil
		}
	}
	return nil, nil
}

func getAllMemEntries(mmapFile mmap.MMap) ([]*models.DataRecord, error) {
	var entries []*models.DataRecord
	dataRecordSize := uint64(0)
	offset := uint64(0)
	//read IndexConst number of data records
	for len(mmapFile) != int(offset) {
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
		dataRecord, _ := models.Deserialize(mmapFile[offset:offset+dataRecordSize], false)
		// ignore for merkle
		//if err != nil {
		//	return nil, err
		//}

		entries = append(entries, dataRecord)
		offset += dataRecordSize
		// when you get to the end it means there is no match
	}
	return entries, nil
}

func readMetaFromFile(mmapFile mmap.MMap) *merkle.MerkleTree {
	return merkle.DeserializeMerkle(mmapFile)
}

// Frdr testing rikvajrd
// subDirName - 01_sstable_00001
func (sstable *SSTable) CheckDataValidity(subDirName string) ([]*models.Data, error) {
	var corruptedData []*models.Data
	var metaMMap mmap.MMap
	var dataMMap mmap.MMap

	var entries []*models.DataRecord
	var merkleTree *merkle.MerkleTree
	var merkleTree2 *merkle.MerkleTree

	subDirPath := filepath.Join(Path, subDirName)
	subDirEntries, err := os.ReadDir(subDirPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	subDirSize := len(subDirEntries)

	// search the single file if len == 1, otherwise multi files
	if subDirSize == 1 {
		// get the data from single file sstable
		singleFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
		currentFile, err := os.OpenFile(singleFilePath, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		currentFileMMap, err := mmap.Map(currentFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}

		//get sizes of each part of SSTable single file
		header := currentFileMMap[:HeaderSize]

		dataSize := binary.BigEndian.Uint64(header[:IndexBlockStart])                      // dataSize
		indexSize := binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart])    // indexSize
		summarySize := binary.BigEndian.Uint64(header[SummaryBlockStart:FilterBlockStart]) // summarySize
		filterSize := binary.BigEndian.Uint64(header[FilterBlockStart:])                   // filterSize

		dataStart := uint64(HeaderSize)
		indexStart := dataStart + dataSize
		summaryStart := indexStart + indexSize
		filterStart := summaryStart + summarySize
		metaStart := filterStart + filterSize

		dataMMap = currentFileMMap[dataStart:indexStart]
		metaMMap = currentFileMMap[metaStart:]

		entries, err = getAllMemEntries(dataMMap)
		if err != nil {
			return nil, err
		}

		merkleTree = merkle.DeserializeMerkle(metaMMap)
		merkleTree2, err = merkle.CreateMerkleTree(entries, merkleTree.HashWithSeed)
		if err != nil {
			return nil, err
		}

		err = currentFileMMap.Unmap()
		if err != nil {
			return nil, err
		}

		err = currentFile.Close()
		if err != nil {
			return nil, err
		}
	} else {
		currentFile, err := os.OpenFile(filepath.Join(subDirPath, subDirEntries[0].Name()), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		dataMMap, err = mmap.Map(currentFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}

		entries, err = getAllMemEntries(dataMMap)
		if err != nil {
			return nil, err
		}

		err = dataMMap.Unmap()
		if err != nil {
			return nil, err
		}
		err = currentFile.Close()
		if err != nil {
			return nil, err
		}

		currentFile, err = os.OpenFile(filepath.Join(subDirPath, subDirEntries[3].Name()), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		metaMMap, err = mmap.Map(currentFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}

		merkleTree = merkle.DeserializeMerkle(metaMMap)
		merkleTree2, err = merkle.CreateMerkleTree(entries, merkleTree.HashWithSeed)
		if err != nil {
			return nil, err
		}
	}

	corruptedIndexes, err := merkleTree.CompareTrees(merkleTree2)
	for _, index := range corruptedIndexes {
		corruptedData = append(corruptedData, entries[index].Data)
	}

	return corruptedData, nil
}

//func compareMerkleTrees(bytes, mmapFileData []byte) ([]uint64, error) {
//	merkleTree := merkle.DeserializeMerkle(bytes)
//
//	entries, err := getAllMemEntries(mmapFileData)
//	if err != nil {
//		return nil, err
//	}
//	newMerkle, err := merkle.CreateMerkleTree(entries, merkleTree.HashWithSeed)
//	if err != nil {
//		return nil, err
//	}
//
//	return merkleTree.CompareTrees(newMerkle)
//}
