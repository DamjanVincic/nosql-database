package lsm

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

type IndexRecord struct {
	keySize uint64
	Key     string
	Offset  uint64
}

func addToIndex(offset uint64, entry *models.Data, result *[]byte) []byte {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	*result = append(*result, serializedIndexRecord...)
	return serializedIndexRecord
}
func NewIndexRecord(memEntry *models.Data, offset uint64) *IndexRecord {
	return &IndexRecord{
		keySize: uint64(len([]byte(memEntry.Key))),
		Key:     memEntry.Key,
		Offset:  offset,
	}
}
func makeMultiFilenames(subdirPath string) *[]string {
	dataFilename := fmt.Sprintf("%s.db", DataFileName)
	indexFilename := fmt.Sprintf("%s.db", IndexFileName)
	summaryFilename := fmt.Sprintf("%s.db", SummaryFileName)
	filterFilename := fmt.Sprintf("%s.db", FilterFileName)
	metadataFilename := fmt.Sprintf("%s.db", MetaFileName)

	fileNames := []string{dataFilename, indexFilename, summaryFilename, filterFilename, metadataFilename}
	var filePaths []string
	for _, fileName := range fileNames {
		filePaths = append(filePaths, filepath.Join(subdirPath, fileName))
	}
	return &filePaths
}

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

func (indexRecord *IndexRecord) SerializeIndexRecord() []byte {

	// 8 bytes for Offset and Keysize
	bytes := make([]byte, KeySizeSize+indexRecord.keySize+OffsetSize)

	// Serialize KeySize (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes, indexRecord.keySize)

	// Serialize Key (variable size)
	copy(bytes[KeyStart:], []byte(indexRecord.Key))

	// Serialize Offset (8 bytes, BigEndian)
	binary.BigEndian.PutUint64(bytes[KeyStart+indexRecord.keySize:], indexRecord.Offset)

	return bytes
}

func DeserializeIndexRecord(bytes []byte) *IndexRecord {
	// Deserialize KeySize (8 bytes, BigEndian)
	keySize := binary.BigEndian.Uint64(bytes[KeySizeStart:KeySizeSize])

	// Deserialize Key (variable size)
	key := string(bytes[KeyStart : KeyStart+int(keySize)])

	// Deserialize Offset (8 bytes, BigEndian)
	offset := binary.BigEndian.Uint64(bytes[KeyStart+int(keySize):])

	return &IndexRecord{
		Key:     key,
		keySize: keySize,
		Offset:  offset,
	}
}

const (
	Multiplier  = 2
	LevelOneMax = 2
	MaxLevels   = 3
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

func CheckCompaction(sizedCompaction bool, lsmLevel uint8, folderEntries *[]string, singleFile bool, indexConst uint16, summaryConst uint16) {
	if sizedCompaction {
		if len(*folderEntries) > LevelOneMax {
			//radi kompakciju celog nivoa
			Compact(folderEntries, lsmLevel, singleFile, indexConst, summaryConst)
		}
	}
}

func Compact(sstables *[]string, lsmLevel uint8, singleFile bool, indexConst uint16, summaryConst uint16) error {
	var maps []*mmap.MMap
	for _, sstable := range *sstables {
		currentFile, err := os.OpenFile(sstable, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		m, err := mmap.Map(currentFile, mmap.RDWR, 0)
		if err != nil {
			return err
		}
		dataBlock := m[DataBlockStart:IndexBlockStart]
		maps = append(maps, &dataBlock)
	}
	return combineSSTables(maps, lsmLevel, singleFile, indexConst, summaryConst)
}

// sstables - list of serialized data records
func combineSSTables(sstables []*mmap.MMap, lsmLevel uint8, singleFile bool, indexConst uint16, summaryConst uint16) error {
	numberOfSSTables := len(sstables)
	// we keep where we left of in each sstable and last key
	offsets := make([]uint64, numberOfSSTables)
	current := make([]*models.DataRecord, numberOfSSTables)

	var currentFile *os.File
	var mmapCurrFile mmap.MMap
	indexData := make([]byte, IndexConstSize)
	// append index thinning const to indexRecords data
	binary.BigEndian.PutUint16(indexData[:IndexConstSize], indexConst)
	var groupedData = make([]*[]byte, 5)
	var summaryData []byte
	var filterData []byte
	var merkleData []byte
	var filterKeys []string
	groupedData[0] = &indexData
	groupedData[1] = &summaryData
	groupedData[2] = &filterData
	groupedData[3] = &merkleData
	// Storage for offsets in case of single file
	// Storage for offsets in case of single file
	var dataBlockSize uint64
	var indexBlockSize uint64
	var summaryBlockSize uint64
	var filterBlockSize uint64
	var metaBlockSize uint64

	var index uint64
	subDirName := fmt.Sprintf("%2d", lsmLevel+1)
	subDirPath := filepath.Join(Path, subDirName)
	subDirEntries, err := os.ReadDir(subDirPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(subDirPath, os.ModePerm)
		index = 1
		if err != nil {
			return err
		}
	}
	//read name from last added sstable in next lsm level and create new name
	tempIndex, err := strconv.Atoi(subDirEntries[len(subDirEntries)-1].Name()[3:])
	index = uint64(tempIndex + 1)
	sstDirName := fmt.Sprintf("%5d", index)
	sstDirPath := filepath.Join(subDirPath, sstDirName)
	err = os.Mkdir(sstDirPath, os.ModePerm)
	if err != nil {
		return err
	}
	var fileName string
	if singleFile {
		fileName = fmt.Sprintf("%s.db", SingleFileName)

	} else {
		fileName = fmt.Sprintf("%s.db", DataFileName)
	}
	//if its single - currentFile = all data
	//otherwise = currentFile = data file
	currentFile, err = os.OpenFile(filepath.Join(sstDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	mmapCurrFile, err = mmap.Map(currentFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	// currentFile.Name()
	//fmt.Print(mmapCurrFile)

	// read first record from each sstable
	for i := 0; i < numberOfSSTables; i++ {
		dataRecord, err := readDataFromFile(*sstables[i], 1, "", offsets[i])
		if err != nil {
			return err
		}
		if dataRecord != nil {
			// set offsets for next
			offsets[i] = uint64(len(dataRecord.Data.Key) + KeySizeSize + ValueSizeSize + len(dataRecord.Data.Value) + TombstoneSize + TimestampSize + CrcSize)
		}
		current[i] = dataRecord
	}
	// find the smallest key
	// we know that the index we return is the sstable we read next (or multiple)
	offset := uint64(0)
	var serializedMinRec []byte
	// needed for offsets and single file header
	var sizeOfDR uint64
	var sizeOfIR uint64
	var sizeOfSR uint64
	var indexOffset uint64 = IndexConstSize
	// counter for index and index summary, for every n index records add one to summary
	countRecords := uint16(0)
	countIndexRecords := uint16(0)
	var merkleHashedRecords []byte

	var summaryMin string
	var summaryMax string
	var serializedIndexRecord []byte
	var serializedSummaryRecord []byte

	var summaryHeader = make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize+SummaryConstSize)

	//create an new merkle tree with new hashfunc and without nodes
	merkleTree := merkle.NewMerkle(nil)
	var dataRecord *models.DataRecord
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
				dataRecord = models.NewDataRecord(minRecord)
			}
			nextRecord, err := readDataFromFile(*sstables[keyIndex], 1, "", offsets[keyIndex])
			if err != nil {
				return err
			}
			if nextRecord != nil {
				// set offsets for next
				offsets[keyIndex] += uint64(len(dataRecord.Data.Key) + CrcSize + KeySizeSize + ValueSizeSize + len(dataRecord.Data.Value) + TombstoneSize + TimestampSize)
			}
			current[keyIndex] = nextRecord
		}
		filterKeys = append(filterKeys, dataRecord.Data.Key)
		serializedMinRec = dataRecord.Serialize()
		sizeOfDR = uint64(len(serializedMinRec))
		dataBlockSize += sizeOfDR
		hashedData, err := merkleTree.CreateNodeData(dataRecord)
		if err != nil {
			return err
		}
		merkleHashedRecords = append(merkleHashedRecords, hashedData...)

		// every Nth one is saved in the index (key, offset of dataRec)
		if countRecords%indexConst == 0 {
			serializedIndexRecord = addToIndex(offset, dataRecord.Data, &indexData)
			sizeOfIR = uint64(len(serializedIndexRecord))
			indexBlockSize += sizeOfIR
			// every Nth one is saved in the summary index (key, offset of indexRec)
			if countIndexRecords%summaryConst == 0 {
				serializedSummaryRecord = addToIndex(indexOffset, dataRecord.Data, &summaryData)
				sizeOfSR = uint64(len(serializedSummaryRecord))
				summaryBlockSize += sizeOfSR
				if summaryMin == "" {
					summaryMin = dataRecord.Data.Key
				}
			}
			indexOffset += sizeOfIR
			countIndexRecords++
		}
		summaryMax = dataRecord.Data.Key
		offset += sizeOfDR
		countRecords++

	}
	//create an empty bloom filter
	filter := bloomfilter.CreateBloomFilter(len(filterData), 0.001)
	//add keys to bf
	for _, key := range filterKeys {
		err = filter.AddElement([]byte(key))
		if err != nil {
			return err
		}
	}
	filterData = filter.Serialize()
	filterBlockSize = uint64(len(filterData))

	//metadata
	err = merkleTree.CreateMerkleTree(merkleHashedRecords)
	if err != nil {
		return err
	}
	merkleData = merkleTree.Serialize()
	metaBlockSize = uint64(len(merkleData))
	//creating summary index header
	binary.BigEndian.PutUint16(summaryHeader[:SummaryMinSizeStart], summaryConst)
	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizeStart:SummaryMaxSizeStart], uint64(len([]byte(summaryMin))))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len([]byte(summaryMax))))
	summaryHeaderSize := SummaryConstSize + SummaryMinSizeSize + SummaryMaxSizeSize + uint64(len([]byte(summaryMin))) + uint64(len([]byte(summaryMax)))
	//append all summary index records
	summaryHeader = append(summaryHeader, []byte(summaryMin)...)
	summaryHeader = append(summaryHeader, []byte(summaryMax)...)
	summaryHeader = append(summaryHeader, summaryData...)
	summaryBlockSize += summaryHeaderSize

	if singleFile {
		header := make([]byte, HeaderSize)
		binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
		binary.BigEndian.PutUint64(header[IndexBlockStart:SummaryBlockStart], indexBlockSize)
		binary.BigEndian.PutUint64(header[SummaryBlockStart:FilterBlockStart], summaryBlockSize)
		binary.BigEndian.PutUint64(header[FilterBlockStart:MetaBlockStart], filterBlockSize)

		fileInfo, err := currentFile.Stat()
		if err != nil {
			return err
		}
		fileSize := fileInfo.Size()
		dataSize := int64(indexBlockSize + summaryBlockSize + filterBlockSize + metaBlockSize)
		if err = currentFile.Truncate(fileSize + dataSize); err != nil {
			return err
		}
		copy(mmapCurrFile, indexData)
		copy(mmapCurrFile, summaryData)
		copy(mmapCurrFile, filterData)
		copy(mmapCurrFile, merkleData)
	} else {
		/*
			for multi file implementation
			write data to dataFile, indexData to indexFile... each block in a separate file
		*/
		filePaths := makeMultiFilenames(subDirPath)
		for idx, fileName := range *filePaths {
			if idx == 0 {
				continue
			}
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

	currentFile.Close()
	mmapCurrFile.Unmap()
	return nil
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

func readDataFromFile(mmapFile mmap.MMap, indexThinningConst uint16, key string, offset uint64) (*models.DataRecord, error) {
	dataRecordSize := uint64(0)
	if offset >= uint64(len(mmapFile)) {
		return nil, errors.New("Key not found")
	}
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
		dataRecord, err := models.Deserialize(mmapFile[offset : offset+dataRecordSize])
		if err != nil {
			return nil, err
		}

		// keys must be equal
		if dataRecord.Data.Key == key || key == "" {
			return dataRecord, nil
		}
		offset += dataRecordSize
		// when you get to the end it means there is no match
		if len(mmapFile) == int(offset) {
			return nil, errors.New("Key not found")
		}
	}
	return nil, errors.New("Key not found")
}

func createNewLevelDir() (uint8, error) {
	lsmIndex := uint8(1)
	var subdirPath string
	var subdirName string
	// if there is no directory sstable, create one
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return 0, err
		}
	}
	maxTables := uint8(LevelOneMax)
	// lsm index is 1 at default, we change it if there is already existing levels
	if len(dirEntries) != 0 {
		subdirName = dirEntries[len(dirEntries)-1].Name()
		n, err := strconv.ParseUint(subdirName[10:], 10, 8)
		if err != nil {
			return 0, err
		}
		lsmIndex = uint8(n)
		if err != nil {
			return 0, err
		}
		maxTables = uint8(Multiplier*LevelOneMax) * lsmIndex
	}
	subdirName = fmt.Sprintf("lsm_level_%02d", lsmIndex)
	subdirPath = filepath.Join(Path, subdirName)
	subDirEntries, err := os.ReadDir(subdirPath)
	if err != nil {
		return 0, err
	}
	// if current level if full create new one
	if uint8(len(subDirEntries)) >= maxTables {
		lsmIndex++
		subdirName = fmt.Sprintf("lsm_level_%02d", lsmIndex)
		subdirPath = filepath.Join(Path, subdirName)
		err = os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return 0, err
		}
	}

	return lsmIndex, nil
}
func Write() {
	//
	createNewLevelDir()
}
