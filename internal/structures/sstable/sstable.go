package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"

	// "math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/DamjanVincic/key-value-engine/internal/structures/keyencoder"

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

	//max length of compressed index record
	CompressedIndexRecordMaxSize = binary.MaxVarintLen64 + binary.MaxVarintLen64

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
	indexConst        uint16
	summaryConst      uint16
	singleFile        bool
	compression       bool
	encoder           *keyencoder.KeyEncoder
	leveledCompaction bool
	firstLevelMax     uint16
	levelMultiplier   uint64
}

func NewSSTable(indexSparseConst uint16, summarySparseConst uint16, singleFile bool, compression bool, leveledCompaction bool, firstLevelMax uint16, levelMultiplier uint64) (*SSTable, error) {
	//check if sstable dir exists, if not create it
	_, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		err := os.Mkdir(Path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	var encoder *keyencoder.KeyEncoder
	if compression {
		encoder, err = keyencoder.ReadFromFile()
		if err != nil {
			return nil, err
		}
	}

	return &SSTable{
		indexConst:        indexSparseConst,
		summaryConst:      summarySparseConst,
		singleFile:        singleFile,
		compression:       compression,
		encoder:           encoder,
		leveledCompaction: leveledCompaction,
		firstLevelMax:     firstLevelMax,
		levelMultiplier:   levelMultiplier,
	}, nil
}

// we know which SSTable to create based on the singleFile variable, set in the configuration file
/* sstable dir contains subdirs for each lsm level
each subdir contains sstables folders
one folder = one sstable
same names for both implementations, no difference
the difference is in the number of files in sstable folder
for multi - 5, for single - 1
path example sstable/01/sst0000200002/data.db   -  data part of the sstable(sst0000200002) placed on the 1st lsm level
*/
func (sstable *SSTable) Write(memEntries []*models.Data) error {
	// new sstable is always placed on the first lsm level
	lsmLevel := uint8(1)

	filePaths, err := sstable.createSStableFolder(lsmLevel)
	if err != nil {
		return err
	}
	//create files
	err = sstable.createFiles(memEntries, sstable.singleFile, filePaths)
	if err != nil {
		return err
	}

	if sstable.compression {
		err = sstable.encoder.WriteToFile()
		if err != nil {
			return err
		}
	}
	// err = sstable.CheckCompaction(filePaths, lsmLevel)
	// if err != nil {
	// 	return err
	// }
	return nil
}

func (sstable *SSTable) createSStableFolder(lsmLevel uint8) ([]string, error) {
	//index - sequence number of the sstable
	var index uint16
	// subdirName : 00 (a two-digit num for the lsm index)
	subdirName := fmt.Sprintf("%02d", lsmLevel)
	subdirPath := filepath.Join(Path, subdirName)
	sstableFolders, err := os.ReadDir(subdirPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(subdirPath, os.ModePerm)
		// if there is no sstable on the lsm level, index for the new one is 1
		index = 1
		if err != nil {
			return nil, err
		}
	}
	// If there are sstables on the lsm level, take index from last added and increment it
	if len(sstableFolders) != 0 {
		lastAddedSstableFolderName := sstableFolders[len(sstableFolders)-1].Name()
		n, err := strconv.ParseUint(lastAddedSstableFolderName[3:], 10, 16)
		if err != nil {
			return nil, err
		}
		index = uint16(n)
		if err != nil {
			return nil, err
		}
		index++
	}

	sstFolderName := fmt.Sprintf("sst%010d", index)
	sstFolderPath := filepath.Join(subdirPath, sstFolderName)
	err = os.Mkdir(sstFolderPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	var filePaths []string
	// creates files and save the data
	if sstable.singleFile {
		// create single file for ssTable
		// name - single.db
		filename := fmt.Sprintf("%s.db", SingleFileName)
		filePaths = append(filePaths, filepath.Join(sstFolderPath, filename))
	} else {
		// Filename format: PART.db, part = sstable element
		// create names of new files
		filePaths = makeMultiFilenames(sstFolderPath)
	}
	return filePaths, nil
}

func makeMultiFilenames(subdirPath string) []string {
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
	return filePaths
}

// we distinguish implementations by the singleFile value (and the num of params, 1 for single, 5 for multi)
func (sstable *SSTable) createFiles(memEntries []*models.Data, singleFile bool, filePaths []string) error {
	// Just a wrapper to store the pointers to a few byte arrays to make the code more readable
	var groupedData = make([]*[]byte, 5)

	// variables for storing serialized data
	var data []byte
	var dataRecords []byte
	groupedData[0] = &dataRecords
	indexRecords := initializeIndexRecords(sstable.indexConst, sstable.compression)
	groupedData[1] = &indexRecords
	var summaryRecords []byte
	//// in summary header we have min and max index record (ranked by key)
	var summaryMin string
	var summaryMax string
	var serializedIndexRecord []byte
	var serializedSummaryRecord []byte
	// for single file header
	var dataBlockSize uint64
	indexBlockSize := uint64(len(*groupedData[1]))
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
	indexOffset := indexBlockSize

	// counter for index and index summary, for every n index records add one to summary
	var countRecords uint16
	var countIndexRecords uint16

	var merkleHashedRecords []byte
	//create an empty bloom filter
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.00001)
	//create an new merkle tree with new hashfunc and without nodes
	merkleTree := merkle.NewMerkle(nil)
	// process of adding entries
	for _, dataRecord := range memEntries {
		//every entry is saved in data segment

		hashedData, err := merkleTree.CreateNodeData(dataRecord, sstable.compression, sstable.encoder)
		if err != nil {
			return err
		}
		merkleHashedRecords = append(merkleHashedRecords, hashedData...)
		serializedRecord := dataRecord.Serialize(sstable.compression, sstable.encoder)
		sizeOfDR = uint64(len(serializedRecord))
		dataRecords = append(dataRecords, serializedRecord...)

		dataBlockSize += sizeOfDR
		// every Nth one is saved in the index (key, offset of dataRec)
		if countRecords%sstable.indexConst == 0 {
			serializedIndexRecord = addToIndex(offset, dataRecord, &indexRecords, sstable.compression, sstable.encoder)
			sizeOfIR = uint64(len(serializedIndexRecord))
			indexBlockSize += sizeOfIR
			// every Nth one is saved in the summary index (key, offset of indexRec)
			if countIndexRecords%sstable.summaryConst == 0 {
				serializedSummaryRecord = addToIndex(indexOffset, dataRecord, &summaryRecords, sstable.compression, sstable.encoder)
				sizeOfSR = uint64(len(serializedSummaryRecord))
				summaryBlockSize += sizeOfSR
				if summaryMin == "" {
					summaryMin = dataRecord.Key
				}
			}
			indexOffset += sizeOfIR
			countIndexRecords++
		}
		summaryMax = dataRecord.Key
		//add key to bf
		err = filter.AddElement([]byte(dataRecord.Key))
		if err != nil {
			return err
		}
		offset += sizeOfDR
		countRecords++
	}
	err := merkleTree.CreateMerkleTree(merkleHashedRecords, sstable.compression, sstable.encoder)
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
	summaryHeader := createSummaryHeader(sstable.summaryConst, summaryMin, summaryMax, sstable.compression, sstable.encoder)
	summaryBlockSize += uint64(len(summaryHeader))
	summaryHeader = append(summaryHeader, summaryRecords...)
	groupedData[2] = &summaryHeader

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

// serializes index const and returns resulting bytes
func initializeIndexRecords(indexConst uint16, compression bool) []byte {
	var indexRecords []byte
	if compression {
		tempBytes := make([]byte, binary.MaxVarintLen16)
		bytesWritten := binary.PutUvarint(tempBytes, uint64(indexConst))
		indexRecords = make([]byte, bytesWritten)
		copy(indexRecords, tempBytes[:bytesWritten])
	} else {
		indexRecords = make([]byte, IndexConstSize)
		// append index thinning const to indexRecords data
		binary.BigEndian.PutUint16(indexRecords[:IndexConstSize], indexConst)
	}
	return indexRecords
}

func createSummaryHeader(summaryConst uint16, summaryMin string, summaryMax string, compression bool, encoder *keyencoder.KeyEncoder) []byte {
	var summaryHeader []byte
	if compression {
		//temporary storage for 16 and 64-bit integers and used bytes in them
		summaryConstBytes := make([]byte, binary.MaxVarintLen16)
		var summaryConstBytesSize int
		summaryMinBytes := make([]byte, binary.MaxVarintLen64)
		var summaryMinBytesSize int
		summaryMaxBytes := make([]byte, binary.MaxVarintLen64)
		var summaryMaxBytesSize int

		//get uint64 encoded value od min and max keys
		encodedMin := encoder.GetEncoded(summaryMin)
		encodedMax := encoder.GetEncoded(summaryMax)

		//serialize all values and get number of bytes used
		summaryConstBytesSize = binary.PutUvarint(summaryConstBytes, uint64(summaryConst))
		summaryMinBytesSize = binary.PutUvarint(summaryMinBytes, encodedMin)
		summaryMaxBytesSize = binary.PutUvarint(summaryMaxBytes, encodedMax)

		//make bytes and append serialized values
		summaryHeader = make([]byte, summaryConstBytesSize+summaryMinBytesSize+summaryMaxBytesSize)
		bytesWritten := 0
		copy(summaryHeader[bytesWritten:bytesWritten+summaryConstBytesSize], summaryConstBytes[:summaryConstBytesSize])
		bytesWritten += summaryConstBytesSize
		copy(summaryHeader[bytesWritten:bytesWritten+summaryMinBytesSize], summaryMinBytes[:summaryMinBytesSize])
		bytesWritten += summaryMinBytesSize
		copy(summaryHeader[bytesWritten:bytesWritten+summaryMaxBytesSize], summaryMaxBytes[:summaryMaxBytesSize])

	} else {
		summaryHeader = make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize+SummaryConstSize)
		binary.BigEndian.PutUint16(summaryHeader[:SummaryMinSizeStart], summaryConst)
		binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizeStart:SummaryMaxSizeStart], uint64(len([]byte(summaryMin))))
		binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len([]byte(summaryMax))))
		summaryHeader = append(summaryHeader, []byte(summaryMin)...)
		summaryHeader = append(summaryHeader, []byte(summaryMax)...)
	}
	return summaryHeader
}

func addToIndex(offset uint64, entry *models.Data, result *[]byte, compression bool, encoder *keyencoder.KeyEncoder) []byte {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord(compression, encoder)
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

// func (sstable *SSTable) CheckCompaction(folderEntries []string, lsmLevel uint8) error {
// 	multiplier := math.Pow(float64(sstable.levelMultiplier), float64(lsmLevel-1))
// 	if uint16(len(folderEntries)) > (sstable.firstLevelMax * uint16(multiplier)) {
// 		//radi kompakciju celog nivoa
// 		if !sstable.leveledCompaction {
// 			err := sstable.sizeTieredCompact(folderEntries, lsmLevel)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	}
// 	return nil
// }

// func (sstable *SSTable) sizeTieredCompact(sstables []string, lsmLevel uint8) error {
// 	var sstForCompaction []mmap.MMap
// 	for _, sstable := range sstables {
// 		file, err := os.OpenFile(sstable, os.O_RDWR, 0644)
// 		if err != nil {
// 			return err
// 		}
// 		mmap, err := mmap.Map(file, mmap.RDWR, 0)
// 		if err != nil {
// 			return err
// 		}
// 		dataBlock := mmap[DataBlockStart:IndexBlockStart]
// 		sstForCompaction = append(sstForCompaction, dataBlock)
// 		err = mmap.Unmap()
// 		if err != nil {
// 			return err
// 		}
// 		err = file.Close()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err := sstable.combineSSTables(sstForCompaction, lsmLevel+1)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (sstable *SSTable) combineSSTables(sstForCompaction []mmap.MMap, nextLsmLevel uint8) error {
// 	lsmLevel := uint8(1)
// 	//index - sequence number of the sstable
// 	var index uint16
// 	// subdirName : 00 (a two-digit num for the lsm index)
// 	subdirName := fmt.Sprintf("%02d", nextLsmLevel)
// 	subdirPath := filepath.Join(Path, subdirName)
// 	sstableFolders, err := os.ReadDir(subdirPath)
// 	if os.IsNotExist(err) {
// 		err := os.Mkdir(subdirPath, os.ModePerm)
// 		// if there is no sstable on the lsm level, index for the new one is 1
// 		index = 1
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	numberOfSSTables := len(sstForCompaction)
// 	// we keep where we left of in each sstable and last key
// 	offsets := make([]uint64, numberOfSSTables)
// 	current := make([]*models.Data, numberOfSSTables)

// 	var currentFile *os.File
// 	var mmapCurrFile mmap.MMap
// 	indexData := initializeIndexRecords(sstable.indexConst, sstable.compression)
// 	// append index thinning const to indexRecords data
// 	binary.BigEndian.PutUint16(indexData[:IndexConstSize], sstable.indexConst)
// 	var groupedData = make([]*[]byte, 5)
// 	var summaryData []byte
// 	var filterData []byte
// 	var merkleData []byte
// 	var filterKeys []string
// 	groupedData[0] = &indexData
// 	groupedData[1] = &summaryData
// 	groupedData[2] = &filterData
// 	groupedData[3] = &merkleData
// 	// Storage for offsets in case of single file
// 	// Storage for offsets in case of single file
// 	var dataBlockSize uint64
// 	var indexBlockSize uint64
// 	var summaryBlockSize uint64
// 	var filterBlockSize uint64
// 	var metaBlockSize uint64

// 	// var index uint64
// 	subDirName := fmt.Sprintf("%2d", lsmLevel+1)
// 	subDirPath := filepath.Join(Path, subDirName)
// 	subDirEntries, err := os.ReadDir(subDirPath)
// 	if os.IsNotExist(err) {
// 		err := os.Mkdir(subDirPath, os.ModePerm)
// 		index = 1
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	//read name from last added sstable in next lsm level and create new name
// 	tempIndex, err := strconv.Atoi(subDirEntries[len(subDirEntries)-1].Name()[3:])
// 	index = uint64(tempIndex + 1)
// 	sstDirName := fmt.Sprintf("%5d", index)
// 	sstDirPath := filepath.Join(subDirPath, sstDirName)
// 	err = os.Mkdir(sstDirPath, os.ModePerm)
// 	if err != nil {
// 		return err
// 	}
// 	var fileName string
// 	if singleFile {
// 		fileName = fmt.Sprintf("%s.db", SingleFileName)
// 	} else {
// 		fileName = fmt.Sprintf("%s.db", DataFileName)
// 	}
// 	//if its single - currentFile = all data
// 	//otherwise = currentFile = data file
// 	currentFile, err = os.OpenFile(filepath.Join(sstDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
// 	if err != nil {
// 		return err
// 	}
// 	mmapCurrFile, err = mmap.Map(currentFile, mmap.RDWR, 0)
// 	if err != nil {
// 		return err
// 	}

// 	// currentFile.Name()
// 	//fmt.Print(mmapCurrFile)

// 	// read first record from each sstable
// 	for i := 0; i < numberOfSSTables; i++ {
// 		dataRecord, err := readDataFromFile(*sstables[i], 1, "", offsets[i], sstable.com)
// 		if err != nil {
// 			return err
// 		}
// 		if dataRecord != nil {
// 			// set offsets for next
// 			offsets[i] = uint64(len(dataRecord.Data.Key) + KeySizeSize + ValueSizeSize + len(dataRecord.Data.Value) + TombstoneSize + TimestampSize + CrcSize)
// 		}
// 		current[i] = dataRecord
// 	}
// 	// find the smallest key
// 	// we know that the index we return is the sstable we read next (or multiple)
// 	offset := uint64(0)
// 	var serializedMinRec []byte
// 	// needed for offsets and single file header
// 	var sizeOfDR uint64
// 	var sizeOfIR uint64
// 	var sizeOfSR uint64
// 	var indexOffset uint64 = IndexConstSize
// 	// counter for index and index summary, for every n index records add one to summary
// 	countRecords := uint16(0)
// 	countIndexRecords := uint16(0)
// 	var merkleHashedRecords []byte

// 	var summaryMin string
// 	var summaryMax string
// 	var serializedIndexRecord []byte
// 	var serializedSummaryRecord []byte

// 	var summaryHeader = make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize+SummaryConstSize)

// 	//create an new merkle tree with new hashfunc and without nodes
// 	merkleTree := merkle.NewMerkle(nil)
// 	var dataRecord *models.DataRecord
// 	for len(sstables) > 0 {
// 		smallestKey := findSmallestKey(current)
// 		if smallestKey == nil {
// 			break
// 		}
// 		var minRecord *models.Data
// 		for _, keyIndex := range smallestKey {
// 			if minRecord == nil {
// 				minRecord = current[keyIndex].Data
// 			}
// 			if minRecord.Timestamp < current[keyIndex].Data.Timestamp {
// 				minRecord = current[keyIndex].Data
// 				dataRecord = models.NewDataRecord(minRecord)
// 			}
// 			nextRecord, err := readDataFromFile(*sstables[keyIndex], 1, "", offsets[keyIndex])
// 			if err != nil {
// 				return err
// 			}
// 			if nextRecord != nil {
// 				// set offsets for next
// 				offsets[keyIndex] += uint64(len(dataRecord.Data.Key) + CrcSize + KeySizeSize + ValueSizeSize + len(dataRecord.Data.Value) + TombstoneSize + TimestampSize)
// 			}
// 			current[keyIndex] = nextRecord
// 		}
// 		filterKeys = append(filterKeys, dataRecord.Data.Key)
// 		serializedMinRec = dataRecord.Serialize()
// 		sizeOfDR = uint64(len(serializedMinRec))
// 		dataBlockSize += sizeOfDR
// 		hashedData, err := merkleTree.CreateNodeData(dataRecord)
// 		if err != nil {
// 			return err
// 		}
// 		merkleHashedRecords = append(merkleHashedRecords, hashedData...)

// 		// every Nth one is saved in the index (key, offset of dataRec)
// 		if countRecords%indexConst == 0 {
// 			serializedIndexRecord = addToIndex(offset, dataRecord.Data, &indexData)
// 			sizeOfIR = uint64(len(serializedIndexRecord))
// 			indexBlockSize += sizeOfIR
// 			// every Nth one is saved in the summary index (key, offset of indexRec)
// 			if countIndexRecords%summaryConst == 0 {
// 				serializedSummaryRecord = addToIndex(indexOffset, dataRecord.Data, &summaryData)
// 				sizeOfSR = uint64(len(serializedSummaryRecord))
// 				summaryBlockSize += sizeOfSR
// 				if summaryMin == "" {
// 					summaryMin = dataRecord.Data.Key
// 				}
// 			}
// 			indexOffset += sizeOfIR
// 			countIndexRecords++
// 		}
// 		summaryMax = dataRecord.Data.Key
// 		offset += sizeOfDR
// 		countRecords++

// 	}
// 	//create an empty bloom filter
// 	filter := bloomfilter.CreateBloomFilter(len(filterData), 0.001)
// 	//add keys to bf
// 	for _, key := range filterKeys {
// 		err = filter.AddElement([]byte(key))
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	filterData = filter.Serialize()
// 	filterBlockSize = uint64(len(filterData))

// 	//metadata
// 	err = merkleTree.CreateMerkleTree(merkleHashedRecords)
// 	if err != nil {
// 		return err
// 	}
// 	merkleData = merkleTree.Serialize()
// 	metaBlockSize = uint64(len(merkleData))
// 	//creating summary index header
// 	binary.BigEndian.PutUint16(summaryHeader[:SummaryMinSizeStart], summaryConst)
// 	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizeStart:SummaryMaxSizeStart], uint64(len([]byte(summaryMin))))
// 	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len([]byte(summaryMax))))
// 	summaryHeaderSize := SummaryConstSize + SummaryMinSizeSize + SummaryMaxSizeSize + uint64(len([]byte(summaryMin))) + uint64(len([]byte(summaryMax)))
// 	//append all summary index records
// 	summaryHeader = append(summaryHeader, []byte(summaryMin)...)
// 	summaryHeader = append(summaryHeader, []byte(summaryMax)...)
// 	summaryHeader = append(summaryHeader, summaryData...)
// 	summaryBlockSize += summaryHeaderSize

// 	if singleFile {
// 		header := make([]byte, HeaderSize)
// 		binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
// 		binary.BigEndian.PutUint64(header[IndexBlockStart:SummaryBlockStart], indexBlockSize)
// 		binary.BigEndian.PutUint64(header[SummaryBlockStart:FilterBlockStart], summaryBlockSize)
// 		binary.BigEndian.PutUint64(header[FilterBlockStart:MetaBlockStart], filterBlockSize)

// 		fileInfo, err := currentFile.Stat()
// 		if err != nil {
// 			return err
// 		}
// 		fileSize := fileInfo.Size()
// 		dataSize := int64(indexBlockSize + summaryBlockSize + filterBlockSize + metaBlockSize)
// 		if err = currentFile.Truncate(fileSize + dataSize); err != nil {
// 			return err
// 		}
// 		copy(mmapCurrFile, indexData)
// 		copy(mmapCurrFile, summaryData)
// 		copy(mmapCurrFile, filterData)
// 		copy(mmapCurrFile, merkleData)
// 	} else {
// 		/*
// 			for multi file implementation
// 			write data to dataFile, indexData to indexFile... each block in a separate file
// 		*/
// 		filePaths := makeMultiFilenames(subDirPath)
// 		for idx, fileName := range *filePaths {
// 			if idx == 0 {
// 				continue
// 			}
// 			file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
// 			if err != nil {
// 				return err
// 			}

// 			err = writeToFile(file, *groupedData[idx])
// 			if err != nil {
// 				return err
// 			}

// 			err = file.Close()
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	currentFile.Close()
// 	mmapCurrFile.Unmap()
// 	return nil
// }

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
	dirSize := len(dirEntries) - 1
	j := dirSize
	for {
		if j < 0 {
			return nil, errors.New("key not found")
		}
		subDirName := dirEntries[j].Name()
		subDirPath := filepath.Join(Path, subDirName)
		subDirEntries, err := os.ReadDir(subDirPath)
		if os.IsNotExist(err) {
			return nil, err
		}
		// search for the key from the newest to the oldest sstable
		// i variable will be increment each time as long as its not equal to the len of dirEntries
		subdirSize := len(subDirEntries) - 1
		i := subdirSize
		for {
			if i < 0 {
				j--
				break
			}
			sstDirName := subDirEntries[i].Name()
			sstDirPath := filepath.Join(subDirPath, sstDirName)
			sstDirEntries, err := os.ReadDir(sstDirPath)
			if os.IsNotExist(err) {
				return nil, err
			}
			sstDirSize := len(sstDirEntries)

			// search the single file if len == 1, otherwise multi files
			if sstDirSize == 1 {
				// get the data from single file sstable
				singleFilePath := filepath.Join(sstDirPath, sstDirEntries[0].Name())
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
			if sstDirSize == 1 {
				filter = mmapSingleFile[filterStart:metaStart]
			} else {
				currentFile, err = os.OpenFile(filepath.Join(sstDirPath, sstDirEntries[1].Name()), os.O_RDWR, 0644)
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
			if sstDirSize != 1 {
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
				if sstDirSize == 1 {
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
			if sstDirSize == 1 {
				summary = mmapSingleFile[summaryStart:filterStart]
			} else {
				currentFile, err = os.OpenFile(filepath.Join(sstDirPath, sstDirEntries[4].Name()), os.O_RDWR, 0644)
				if err != nil {
					return nil, err
				}
				summary, err = mmap.Map(currentFile, mmap.RDWR, 0)
				if err != nil {
					return nil, err
				}
			}

			indexOffset, summaryThinningConst, err := readSummaryFromFile(summary, key, sstable.compression, sstable.encoder)

			if sstDirSize != 1 {
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
				if sstDirSize == 1 {
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
			if sstDirSize == 1 {
				index = mmapSingleFile[indexStart:summaryStart]
			} else {
				currentFile, err = os.OpenFile(filepath.Join(sstDirPath, sstDirEntries[2].Name()), os.O_RDWR, 0644)
				if err != nil {
					return nil, err
				}
				index, err = mmap.Map(currentFile, mmap.RDWR, 0)
				if err != nil {
					return nil, err
				}
			}

			dataOffset, indexThinningConst, err := readIndexFromFile(index, summaryThinningConst, key, indexOffset, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}

			if sstDirSize != 1 {
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
			if sstDirSize == 1 {
				data = mmapSingleFile[dataStart:indexStart]
			} else {
				currentFile, err = os.OpenFile(filepath.Join(sstDirPath, sstDirEntries[0].Name()), os.O_RDWR, 0644)
				if err != nil {
					return nil, err
				}
				data, err = mmap.Map(currentFile, mmap.RDWR, 0)
				if err != nil {
					return nil, err
				}
			}
			dataRecord, err := readDataFromFile(data, indexThinningConst, key, dataOffset, sstable.compression, sstable.encoder)
			if err != nil {
				if err.Error() == "key not found" {
					i--
					continue
				}
				return nil, err
			}

			if sstDirSize != 1 {
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
				if sstDirSize == 1 {
					err = mmapSingleFile.Unmap()
					if err != nil {
						return nil, err
					}
					err = currentFile.Close()
					if err != nil {
						return nil, err
					}
				}

				return dataRecord, nil
			}
			i--

			if sstDirSize == 1 {
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
func readSummaryFromFile(mmapFile mmap.MMap, key string, compression bool, encoder *keyencoder.KeyEncoder) (uint64, uint16, error) {

	summaryConst, summaryMin, summaryMax, headerLength, err := readSummaryHeader(mmapFile, compression, encoder)

	if err != nil {
		return 0, 0, err
	}

	// check if key is in range of summary indexes
	if key < summaryMin || key > summaryMax {
		return 0, 0, errors.New("key not in range of summary index table")
	}

	// mmapFile = only summary records
	mmapFile = mmapFile[headerLength:]
	var previousSummaryRecord *IndexRecord
	var currentSummaryRecord *IndexRecord
	var recordLength uint64
	var offset uint64
	for {
		currentSummaryRecord, recordLength, err = readNextIndexRecord(mmapFile, offset, compression, encoder)
		if err != nil {
			return 0, 0, err
		}

		if currentSummaryRecord.Key == key {
			return currentSummaryRecord.Offset, summaryConst, nil
		}

		if currentSummaryRecord.Key > key {
			if previousSummaryRecord == nil {
				return 0, 0, errors.New("first record in summary smaller than summary min")
			}
			return previousSummaryRecord.Offset, summaryConst, nil
		}

		offset += recordLength

		// if we came to the end of the file return last one
		// because if it passed all the way to here and it didn't return nil when we checked if it is in the range in keys
		// then it has to be somewhere near the end after the last summary index
		if uint64(len(mmapFile)) <= offset {
			return currentSummaryRecord.Offset, summaryConst, nil
		}
		previousSummaryRecord = currentSummaryRecord
	}
}

// reads summary thinning const, min and max key, also returns header length
func readSummaryHeader(mmapFile mmap.MMap, compression bool, encoder *keyencoder.KeyEncoder) (summaryConst uint16, min string, max string, bytesRead uint64, err error) {
	summaryConst = 0
	bytesRead = 0
	min = ""
	max = ""
	err = nil

	if compression {
		bytesStep := 0

		var endOffset uint64

		//tempSummaryConst is needed because varint returns only 64-bit integers
		tempSummaryConst, bytesStep := binary.Uvarint(mmapFile)
		bytesRead += uint64(bytesStep)
		summaryConst = uint16(tempSummaryConst)

		if len(mmapFile[bytesRead:]) < CompressedIndexRecordMaxSize {
			endOffset = uint64(len(mmapFile[bytesRead:]))
		} else {
			endOffset = bytesRead + CompressedIndexRecordMaxSize
		}
		encodedMin, bytesStep := binary.Uvarint(mmapFile[bytesRead:endOffset])
		bytesRead += uint64(bytesStep)
		min, err = encoder.GetKey(encodedMin)
		if err != nil {
			return
		}

		if len(mmapFile[bytesRead:]) < CompressedIndexRecordMaxSize {
			endOffset = uint64(len(mmapFile[bytesRead:]))
		} else {
			endOffset = bytesRead + CompressedIndexRecordMaxSize
		}
		encodedMax, bytesStep := binary.Uvarint(mmapFile[bytesRead:endOffset])
		bytesRead += uint64(bytesStep)
		max, err = encoder.GetKey(encodedMax)
		if err != nil {
			return
		}

	} else {
		// first, we get sizes of summary min and max and summary thinning const
		summaryConst = binary.BigEndian.Uint16(mmapFile[SummaryConstStart:SummaryMinSizeStart])

		summaryMinSize := binary.BigEndian.Uint64(mmapFile[SummaryMinSizeStart:SummaryMaxSizeStart])
		summaryMaxSize := binary.BigEndian.Uint64(mmapFile[SummaryMaxSizeStart : SummaryMaxSizeStart+SummaryMaxSizeSize])

		// then read them and deserialize to get index records
		keysStart := uint64(SummaryMaxSizeStart + SummaryMaxSizeSize)
		serializedSummaryMin := mmapFile[keysStart : keysStart+summaryMinSize]
		serializedSummaryMax := mmapFile[keysStart+summaryMinSize : keysStart+summaryMinSize+summaryMaxSize]

		min = string(serializedSummaryMin)
		max = string(serializedSummaryMax)

		bytesRead = keysStart + summaryMinSize + summaryMaxSize
	}

	return
}

func readNextIndexRecord(mmapFile mmap.MMap, offset uint64, compression bool, encoder *keyencoder.KeyEncoder) (indexRecord *IndexRecord, recordLength uint64, err error) {
	err = nil
	if compression {
		if len(mmapFile[offset:]) < CompressedIndexRecordMaxSize {
			indexRecord, recordLength, err = DeserializeIndexRecord(mmapFile[offset:], compression, encoder)
		} else {
			indexRecord, recordLength, err = DeserializeIndexRecord(mmapFile[offset:offset+CompressedIndexRecordMaxSize], compression, encoder)
		}

	} else {
		// each record has keySize, key and offset
		keySize := binary.BigEndian.Uint64(mmapFile[offset+KeySizeStart : offset+KeySizeSize])
		recordLength = KeySizeSize + keySize + OffsetSize
		indexRecord, _, err = DeserializeIndexRecord(mmapFile[offset:offset+recordLength], compression, encoder)
	}
	return
}

// check if key is in index range, if it is return data record offset, if it is not return 0
// we start reading summaryThinningConst number of index records from offset in index file
func readIndexFromFile(mmapFile mmap.MMap, summaryConst uint16, key string, offset uint64, compression bool, encoder *keyencoder.KeyEncoder) (uint64, uint16, error) {
	var previousRecord *IndexRecord
	var currentRecord *IndexRecord
	var err error
	indexThinningConst := readIndexHeader(mmapFile, compression)
	indexRecordSize := uint64(0)
	//read SummaryConst number of index records
	for i := uint16(0); i < summaryConst; i++ {
		currentRecord, indexRecordSize, err = readNextIndexRecord(mmapFile, offset, compression, encoder)
		if err != nil {
			return 0, 0, err
		}

		if currentRecord.Key == key {
			return currentRecord.Offset, indexThinningConst, nil
		}

		// when you find the record which key is bigger, the result is the previous record which key is smaller
		if currentRecord.Key > key {
			if previousRecord == nil {
				return 0, 0, errors.New("key on given index offset is smaller than given key")
			}
			return previousRecord.Offset, indexThinningConst, nil
		}
		offset += indexRecordSize
		// when you get to the end it means the result is the last one read
		if uint64(len(mmapFile)) <= offset {
			return currentRecord.Offset, indexThinningConst, nil
		}
		previousRecord = currentRecord
	}
	//read all SummaryConst rec it means the result is the last one read
	return currentRecord.Offset, indexThinningConst, nil
}

// rads index thinning const
func readIndexHeader(mmapFile mmap.MMap, compression bool) uint16 {
	if compression {
		indexConst, _ := binary.Uvarint(mmapFile)
		return uint16(indexConst)
	} else {
		return binary.BigEndian.Uint16(mmapFile[:IndexConstSize])
	}
}

// param mmapFile - we do this instead passing the file or filename itself, so we can use function for
// both multi and single file sstable, we do this for all reads
func readDataFromFile(mmapFile mmap.MMap, indexThinningConst uint16, key string, offset uint64, compression bool, encoder *keyencoder.KeyEncoder) (*models.Data, error) {
	//read IndexConst number of data records
	for i := uint16(0); i < indexThinningConst; i++ {
		dataRecord, dataRecordSize, err := models.Deserialize(mmapFile[offset:], compression, encoder)

		if err != nil {
			return dataRecord, err
		}
		// keys must be equal
		if dataRecord.Key == key || key == "" {
			return dataRecord, nil
		}
		offset += dataRecordSize
		// when you get to the end it means there is no match
		if len(mmapFile) <= int(offset) {
			return nil, errors.New("key not found")
		}
	}
	return nil, errors.New("key not found")
}

func RemoveSSTable(filenames []string) error {
	for i := range filenames {
		err := os.Chmod(filenames[i], 0777) // Change permissions to allow deletion
		if err != nil {
			return err
		}

		err = os.RemoveAll(filenames[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Frdr testing rikvajrd
// subDirName - 01_sstable_00001
func (sstable *SSTable) CheckDataValidity(subDirName string) ([]*models.Data, error) {
	var corruptedData []*models.Data
	var metaMMap mmap.MMap
	var dataMMap mmap.MMap

	var merkleTree *merkle.MerkleTree
	var merkleTree2 *merkle.MerkleTree

	subDirPath := filepath.Join(Path, subDirName)
	subDirEntries, err := os.ReadDir(subDirPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	subDirSize := len(subDirEntries)

	offset := uint64(0)
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

		merkleTree = merkle.DeserializeMerkle(metaMMap)
		merkleTree2 = merkle.NewMerkle(merkleTree.HashWithSeed)
		merkleTree2Data := make([]byte, 0)
		for offset < uint64(len(dataMMap)) {
			dataRec, err := readDataFromFile(dataMMap, 1, "", offset, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			hashedData, err := merkleTree2.CreateNodeData(dataRec, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			merkleTree2Data = append(merkleTree2Data, hashedData...)
			offset += uint64(len(dataRec.Serialize(sstable.compression, sstable.encoder)))
		}
		err = merkleTree2.CreateMerkleTree(merkleTree2Data, sstable.compression, sstable.encoder)
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
		metaFile, err := os.OpenFile(filepath.Join(subDirPath, subDirEntries[3].Name()), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		tMetaMMap, err := mmap.Map(metaFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}
		copy(metaMMap, tMetaMMap)
		err = metaMMap.Unmap()
		if err != nil {
			return nil, err
		}
		err = metaFile.Close()
		if err != nil {
			return nil, err
		}
		merkleTree = merkle.DeserializeMerkle(metaMMap)

		dataFile, err := os.OpenFile(filepath.Join(subDirPath, subDirEntries[0].Name()), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		tDataMMap, err := mmap.Map(dataFile, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}
		copy(dataMMap, tDataMMap)
		err = dataMMap.Unmap()
		if err != nil {
			return nil, err
		}
		err = dataFile.Close()
		if err != nil {
			return nil, err
		}

		merkleTree2 = merkle.NewMerkle(merkleTree.HashWithSeed)
		merkleTree2Data := make([]byte, 0)
		for offset < uint64(len(dataMMap)) {
			dataRec, err := readDataFromFile(dataMMap, 1, "", offset, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			hashedData, err := merkleTree2.CreateNodeData(dataRec, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			merkleTree2Data = append(merkleTree2Data, hashedData...)
			offset += uint64(len(dataRec.Serialize(sstable.compression, sstable.encoder)))
		}
		err = merkleTree2.CreateMerkleTree(merkleTree2Data, sstable.compression, sstable.encoder)
		if err != nil {
			return nil, err
		}

	}

	corruptedIndexes, err := merkleTree.CompareTrees(merkleTree2)
	if err != nil {
		return nil, err
	}
	offset = 0
	var dataRec *models.Data
	for _, index := range corruptedIndexes {
		for index >= 0 {
			dataRec, err = readDataFromFile(dataMMap, 1, "", offset, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			index--
			offset += uint64(len(dataRec.Serialize(sstable.compression, sstable.encoder)))
		}
		corruptedData = append(corruptedData, dataRec)
	}

	return corruptedData, nil
}
