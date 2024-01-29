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
	wal "github.com/DamjanVincic/key-value-engine/internal/structures/wal"

	"github.com/edsrzf/mmap-go"
)

const (
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	OffsetSize    = 8
	ValueSizeSize = 8
	//for dataRecord
	TimestampStart     = 0
	TombstoneStart     = TimestampStart + TimestampSize
	DataKeySizeStart   = TombstoneStart + TombstoneSize
	DataValueSizeStart = DataKeySizeStart + KeySizeSize
	DataKeyStart       = DataValueSizeStart + ValueSizeSize
	RecordHeaderSize   = TimestampSize + TombstoneSize + KeySizeSize + ValueSizeSize
	//for index and summary index thinning
	IndexConst   = 5 //from config file
	SummaryConst = 5 //from config file
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
	IndexBlockStart      = DataBlockStart + DataBlockSizeSize
	SummaryBlockStart    = IndexBlockStart + IndexBlockSizeSize
	FilterBlockStart     = SummaryBlockStart + SummaryBlockSizeSize
	MetaBlockStart       = FilterBlockStart + FilterBlockSizeSize

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

// format in which SSTable gets data from memTable
type MemEntry struct {
	Key   string
	Value *models.Data
}

/*
same struct for SSTable single and multi-file implementation

	for single - filename is the name of the TOC file which contains filenames of data, index, summary, filter and meta file
	for multi - filename is the name of the only file which contains data, index, summary, filter and meta blocks
	there are same functions for both implementations, not separate ones
*/
type SSTable struct {
	indexConst   uint16
	summaryConst uint16
	filename     string
}

// we know which SSTable to create based on the singleFile variable, set in the configuration file
func NewSSTable(memEntries []*models.Data, singleFile bool) (*SSTable, error) {
	// Create the ssTable directory (with all ssTable files) if it doesn't exist
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

	// temporarly the lsm index is set to 1
	lsmIndex := uint8(1)
	//index - sequence number of the sstable
	var index uint8
	/* sstable dir contains subdirs for each ssTable
	one subdir = one sstable
	same names for both implementations, no difference
	the difference is in the number of files in subdir
	for multi - 6, mor single - 1 */
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
			return nil, err
		}
	} else {
		// Get the last sstable (take the index from the last entered one)
		// sort the entries in numerical order, because they are returned in lexicographical
		sort.Slice(dirEntries, func(i, j int) bool {
			numI, _ := strconv.Atoi(dirEntries[i].Name()[:])
			numJ, _ := strconv.Atoi(dirEntries[j].Name()[:])
			return numI < numJ
		})

		subdirName = dirEntries[len(dirEntries)-1].Name()
		n, err := strconv.ParseUint(subdirName[11:], 10, 8)
		if err != nil {
			return nil, err
		}
		index = uint8(n)
		if err != nil {
			return nil, err
		}
		index++
		subdirName = fmt.Sprintf("%02d_sstable_%05d", lsmIndex, index)
		subdirPath = filepath.Join(Path, subdirName)
		err = os.Mkdir(subdirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	// creates files and save the data
	if singleFile {
		// create single file for ssTable
		// name - sst_00000  - 5-digit num for index
		filename := fmt.Sprintf("%s%05d.db", Prefix, index)
		file, err := os.OpenFile(filepath.Join(subdirPath, filename), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		err = createFiles(memEntries, singleFile, file)
		if err != nil {
			return nil, err
		}

		err = file.Close()
		if err != nil {
			return nil, err
		}

		return &SSTable{
			indexConst:   IndexConst,
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
			indexConst:   IndexConst,
			summaryConst: SummaryConst,
			filename:     tocFilename,
		}, nil
	}
}

// we distinguish implementations by the singleFile value (and the num of params, 1 for single, 6 for multi)
func createFiles(memEntries []*models.Data, singleFile bool, files ...*os.File) error {
	// variables for storing serialized data
	var data []byte
	var dataRecords []byte
	var indexRecords []byte
	var summaryRecords []byte
	var tocData []byte
	// in summary header we have min and max index record (ranked by key)
	summaryHeader := make([]byte, SummaryMinSizeSize+SummaryMaxSizeSize)
	var summaryMin []byte
	var summaryMax []byte
	var serializedIndexRecord []byte
	var serializedSummaryRecord []byte
	// for single file header
	dataBlockSize := uint64(0)
	indexBlockSize := uint64(0)
	summaryBlockSize := uint64(0)
	filterBlockSize := uint64(0)

	// needed for offsets and single file header
	sizeOfDR := uint64(0)
	sizeOfIR := uint64(0)
	sizeOfSR := uint64(0)

	// same start offset for both implementations
	//if its single file implementation we will take blocks from the single file and consider them as multi files
	offset := uint64(0)
	//for summary index
	indexOffset := uint64(0)

	// counter for index and index summary, for every n index records add one to summary
	countRecords := 0
	countIndexRecords := 0

	merkleDataRecords := []*models.DataRecord{}
	//create an empty bloom filter
	filter := bloomfilter.CreateBloomFilter(len(memEntries), 0.001)
	// proccess of adding entries
	for _, entry := range memEntries {
		//every entry is saved in data segment

		dataRecord := *models.NewDataRecord(entry)
		merkleDataRecords = append(merkleDataRecords, &dataRecord)
		serializedRecord := dataRecord.Serialize()
		sizeOfDR = uint64(len(serializedRecord))
		dataRecords = append(dataRecords, serializedRecord...)

		dataBlockSize += sizeOfDR
		// every Nth one is saved in the index (key, offset of dataRec)
		if countRecords%IndexConst == 0 {
			serializedIndexRecord, indexRecords = addToIndex(offset, entry, indexRecords)
			sizeOfIR = uint64(len(serializedIndexRecord))
			indexBlockSize += sizeOfIR
			// every Nth one is saved in the summary index (key, offset of indexRec)
			if countIndexRecords%SummaryConst == 0 {
				serializedSummaryRecord, summaryRecords = addToIndex(indexOffset, entry, summaryRecords)
				sizeOfSR = uint64(len(serializedSummaryRecord))
				summaryBlockSize += sizeOfSR
				if summaryMin == nil {
					summaryMin = serializedSummaryRecord
				}
			}
			indexOffset += sizeOfIR
			countIndexRecords++
			summaryMax = serializedIndexRecord
		}
		//add key to bf
		err := filter.AddElement([]byte(entry.Key))
		if err != nil {
			return err
		}
		offset += sizeOfDR
		countRecords++
	}
	merkle, err := CreateMerkleTree(merkleDataRecords, HashWithSeed{Seed: []byte{}})
	if err != nil {
		return err
	}
	//serialize filter and data
	filterData := filter.Serialize()
	filterBlockSize = uint64(len(filterData))
	merkleData := merkle.Serialize()

	//creating summary index header
	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizestart:SummaryMaxSizeStart], uint64(len(summaryMin)))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len(summaryMax)))
	summaryHeaderSize := SummaryMinSizeSize + SummaryMaxSizeSize + uint64(len(summaryMin)) + uint64(len(summaryMax))
	//append all summary index records
	summaryHeader = append(summaryHeader, summaryMin...)
	summaryHeader = append(summaryHeader, summaryMax...)
	summaryHeader = append(summaryHeader, summaryRecords...)
	summaryBlockSize += uint64(summaryHeaderSize)

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
		data = append(data, dataRecords...)
		data = append(data, indexRecords...)
		data = append(data, summaryHeader...)
		data = append(data, filterData...)
		data = append(data, merkleData...)

		err = WriteToFile(files[0], data)
		if err != nil {
			return err
		}

		return nil
	}
	/*
		for multi file implementation
		create TOC file data - names of all sstable files
		write data to dataFile, indexData to indexFile... each block in a separate file
	*/
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

// get serialized data
func addToDataSegment(entry *MemEntry, result []byte, merkleDataRecords []*wal.Record) (uint64, []byte) {
	dataRecord := *wal.NewRecord(entry.Key, entry.Value.Value, entry.Value.Tombstone)
	merkleDataRecords = append(merkleDataRecords, &dataRecord)
	serializedRecord := dataRecord.Serialize()
	return uint64(len(serializedRecord)), append(result, serializedRecord...)
}

func addToIndex(offset uint64, entry *models.Data, result []byte) ([]byte, []byte) {
	indexRecord := NewIndexRecord(entry, offset)
	serializedIndexRecord := indexRecord.SerializeIndexRecord()
	return serializedIndexRecord, append(result, serializedIndexRecord...)
}

// TOC file contains filenames of all sstable parts - data, index, summary, filter, metadata
func createTocData(dataFilename, indexFilename, summaryFilename, filterFilename, metadataFilename string) []byte {
	// reserve places for sizes of filenames
	dataLength := make([]byte, FileNamesSizeSize)
	indexLength := make([]byte, FileNamesSizeSize)
	summaryLength := make([]byte, FileNamesSizeSize)
	filterLength := make([]byte, FileNamesSizeSize)

	// put sizes in created byte array
	binary.BigEndian.PutUint64(dataLength, uint64(len(dataFilename)))
	binary.BigEndian.PutUint64(indexLength, uint64(len(indexFilename)))
	binary.BigEndian.PutUint64(summaryLength, uint64(len(summaryFilename)))
	binary.BigEndian.PutUint64(filterLength, uint64(len(filterFilename)))

	// add all data together
	data := append(dataLength, indexLength...)
	data = append(data, summaryLength...)
	data = append(data, filterLength...)

	data = append(data, []byte(dataFilename)...)
	data = append(data, []byte(indexFilename)...)
	data = append(data, []byte(summaryFilename)...)
	data = append(data, []byte(filterFilename)...)
	data = append(data, []byte(metadataFilename)...)

	return data
}

// write data to file using mmap
func WriteToFile(file *os.File, data []byte) error {
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
func Get(key string) (*models.Data, error) {

	var mmapFileData mmap.MMap
	var mmapFileFilter mmap.MMap
	var mmapFileSummary mmap.MMap
	var mmapFileIndex mmap.MMap
	var mmapFileMeta mmap.MMap

	// if there is no dir to read from return nil
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		return nil, err
	}

	//sort in numerical order by indexes, it is returned in lexicographical
	sort.Slice(dirEntries, func(i, j int) bool {
		numI, _ := strconv.Atoi(dirEntries[i].Name()[11:])
		numJ, _ := strconv.Atoi(dirEntries[j].Name()[11:])
		return numI < numJ
	})

	// search for the key from the newest to the oldest sstable
	// i variable will be increment each time as long as its not equal to the len of dirEntries
	i := 1
	for {
		subDirName := dirEntries[len(dirEntries)-i].Name()
		subDirPath := filepath.Join(Path, subDirName)
		subDirEntries, err := os.ReadDir(subDirPath)
		if os.IsNotExist(err) {
			return nil, err
		}

		// search the single file if len == 1, otherwise multi files
		if len(subDirEntries) == 1 {
			// get the data from single file sstable
			simpleFilePath := filepath.Join(subDirPath, subDirEntries[0].Name())
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
			datasize := uint64(binary.BigEndian.Uint64(header[:IndexBlockStart]))
			indexsize := uint64(binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart]))
			summarysize := uint64(binary.BigEndian.Uint64(header[SummaryBlockStart:FilterBlockStart]))
			filtersize := uint64(binary.BigEndian.Uint64(header[FilterBlockStart:]))

			dataStart := uint64(HeaderSize)
			indexStart := dataStart + datasize
			summaryStart := indexStart + indexsize
			filterStart := summaryStart + summarysize
			metaStart := filterStart + filtersize

			// same as hole mmap file in multi file sstable
			// copy data in new slices to prevent errors after Unmap()
			mmapFileDataPart := mmapFileSimple[dataStart:indexStart]
			mmapFileData = make([]byte, len(mmapFileDataPart))
			copy(mmapFileData, mmapFileDataPart)

			mmapFileIndexPart := mmapFileSimple[indexStart:summaryStart]
			mmapFileIndex = make([]byte, len(mmapFileIndexPart))
			copy(mmapFileIndex, mmapFileIndexPart)

			mmapFileSummaryPart := mmapFileSimple[summaryStart:filterStart]
			mmapFileSummary = make([]byte, len(mmapFileSummaryPart))
			copy(mmapFileSummary, mmapFileSummaryPart)

			mmapFileFilterPart := mmapFileSimple[filterStart:metaStart]
			mmapFileFilter = make([]byte, len(mmapFileFilterPart))
			copy(mmapFileFilter, mmapFileFilterPart)

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
			// tocFilePath := filepath.Join(subDirPath, subDirEntries[5].Name())

			// in the next part we are doing the copying in case mmap acts unexpectedly after file is closed
			// get data file to read
			dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			mmapFileDataPart, err := mmap.Map(dataFile, mmap.RDWR, 0)
			if err != nil {
				return nil, err
			}
			mmapFileData = make([]byte, len(mmapFileDataPart))
			copy(mmapFileData, mmapFileDataPart)

			// get filter to read
			filterFile, err := os.OpenFile(filterFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
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

			// get summary file to read
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

			// get metadata file to read
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

			// tocFile, err := os.OpenFile(tocFilePath, os.O_RDWR, 0644)

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
			// err = tocFile.Close()
			// if err != nil {
			// 	return nil, err
			// }
		}

		if !compareMerkleTrees(mmapFileMeta, mmapFileData) {
			return nil, errors.New("CORRUPTED DATA")
		}

		// start process for getting the element
		// first we need to check if its in bloom filter
		found, err := ReadBloomFilterFromFile(key, mmapFileFilter)
		if err != nil {
			return nil, err
		}
		// if its not found, check next sstable
		if !found {
			i++
			// if there are no more sstables, return nil for value
			if i == len(dirEntries) {
				return nil, nil
			}
			continue
		}

		//check if its in summary range (between min and max index)
		indexOffset, err := ReadSummaryFromFile(mmapFileSummary, key)

		if err != nil {
			i++
			continue
		}

		//check if its in index
		dataOffset, err := ReadIndexFromFile(mmapFileIndex, key, indexOffset)

		if err != nil {
			i++
			continue
		}

		//find it in data
		dataRecord, err := ReadDataFromFile(mmapFileData, key, dataOffset)

		if err != nil {
			i++
			continue
		}
		if dataRecord != nil {
			return &models.Data{Value: dataRecord.Value, Tombstone: dataRecord.Tombstone, Timestamp: dataRecord.Timestamp}, nil
		}
	}
}

// read filenames from TOC file
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

	// Read sizes
	dataSize := binary.BigEndian.Uint64(mmapFile[:FileNamesSizeSize])
	indexSize := binary.BigEndian.Uint64(mmapFile[FileNamesSizeSize : 2*FileNamesSizeSize])
	summarySize := binary.BigEndian.Uint64(mmapFile[2*FileNamesSizeSize : 3*FileNamesSizeSize])
	filterSize := binary.BigEndian.Uint64(mmapFile[3*FileNamesSizeSize : 4*FileNamesSizeSize])

	dataStart := uint64(HeaderSize)
	indexStart := dataStart + dataSize
	summaryStart := indexStart + indexSize
	filterStart := summaryStart + summarySize
	metaStart := filterStart + filterSize

	// read each filename
	dataFile := string(mmapFile[dataStart:indexStart])
	indexFile := string(mmapFile[indexStart:summaryStart])
	summaryFile := string(mmapFile[summaryStart:filterStart])
	filterFile := string(mmapFile[filterStart:metaStart])
	metadataFile := string(mmapFile[metaStart:])

	return []string{dataFile, indexFile, summaryFile, filterFile, metadataFile}, nil
}

// mmapFile in case of multi file sstable will be the hole file
// in the case of single file sstable it will only be part that is bloom filter
func ReadBloomFilterFromFile(key string, mmapFile mmap.MMap) (bool, error) {
	filter := bloomfilter.Deserialize(mmapFile)
	found, err := filter.ContainsElement([]byte(key))
	if err != nil {
		return false, err
	}
	return found, err
}

// check if key is in summary range, if it is return index record offset, if it is not return 0
func ReadSummaryFromFile(mmapFile mmap.MMap, key string) (uint64, error) {

	// first, we get sizes of summary min and max
	summaryMinSize := binary.BigEndian.Uint64(mmapFile[SummaryMinSizestart:SummaryMaxSizeStart])
	summaryMaxSize := binary.BigEndian.Uint64(mmapFile[SummaryMaxSizeStart : SummaryMaxSizeStart+SummaryMaxSizeSize])

	// then read them and deserialize to get index records
	keysStart := uint64(SummaryMaxSizeStart + SummaryMaxSizeSize)
	serializedSummaryMin := mmapFile[keysStart : keysStart+summaryMinSize]
	serializedSummaryMax := mmapFile[keysStart+summaryMinSize : keysStart+summaryMinSize+summaryMaxSize]

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

	// mmapFile = only summary records
	mmapFile = mmapFile[keysStart+summaryMinSize+summaryMaxSize:]
	mmapFileSize := uint64(len(mmapFile))
	var summaryRecords []*IndexRecord
	offset := uint64(0)
	for {
		// each summary record has keySize, key and offset
		keySize := binary.BigEndian.Uint64(mmapFile[offset+KeySizeStart : offset+KeySizeSize])
		summaryRecordSize := KeySizeSize + keySize + OffsetSize
		serializedSummRec := mmapFile[offset : offset+summaryRecordSize]
		summaryRecord, err := DeserializeIndexRecord(serializedSummRec)
		if err != nil {
			return 0, errors.New("error deserializing index record")
		}
		summaryRecords = append(summaryRecords, summaryRecord)

		// return second to last if we found the place of the key
		if len(summaryRecords) >= 2 && summaryRecords[len(summaryRecords)-1].Key > key && summaryRecords[len(summaryRecords)-2].Key <= key {
			return summaryRecords[len(summaryRecords)-2].Offset, nil
		}
		offset += summaryRecordSize
		// if we came to the end of the file return last one
		// because if it passed all the way to here and it didnt return nil when we checked if its in the range in keys
		// then it has to be somewhere near the end after the last summary index
		if mmapFileSize == offset {
			return summaryRecord.Offset, nil
		}

	}
}

// check if key is in index range, if it is return data record offset, if it is not return 0
func ReadIndexFromFile(mmapFile mmap.MMap, key string, offsetStart uint64) (uint64, error) {
	var indexRecords []*IndexRecord
	// 8 for size of key size
	// key size for key
	// 8 for offset
	// make sure that the next thing we read is indeed index (/key size of the key of index)
	mmapFile = mmapFile[offsetStart:]
	mmapFileSize := uint64(len(mmapFile))
	indexRecordSize := uint64(0)
	offset := uint64(0)
	//read SummaryConst number of index records
	for i := 0; i < SummaryConst; i++ {
		keySize := binary.BigEndian.Uint64(mmapFile[offset+KeySizeStart : offset+KeySizeSize])
		// this could be const as well, we know all offsets are uint64
		indexRecordSize = keySize + KeySizeSize + OffsetSize
		indexRecord, err := DeserializeIndexRecord(mmapFile[offset : offset+indexRecordSize])
		if err != nil {
			return 0, errors.New("error deserializing index record")
		}
		indexRecords = append(indexRecords, indexRecord)

		// when you find the record which key is bigger, the result is the previous record which key is smaller
		if len(indexRecords) >= 2 && indexRecords[len(indexRecords)-1].Key > key && indexRecords[len(indexRecords)-2].Key <= key {
			return indexRecords[len(indexRecords)-2].Offset, nil
		}
		offset += indexRecordSize
		// when you get to the end it means the result is the last one read
		if mmapFileSize == offset {
			return indexRecord.Offset, nil
		}
	}
	//read all SummaryConst rec it means the result is the last one read
	return indexRecords[len(indexRecords)-1].Offset, nil
}

// we need to know where datarecord is placed to after looking for key in
// index file we have offset of data we are looking for and for the one that goes after it
// deserialization bytes between these to locations gives us the said record we are looking for
// param mmapFile - we do this instead passing the file or filename itself so we can use function for
// both multi and single sile sstable, we do this for all reads
func ReadDataFromFile(mmapFile mmap.MMap, key string, offset uint64) (*wal.Record, error) {
	mmapFile = mmapFile[offset:]
	mmapFileSize := len(mmapFile)
	dataRecordSize := uint64(0)
	offset = 0
	//read IndexConst number of data records
	for i := 0; i < IndexConst; i++ {
		keySize := binary.BigEndian.Uint64(mmapFile[offset+DataKeySizeStart : offset+DataValueSizeStart])
		valueSize := binary.BigEndian.Uint64(mmapFile[offset+DataValueSizeStart : offset+DataKeyStart])

		// make sure to read complete data rec
		dataRecordSize = RecordHeaderSize + keySize + valueSize
		dataRecord, err := wal.Deserialize(mmapFile[offset : offset+dataRecordSize])
		if err != nil {
			return nil, errors.New("error deserializing index record")
		}

		// keys must be equal
		if dataRecord.Key == key {
			return dataRecord, nil
		}
		offset += dataRecordSize
		// when you get to the end it means there is no match
		if mmapFileSize == int(offset) {
			return nil, nil
		}
	}
	return nil, nil
}

func ReadMerkle(mmapFile mmap.MMap) (*MerkleTree, error) {
	merkle, err := DeserializeMerkle(mmapFile)
	if err != nil {
		return nil, err
	}
	return merkle, err
}
func GetAllMemEntries(mmapFile mmap.MMap) ([]*models.DataRecord, error) {
	var entries []*models.DataRecord
	mmapFileSize := len(mmapFile)
	dataRecordSize := uint64(0)
	offset := uint64(0)
	//read IndexConst number of data records
	for mmapFileSize != int(offset) {
		keySize := binary.BigEndian.Uint64(mmapFile[offset+DataKeySizeStart : offset+DataValueSizeStart])
		valueSize := binary.BigEndian.Uint64(mmapFile[offset+DataValueSizeStart : offset+DataKeyStart])

		// make sure to read complete data rec
		dataRecordSize = RecordHeaderSize + keySize + valueSize
		dataRecord := models.Deserialize(mmapFile[offset : offset+dataRecordSize])
		entries = append(entries, dataRecord)
		offset += dataRecordSize
		// when you get to the end it means there is no match
	}
	return entries, nil
}
func compareMerkleTrees(mmapFileMeta, mmapFileData mmap.MMap) bool {
	merkle, err := ReadMerkle(mmapFileMeta)
	if err != nil {
		return false
	}
	hashWithSeed := merkle.HashWithSeed
	entries, err := GetAllMemEntries(mmapFileData)
	if err != nil {
		return false
	}
	newMerkle, err := CreateMerkleTree(entries, hashWithSeed)
	if err != nil {
		return false
	}
	return merkle.IsEqualTo(newMerkle)
}
