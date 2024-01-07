package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/DamjanVincic/key-value-engine/internal/models"

	"github.com/DamjanVincic/key-value-engine/internal/structures/bloomfilter"
)

/*
+---------------+-----------------+---------------+--...--+
|    CRC (4B)   | Timestamp (8B) | Tombstone(1B) |  Value |
+---------------+-----------------+---------------+--...--+
CRC = 32bit hash computed over the payload using CRC
Tombstone = If this record was deleted and has a value
Value = Value data
Timestamp = Timestamp of the operation in seconds

In order to optimize memory, we will not store the Key, KeySize and ValueSize found in Index.

The user can choose the way tables are written to disk, whether all structures are written in the same or separate files.
If user chooses the option to save structures to the same file, the program works with the SimpleSSTable structure and its functions,otherwise with SSTable.

Write path : DOPISATI
Read path : DOPISATI

sta cemo za summary proredjenost??
*/
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
	// File naming constants for simpleSSTable
	SimplePrefix = "ssst_"
)

type MemEntry struct {
	Key   string
	Value *models.Data
}

type SSTable struct {
	summaryConst         uint16
	dataFilename         string
	indexFilename        string
	summaryIndexFilename string
	filterFilename       string
	metadataFilename     string
	tocFilename          string
}

type SimpleSSTable struct {
	filename string
}

func NewSimpleSSTable(memEntries []MemEntry) (*SimpleSSTable, error) {
	// Create the directory if it doesn't exist
	dirEntries, err := os.ReadDir(SimplePath)
	if os.IsNotExist(err) {
		err := os.Mkdir(SimplePath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	var filename string
	var index uint8
	lsmIndex := uint8(1)

	// If there are no files in the directory, create the first one
	if len(dirEntries) == 0 {
		index = 1
		// Filename format: ssst_00001_lsmindex.log
	} else {
		// Get the last file
		filename = dirEntries[len(dirEntries)-1].Name()
		n, err := strconv.ParseUint(filename[5:10], 10, 32)
		index = uint8(n)
		if err != nil {
			return nil, err
		}
		index++
	}
	filename = fmt.Sprintf("%s%05d_%d.db", SimplePrefix, index, lsmIndex)
	file, err := os.OpenFile(filepath.Join(SimplePath, filename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	err = createFile(memEntries, file)
	if err != nil {
		return nil, err
	}

	file.Close()
	return &SimpleSSTable{
		filename: filename,
	}, nil

}

func createFile(memEntries []MemEntry, file *os.File) error {
	// Create one file for one SSTable, file contains data, index, summary, filter and merkle blocks and in header we store offsets of each block
	// skip first HeaderSize bytes for header
	offset := uint64(HeaderSize)
	file.Seek(int64(offset), 0)
	var indexRecords []byte
	var summaryRecords []byte

	bf := bloomfilter.CreateBloomFilter(len(memEntries), 0.2)
	counter := 0
	dataBlockSize := uint64(0)
	indexBlockSize := uint64(0)
	summaryBlockSize := uint64(0)

	for _, memEntry := range memEntries {
		sizeOfDR, err := writeDataRecord(file, memEntry)
		if err != nil {
			return err
		}
		serializedIndexRecord := NewIndexRecord(memEntry, offset).SerializeIndexRecord()
		indexRecords = append(indexRecords, serializedIndexRecord...)
		indexBlockSize += uint64(len(serializedIndexRecord))
		if counter%SummaryConst == 0 {
			summaryRecords = append(summaryRecords, serializedIndexRecord...)
			summaryBlockSize += uint64(len(serializedIndexRecord))
		}
		counter++
		offset += sizeOfDR
		dataBlockSize += sizeOfDR
		bf.AddElement([]byte(memEntry.Key))
	}
	filterBlockSize, err := writeBF(bf, file)
	if err != nil {
		return err
	}
	if _, err := file.Write(indexRecords); err != nil {
		return err
	}
	if _, err := file.Write(summaryRecords); err != nil {
		return err
	}

	file.Seek(0, 0)
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
	binary.BigEndian.PutUint64(header[DataBlockStart:FilterBlockStart], filterBlockSize)
	binary.BigEndian.PutUint64(header[FilterBlockStart:SummaryBlockStart], indexBlockSize)
	binary.BigEndian.PutUint64(header[SummaryBlockStart:MetaBlockStart], summaryBlockSize)
	if _, err := file.Write(header); err != nil {
		return err
	}
	return nil
}
func writeDataRecord(file *os.File, entry MemEntry) (uint64, error) {
	dRecord := *NewDataRecord(entry)
	serializedRecord := dRecord.SerializeDataRecord()

	if _, err := file.Write(serializedRecord); err != nil {
		return uint64(len(serializedRecord)), err
	}
	return uint64(len(serializedRecord)), nil

}
func writeBF(bf bloomfilter.BloomFilter, file *os.File) (uint64, error) {
	serializedBF := bf.Serialize()
	filterBlockSize := uint64(len(serializedBF))
	if err := writeToFile(file, serializedBF); err != nil {
		return filterBlockSize, err
	}
	return filterBlockSize, nil
}

func NewSSTable(memEntries []MemEntry, tableSize uint64) (*SSTable, error) {
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
	//index indicates the order in which the directories(sstables) were created
	var index uint32
	var subdirPath string
	var subdirName string

	// subdirName : sstableN (N - index)
	// If there are no subdirectoriums in the directory, create the first one

	if len(dirEntries) == 0 {
		index = 1
	} else {
		// Get the index from last added file and inkrement it by one
		subdirName = dirEntries[len(dirEntries)-1].Name()
		n, err := strconv.ParseUint(subdirName[7:], 10, 32)
		index = uint32(n)
		if err != nil {
			return nil, err
		}
		index++
	}
	subdirName = fmt.Sprintf("sstable%d", index)
	subdirPath = filepath.Join(Path, subdirName)
	err = os.Mkdir(subdirPath, os.ModePerm)
	if err != nil {
		return nil, err
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
	for _, entry := range memEntries {
		offset, _ = addToDataSegment(dataFile, entry)
		addToSparseIndex(indexFile, entry, offset)
	}

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
