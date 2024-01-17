package sstable

import (
	"encoding/binary"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/edsrzf/mmap-go"
	"os"
	"path/filepath"

	"github.com/DamjanVincic/key-value-engine/internal/structures/bloomfilter"
)

func ssstWriteFile(memEntries []*MemEntry, file *os.File) error {
	// all data for mapping file
	var data []byte
	dataSize := int64(0)
	// Create one file for one SSTable, file contains data, index, summary, filter and merkle blocks and in header we store offsets of each block
	// skip first HeaderSize bytes for header
	// here we save 4 uint42 number which say size of each block
	offset := uint64(HeaderSize)
	file.Seek(int64(offset), 0)
	var indexRecords []byte
	var summaryRecords []byte

	// summary structure must store min and max key value
	var summaryMin []byte
	var summaryMax []byte
	bf := bloomfilter.CreateBloomFilter(len(memEntries), 0.2)

	//counter variable is for summary partition
	counter := 0

	dataBlockSize := uint64(0)
	indexBlockSize := uint64(0)
	summaryBlockSize := uint64(0)
	metaBlockSize := uint64(0)

	for _, memEntry := range memEntries {
		//append data record to data
		dRecord := *NewDataRecord(memEntry)
		serializedRecord := dRecord.SerializeDataRecord()
		sizeOfDR := uint64(len(serializedRecord))
		data = append(data, serializedRecord...)

		//append index record to index byte array
		serializedIndexRecord := NewIndexRecord(memEntry, offset).SerializeIndexRecord()
		indexRecords = append(indexRecords, serializedIndexRecord...)
		indexBlockSize += uint64(len(serializedIndexRecord))

		if counter%SummaryConst == 0 {
			summaryRecords = append(summaryRecords, serializedIndexRecord...)
			summaryBlockSize += uint64(len(serializedIndexRecord))

			if summaryMin == nil {
				summaryMin = serializedIndexRecord
			}
		}
		counter++
		offset += sizeOfDR
		dataBlockSize += sizeOfDR
		bf.AddElement([]byte(memEntry.Key))
		summaryMax = serializedIndexRecord
	}
	//append filter to data
	serializedBF := bf.Serialize()
	filterBlockSize := uint64(len(serializedBF))
	data = append(data, serializedBF...)

	//append index to data
	data = append(data, indexRecords...)

	//create summary header in which we store sizes of min and max key value and min and max key values
	//put that informations in the beggining of summary block for faster reading later
	summaryHeader := make([]byte, 16)
	binary.BigEndian.PutUint64(summaryHeader[SummaryMinSizestart:SummaryMaxSizeStart], uint64(len(summaryMin)))
	binary.BigEndian.PutUint64(summaryHeader[SummaryMaxSizeStart:SummaryMaxSizeStart+SummaryMaxSizeSize], uint64(len(summaryMax)))
	summaryHeader = append(summaryHeader, summaryMin...)
	summaryHeader = append(summaryHeader, summaryMax...)
	summaryHeader = append(summaryHeader, summaryRecords...)
	summaryBlockSize += uint64(len(summaryHeader))
	//append summary to data
	data = append(data, summaryHeader...)
	data = append(data, summaryRecords...)

	//create and append merkle
	merkle, _ := CreateMerkleTree(memEntries)
	serializedMerkle := merkle.Serialize()
	data = append(data, serializedMerkle...)
	metaBlockSize += uint64(len(serializedMerkle))

	file.Seek(0, 0)
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint64(header[:DataBlockSizeSize], dataBlockSize)
	binary.BigEndian.PutUint64(header[FilterBlockStart:IndexBlockStart], filterBlockSize)
	binary.BigEndian.PutUint64(header[IndexBlockStart:SummaryBlockStart], indexBlockSize)
	binary.BigEndian.PutUint64(header[SummaryBlockStart:MetaBlockStart], summaryBlockSize)
	binary.BigEndian.PutUint64(header[MetaBlockStart:], metaBlockSize)

	//size of all data that needs to be written to mmap
	dataSize = dataSize + int64(dataBlockSize) + int64(filterBlockSize) + int64(indexBlockSize) + int64(summaryBlockSize) + int64(metaBlockSize) + HeaderSize
	//append data of all parts of SSTable to header
	header = append(header, data...)

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	fileSize := uint64(fileInfo.Size())

	err = file.Truncate(int64(fileSize + uint64(dataSize)))
	if err != nil {
		return err
	}

	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	// Copy the bytes that can fit in the current file
	copy(mmapFile[fileSize:], header)

	err = mmapFile.Unmap()
	if err != nil {
		return err
	}

	return nil
}

func (ssst *SimpleSSTable) Get() (*models.Data, error) {
	file, err := os.OpenFile(filepath.Join(SimplePath, ssst.Filename), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
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
	//get sizes of each part of SimpleSSTable
	header := mmapFile[:HeaderSize]
	datasize := uint64(binary.BigEndian.Uint64(header[:8]))
	filtersize := uint64(binary.BigEndian.Uint64(header[8:16]))
	indexsize := uint64(binary.BigEndian.Uint64(header[16:24]))
	summarysize := uint64(binary.BigEndian.Uint64(header[24:]))

	dataStart := HeaderSize
	filterStart := dataStart + int(datasize)
	indexStart := filterStart + int(filtersize)
	summaryStart := indexStart + int(indexsize)
	metaStart := summaryStart + int(summarysize)

	data := []byte{}
	filter := []byte{}
	index := []byte{}
	summary := []byte{}
	meta := []byte{}

	data = append(data, mmapFile[dataStart:filterStart]...)
	filter = append(filter, mmapFile[filterStart:indexStart]...)
	index = append(index, mmapFile[indexStart:summaryStart]...)
	summary = append(summary, mmapFile[summaryStart:metaStart]...)
	meta = append(meta, mmapFile[metaStart:]...)

	d, _ := DeserializeDataRecord(data)
	fmt.Println(d)
	f := bloomfilter.Deserialize(filter)
	fmt.Println(f)
	i, _ := DeserializeIndexRecord(index)
	s, _ := DeserializeIndexRecord(summary[16:])
	fmt.Println(i)
	fmt.Println(s)
	fmt.Println(meta)
	err = mmapFile.Unmap()
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	return nil, nil
}
