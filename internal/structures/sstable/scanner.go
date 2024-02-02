package sstable

import (
	"encoding/binary"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/keyencoder"
	"github.com/DamjanVincic/key-value-engine/internal/structures/memtable"
	"github.com/edsrzf/mmap-go"
	"os"
	"path/filepath"
	"strings"
)

func (sstable *SSTable) getDataFilesWithPrefix(prefix string) (files []mmap.MMap, err error) {
	files = make([]mmap.MMap, 0)
	err = nil

	var data mmap.MMap
	var summary mmap.MMap

	// Storage for offsets in case of single file
	var dataStart uint64
	var indexStart uint64
	var summaryStart uint64
	var filterStart uint64

	var currentFile *os.File
	var mmapSingleFile mmap.MMap

	// if there is no dir to read from return nil
	dirEntries, err := os.ReadDir(Path)
	if os.IsNotExist(err) {
		return nil, err
	}

	// i variable will be increment each time as long as its not equal to the len of dirEntries
	dirSize := len(dirEntries) - 1
	i := dirSize
	for i >= 0 {
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

			dataSize := binary.BigEndian.Uint64(header[:IndexBlockStart])                   // dataSize
			indexSize := binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart]) // indexSize

			dataStart = uint64(HeaderSize)
			indexStart = dataStart + dataSize
			summaryStart = indexStart + indexSize
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

		var minKey string
		var maxKey string

		_, minKey, maxKey, _, err = readSummaryHeader(summary, sstable.compression, sstable.encoder)
		if err != nil {
			return
		}

		if (minKey > prefix && !strings.HasPrefix(minKey, prefix)) || (maxKey < prefix) {
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

		var positionedFile mmap.MMap
		positionedFile, err = findFirstWithPrefix(prefix, data, sstable.compression, sstable.encoder)
		if err != nil {
			return nil, err
		}
		if positionedFile != nil {
			files = append(files, positionedFile)
		}

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
		i--
	}
	return

	//ne treba da ih zatvaramo!!
}

func findFirstWithPrefix(prefix string, mmapFile mmap.MMap, compression bool, encoder *keyencoder.KeyEncoder) (mmap.MMap, error) {
	var offset uint64
	for {
		record, recordSize, err := models.Deserialize(mmapFile[offset:], compression, encoder)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(record.Key, prefix) {
			return mmapFile[offset:], nil
		}
		offset += recordSize
		if uint64(len(mmapFile)) <= offset {
			return nil, nil
		}
	}
}

type ScanningCandidate struct {
	key        string
	file       *ScannerFile
	offset     uint64
	recordSize uint64
	timestamp  uint64
}

type ScannerFile struct {
	file   mmap.MMap
	offset uint64
}

func (sstable *SSTable) PrefixScan(prefix string, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable) ([]*models.Data, error) {
	memKeys := memtable.GetKeysWithPrefix(prefix)
	candidates := make([]*ScanningCandidate, 0)
	found := make([]*ScanningCandidate, 0)
	var minCandidate *ScanningCandidate

	recordsNeeded := pageNumber * pageSize

	files, err := sstable.getDataFilesWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	scannerFiles := make([]*ScannerFile, len(files))
	for i, file := range files {
		scannerFiles[i] = &ScannerFile{file, 0}
	}

	for {
		if len(memKeys) > 0 {
			minCandidate = &ScanningCandidate{memKeys[0], nil, 0, 0, 0}
		}
		for _, scannerFile := range scannerFiles {
			record, recordSize, err := models.Deserialize(scannerFile.file[scannerFile.offset:], sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			if minCandidate == nil {
				minCandidate = &ScanningCandidate{record.Key, scannerFile, scannerFile.offset, recordSize, record.Timestamp}
				continue
			}
			if minCandidate.key > record.Key {
				minCandidate = &ScanningCandidate{record.Key, scannerFile, scannerFile.offset, recordSize, record.Timestamp}
				candidates = make([]*ScanningCandidate, 0)
				continue
			}
			if minCandidate.key == record.Key {
				if minCandidate.timestamp < record.Timestamp && minCandidate.file != nil {
					candidates = append(candidates, minCandidate)
					minCandidate = &ScanningCandidate{record.Key, scannerFile, scannerFile.offset, recordSize, record.Timestamp}
				} else {
					candidates = append(candidates, &ScanningCandidate{record.Key, scannerFile, scannerFile.offset, recordSize, record.Timestamp})
				}
			}
		}
		if minCandidate == nil || uint64(len(found)) >= recordsNeeded {
			break
		}

		found = append(found, minCandidate)

		if minCandidate.file == nil {
			memKeys = memKeys[1:]
		} else {
			minCandidate.file.offset += minCandidate.recordSize
			if uint64(len(minCandidate.file.file)) <= minCandidate.file.offset {
				scannerFiles = removeFromSlice(scannerFiles, minCandidate.file)
			}
			for _, candidate := range candidates {
				candidate.file.offset += candidate.recordSize
				if uint64(len(candidate.file.file)) <= candidate.file.offset {
					scannerFiles = removeFromSlice(scannerFiles, candidate.file)
				}
			}
		}
		minCandidate = nil
		candidates = make([]*ScanningCandidate, 0)
	}

	if uint64(len(found)) < (pageNumber-1)*pageSize {
		return nil, nil
		//zatvori fajlove!
	}
	records := make([]*models.Data, 0)
	for i := (pageNumber - 1) * pageSize; i < uint64(len(found)); i++ {
		if found[i].file == nil {
			records = append(records, memtable.Get(found[i].key))
		}
		record, _, err := models.Deserialize(found[i].file.file[found[i].offset:], sstable.compression, sstable.encoder)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func removeFromSlice(slice []*ScannerFile, item *ScannerFile) []*ScannerFile {
	for i, other := range slice {
		if other == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return nil
}
