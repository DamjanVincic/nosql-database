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

func (sstable *SSTable) getDataFilesWithPrefix(prefix string) (files []*ScannerFile, err error) {
	files = make([]*ScannerFile, 0)
	err = nil

	var data mmap.MMap
	var summary mmap.MMap

	// Storage for offsets in case of single mmapFile
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
	dirSize := len(dirEntries) - 1
	j := dirSize
	for j >= 0 {
		//if j < 0 {
		//	return
		//}
		subDirName := dirEntries[j].Name()
		subDirPath := filepath.Join(Path, subDirName)
		subDirEntries, err := os.ReadDir(subDirPath)
		if os.IsNotExist(err) {
			return nil, err
		}

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

			// search the single mmapFile if len == 1, otherwise multi files
			if sstDirSize == 1 {
				// get the data from single mmapFile sstable
				singleFilePath := filepath.Join(sstDirPath, sstDirEntries[0].Name())
				currentFile, err = os.OpenFile(singleFilePath, os.O_RDWR, 0644)
				if err != nil {
					return nil, err
				}

				mmapSingleFile, err = mmap.Map(currentFile, mmap.RDWR, 0)
				if err != nil {
					return nil, err
				}

				//get sizes of each part of SSTable single mmapFile
				header := mmapSingleFile[:HeaderSize]

				dataSize := binary.BigEndian.Uint64(header[:IndexBlockStart])                      // dataSize
				indexSize := binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart])    // indexSize
				summarySize := binary.BigEndian.Uint64(header[SummaryBlockStart:FilterBlockStart]) // summarySize
				//filterSize := binary.BigEndian.Uint64(header[FilterBlockStart:])                   // filterSize

				dataStart = uint64(HeaderSize)
				indexStart = dataStart + dataSize
				summaryStart = indexStart + indexSize
				filterStart = summaryStart + summarySize
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

			_, minKey, maxKey, _, err2 := readSummaryHeader(summary, sstable.compression, sstable.encoder)

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

			if err2 != nil {
				return nil, err2
			}

			if (minKey > prefix && !strings.HasPrefix(minKey, prefix)) || (maxKey < prefix) {
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

			var positionedFile mmap.MMap
			positionedFile, err = findFirstWithPrefix(prefix, data, sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			if positionedFile != nil {
				files = append(files, &ScannerFile{mmapFile: positionedFile, osFile: currentFile, offset: 0})
			}

			i--
		}

	}
	return
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
	mmapFile mmap.MMap
	osFile   *os.File
	offset   uint64
}

func (sstable *SSTable) PrefixScan(prefix string, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable) ([]*models.Data, error) {
	memKeys := memtable.GetKeysWithPrefix(prefix)
	found := make([]*ScanningCandidate, 0)

	var minCandidate *ScanningCandidate
	var candidates []*ScanningCandidate

	recordsNeeded := pageNumber * pageSize

	scannerFiles, err := sstable.getDataFilesWithPrefix(prefix)

	filesToBeClosed := make([]*ScannerFile, len(scannerFiles))
	copy(filesToBeClosed, scannerFiles)

	if err != nil {
		return nil, err
	}

	for uint64(len(found)) < recordsNeeded {
		minCandidate, candidates, err = sstable.getCandidates(memKeys, scannerFiles)

		if minCandidate == nil {
			break
		}

		found = append(found, minCandidate)

		memKeys, scannerFiles = sstable.updateScannerFiles(minCandidate, candidates, memKeys, scannerFiles)
	}

	//there isn't enough keys with given prefix to fill pages before requested page
	if uint64(len(found)) < (pageNumber-1)*pageSize {
		err = sstable.closeScannerFiles(filesToBeClosed)
		return nil, err
	}

	var records []*models.Data

	records, err = sstable.getFoundRecords(found, memtable, pageNumber, pageSize)

	if err != nil {
		return nil, err
	}

	err = sstable.closeScannerFiles(filesToBeClosed)

	if err != nil {
		return nil, err
	}

	return records, nil
}

func (sstable *SSTable) getCandidates(memKeys []string, scannerFiles []*ScannerFile) (minCandidate *ScanningCandidate, candidates []*ScanningCandidate, err error) {
	err = nil
	minCandidate = nil
	candidates = make([]*ScanningCandidate, 0)

	if len(memKeys) > 0 {
		minCandidate = &ScanningCandidate{memKeys[0], nil, 0, 0, 0}
	}
	for _, scannerFile := range scannerFiles {
		var record *models.Data
		var recordSize uint64

		record, recordSize, err = models.Deserialize(scannerFile.mmapFile[scannerFile.offset:], sstable.compression, sstable.encoder)
		if err != nil {
			return
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
	return
}

func (sstable *SSTable) updateScannerFiles(minCandidate *ScanningCandidate, candidates []*ScanningCandidate, memKeys []string, scannerFiles []*ScannerFile) ([]string, []*ScannerFile) {
	if minCandidate.file == nil {
		memKeys = memKeys[1:]
	} else {
		minCandidate.file.offset += minCandidate.recordSize
		if uint64(len(minCandidate.file.mmapFile)) <= minCandidate.file.offset {
			scannerFiles = removeFromSlice(scannerFiles, minCandidate.file)
		}
	}
	for _, candidate := range candidates {
		candidate.file.offset += candidate.recordSize
		if uint64(len(candidate.file.mmapFile)) <= candidate.file.offset {
			scannerFiles = removeFromSlice(scannerFiles, candidate.file)
		}
	}
	return memKeys, scannerFiles
}

func (sstable *SSTable) closeScannerFiles(filesToBeClosed []*ScannerFile) error {
	var err error
	for _, file := range filesToBeClosed {
		err = file.mmapFile.Unmap()
		err = file.osFile.Close()
	}
	return err
}

func (sstable *SSTable) getFoundRecords(found []*ScanningCandidate, memtable *memtable.Memtable, pageNumber uint64, pageSize uint64) ([]*models.Data, error) {
	records := make([]*models.Data, 0)
	for i := (pageNumber - 1) * pageSize; i < uint64(len(found)); i++ {
		if found[i].file == nil {
			records = append(records, memtable.Get(found[i].key))
		} else {
			record, _, err := models.Deserialize(found[i].file.mmapFile[found[i].offset:], sstable.compression, sstable.encoder)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
		}
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
