package sstable

import (
	"encoding/binary"
	"errors"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/keyencoder"
	"github.com/DamjanVincic/key-value-engine/internal/structures/memtable"
	"github.com/edsrzf/mmap-go"
	"os"
	"path/filepath"
	"strings"
)

func (sstable *SSTable) getDataFilesForScanning(param1 string, param2 string, prefix bool) (files []*ScannerFile, err error) {
	files = make([]*ScannerFile, 0)
	err = nil

	var data mmap.MMap
	var summary mmap.MMap
	var index mmap.MMap

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

			_, minKey, maxKey, _, err := readSummaryHeader(summary, sstable.compression, sstable.encoder)

			prefix_not_possible := (minKey > param1 && !strings.HasPrefix(minKey, param1)) || (maxKey < param1)
			range_not_possible := maxKey < param1 || minKey > param2

			if (prefix && prefix_not_possible) || (!prefix && range_not_possible) {
				i--
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

			minKeyIsInRange := minKey >= param1 && minKey <= param2

			if (prefix && strings.HasPrefix(minKey, param1)) || (!prefix && minKeyIsInRange) {
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

				if sstDirSize == 1 {
					files = append(files, &ScannerFile{mmapFile: data, originalMmapFile: mmapSingleFile, osFile: currentFile, offset: 0})
				} else {
					files = append(files, &ScannerFile{mmapFile: data, originalMmapFile: data, osFile: currentFile, offset: 0})
				}

			} else {
				indexOffset, summaryThinningConst, err := readSummaryFromFile(summary, param1, sstable.compression, sstable.encoder)

				if err != nil {
					//uradi zatvaranje fajlova
					return nil, err
				}

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

				dataOffset, _, err := readIndexFromFile(index, summaryThinningConst, param1, indexOffset, sstable.compression, sstable.encoder)

				if err != nil {
					//uradi zatvaranje fajlova
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

				var positionedFile mmap.MMap

				if prefix {
					positionedFile, err = findFirstWithPrefix(param1, data[dataOffset:], sstable.compression, sstable.encoder)
				} else {
					positionedFile, err = findFirstInRange(param1, param2, data[dataOffset:], sstable.compression, sstable.encoder)
				}

				if err != nil {
					return nil, err
				}
				if positionedFile != nil {
					if sstDirSize == 1 {
						files = append(files, &ScannerFile{mmapFile: positionedFile, originalMmapFile: mmapSingleFile, osFile: currentFile, offset: 0})
					} else {
						files = append(files, &ScannerFile{mmapFile: positionedFile, originalMmapFile: data, osFile: currentFile, offset: 0})
					}
				}
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

func findFirstInRange(min string, max string, mmapFile mmap.MMap, compression bool, encoder *keyencoder.KeyEncoder) (mmap.MMap, error) {
	var offset uint64
	for {
		record, recordSize, err := models.Deserialize(mmapFile[offset:], compression, encoder)
		if err != nil {
			return nil, err
		}
		if record.Key >= min && record.Key <= max {
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
	mmapFile         mmap.MMap
	originalMmapFile mmap.MMap
	osFile           *os.File
	offset           uint64
}

type PrefixIterator struct {
	memKeys         []string
	scannerFiles    []*ScannerFile
	filesToBeClosed []*ScannerFile
	sstable         *SSTable
	memtable        *memtable.Memtable
	prefix          string
}

func (sstable *SSTable) NewPrefixIterator(prefix string, memtable *memtable.Memtable) (*PrefixIterator, *models.Data, error) {
	records, err, memKeys, scannerFiles, filesToBeClosed := sstable.scanWithConditions(prefix, "", true, 1, 1, memtable, true)
	if err != nil {
		return nil, nil, err
	}
	var record *models.Data
	if len(records) > 0 {
		record = records[0]
	}
	return &PrefixIterator{memKeys: memKeys, scannerFiles: scannerFiles, filesToBeClosed: filesToBeClosed, sstable: sstable, memtable: memtable, prefix: prefix}, record, err
}

func (iterator *PrefixIterator) Next() (*models.Data, error) {

	minCandidate, candidates, filesWithoutPrefix, err := iterator.sstable.getCandidates(iterator.memKeys, iterator.scannerFiles, iterator.prefix, "", true)

	if err != nil {
		return nil, err
	}

	for _, file := range filesWithoutPrefix {
		iterator.scannerFiles = removeFromSlice(iterator.scannerFiles, file)
	}

	if minCandidate == nil {
		return nil, errors.New("no records left")
	}

	var record *models.Data

	if minCandidate.file == nil {
		record = iterator.memtable.Get(minCandidate.key)
	} else {
		record, _, err = models.Deserialize(minCandidate.file.mmapFile[minCandidate.offset:], iterator.sstable.compression, iterator.sstable.encoder)
		if err != nil {
			return nil, err
		}
	}

	iterator.memKeys, iterator.scannerFiles = iterator.sstable.updateScannerFiles(minCandidate, candidates, iterator.memKeys, iterator.scannerFiles)

	return record, nil
}

func (iterator *PrefixIterator) Stop() error {
	return iterator.sstable.closeScannerFiles(iterator.filesToBeClosed)
}

func (sstable *SSTable) PrefixScan(prefix string, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable) (records []*models.Data, err error) {
	records, err, _, _, _ = sstable.scanWithConditions(prefix, "", true, pageNumber, pageSize, memtable, false)

	if err != nil {
		records = nil
	}
	return
}

func (sstable *SSTable) scanWithConditions(param1 string, param2 string, prefix bool, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable, iterator bool) (records []*models.Data, err error, memKeys []string, scannerFiles []*ScannerFile, filesToBeClosed []*ScannerFile) {
	if prefix {
		memKeys = memtable.GetKeysWithPrefix(param1)
	} else {
		memKeys = memtable.GetKeysInRange(param1, param2)
	}

	found := make([]*ScanningCandidate, 0)
	records = make([]*models.Data, 0)
	err = nil

	//stores newest candidate with min key in current iteration
	var minCandidate *ScanningCandidate
	//stores candidates with the same key as minCandidate, but are older (so we don't consider them in the next iteration)
	var candidates []*ScanningCandidate

	//records needed to fill requested page
	recordsNeeded := pageNumber * pageSize

	//get mmapFiles positioned to start with record that has first occurrence of given prefix
	scannerFiles, err = sstable.getDataFilesForScanning(param1, param2, prefix)

	//keep track of all files to be closed when done
	filesToBeClosed = make([]*ScannerFile, len(scannerFiles))
	copy(filesToBeClosed, scannerFiles)

	if err != nil {
		return
	}

	for uint64(len(found)) < recordsNeeded {
		var filesWithoutPrefix []*ScannerFile

		minCandidate, candidates, filesWithoutPrefix, err = sstable.getCandidates(memKeys, scannerFiles, param1, param2, prefix)

		for _, file := range filesWithoutPrefix {
			scannerFiles = removeFromSlice(scannerFiles, file)
		}

		if minCandidate == nil {
			break
		}

		found = append(found, minCandidate)

		memKeys, scannerFiles = sstable.updateScannerFiles(minCandidate, candidates, memKeys, scannerFiles)
	}

	//there isn't enough keys with given prefix to fill pages before requested page
	if uint64(len(found)) < (pageNumber-1)*pageSize {
		err = sstable.closeScannerFiles(filesToBeClosed)
		return
	}

	records, err = sstable.getFoundRecords(found, memtable, pageNumber, pageSize)

	if err != nil {
		return
	}

	if !iterator {
		err = sstable.closeScannerFiles(filesToBeClosed)
	}

	return
}

func (sstable *SSTable) getCandidates(memKeys []string, scannerFiles []*ScannerFile, param1 string, param2 string, prefix bool) (minCandidate *ScanningCandidate, candidates []*ScanningCandidate, filesWithoutPrefix []*ScannerFile, err error) {
	err = nil
	minCandidate = nil
	candidates = make([]*ScanningCandidate, 0)

	filesWithoutPrefix = make([]*ScannerFile, 0)

	if len(memKeys) > 0 {
		minCandidate = &ScanningCandidate{memKeys[0], nil, 0, 0, 0}
	}
	for _, scannerFile := range scannerFiles {
		var record *models.Data
		var recordSize uint64

		record, recordSize, err = models.Deserialize(scannerFile.mmapFile[scannerFile.offset:], sstable.compression, sstable.encoder)

		if prefix {
			if !strings.HasPrefix(record.Key, param1) {
				filesWithoutPrefix = append(filesWithoutPrefix, scannerFile)
				continue
			}
		} else {
			if record.Key > param2 {
				filesWithoutPrefix = append(filesWithoutPrefix, scannerFile)
				continue
			}
		}

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
		err = file.originalMmapFile.Unmap()
		if err != nil {
			return err
		}
		err = file.osFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
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
