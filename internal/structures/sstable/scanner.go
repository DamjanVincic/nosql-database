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
	found           []*ScanningCandidate
	currentIdx      uint64
}

type RangeIterator struct {
	memKeys         []string
	scannerFiles    []*ScannerFile
	filesToBeClosed []*ScannerFile
	sstable         *SSTable
	memtable        *memtable.Memtable
	min             string
	max             string
	found           []*ScanningCandidate
	currentIdx      uint64
}

// returns mmaped data files of all sstables that contain records with given conditions
// files are positioned to the first record in which conditions are satisfied
// conditions can be given in prefix or range
// if prefix is true, param1 is given prefix if prefix is false, param1 is min and param2 is max of the range
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

				dataStart = uint64(HeaderSize)
				indexStart = dataStart + dataSize
				summaryStart = indexStart + indexSize
				filterStart = summaryStart + summarySize
			}

			//open summary file in multifile, or position to summary block in singlefile
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

			//check if there could be records with given conditions in this sstable
			prefix_not_possible := (minKey > param1 && !strings.HasPrefix(minKey, param1)) || (maxKey < param1)
			range_not_possible := maxKey < param1 || minKey > param2

			//if there can't be records with given conditions, close summary file and move on to the next sstable
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
			//check if the first key meets conditions, if it does, repositioning isn't needed
			if (prefix && strings.HasPrefix(minKey, param1)) || (!prefix && minKeyIsInRange) {
				//close summary file
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

				//open data file
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

				//append data file to found files
				if sstDirSize == 1 {
					files = append(files, &ScannerFile{mmapFile: data, originalMmapFile: mmapSingleFile, osFile: currentFile, offset: 0})
				} else {
					files = append(files, &ScannerFile{mmapFile: data, originalMmapFile: data, osFile: currentFile, offset: 0})
				}

			} else {
				//find position of closest key in summary and get it's offset in index
				indexOffset, summaryThinningConst, err := readSummaryFromFile(summary, param1, sstable.compression, sstable.encoder)

				if err != nil {
					return nil, err
				}

				//close summary file
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

				//open index file
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

				//find the closest record in index, and find it's offset in data
				dataOffset, _, err := readIndexFromFile(index, summaryThinningConst, param1, indexOffset, sstable.compression, sstable.encoder)

				if err != nil {
					return nil, err
				}

				//close index file
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

				//open data file
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
				//position data file to the first record that meets conditions
				if prefix {
					positionedFile, err = findFirstWithPrefix(param1, data[dataOffset:], sstable.compression, sstable.encoder)
				} else {
					positionedFile, err = findFirstInRange(param1, param2, data[dataOffset:], sstable.compression, sstable.encoder)
				}

				if err != nil {
					return nil, err
				}

				//append positioned file to found files
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

// finds first record with given prefix in given file, returns the same file repositioned to said record
func findFirstWithPrefix(prefix string, mmapFile mmap.MMap, compression bool, encoder *keyencoder.KeyEncoder) (mmap.MMap, error) {
	var offset uint64
	for {
		//deserialize record on current offset
		record, recordSize, err := models.Deserialize(mmapFile[offset:], compression, encoder)
		if err != nil {
			return nil, err
		}
		//if current record has given prefix, return file repositioned to current offset
		if strings.HasPrefix(record.Key, prefix) {
			return mmapFile[offset:], nil
		}
		//move offset to next record
		offset += recordSize
		//if enf of file is reached, there are no records with given prefix
		if uint64(len(mmapFile)) <= offset {
			return nil, nil
		}
	}
}

// finds first record in given range in given file, returns the same file repositioned to said record
func findFirstInRange(min string, max string, mmapFile mmap.MMap, compression bool, encoder *keyencoder.KeyEncoder) (mmap.MMap, error) {
	var offset uint64
	for {
		//deserialize record on current offset
		record, recordSize, err := models.Deserialize(mmapFile[offset:], compression, encoder)
		if err != nil {
			return nil, err
		}
		//if current record is in given range, return file repositioned to current offset
		if record.Key >= min && record.Key <= max {
			return mmapFile[offset:], nil
		}
		//move offset to next record
		offset += recordSize
		//if enf of file is reached, there are no records in given range
		if uint64(len(mmapFile)) <= offset {
			return nil, nil
		}
	}
}

// makes instance iterator for iterating trough records with given prefix, returns first record found
func (sstable *SSTable) NewPrefixIterator(prefix string, memtable *memtable.Memtable) (*PrefixIterator, *models.Data, error) {
	records, err, memKeys, scannerFiles, filesToBeClosed, found := sstable.scanWithConditions(prefix, "", true, 1, 1, memtable, true)
	if err != nil {
		return nil, nil, err
	}
	var record *models.Data
	if len(records) > 0 {
		record = records[0]
	}
	return &PrefixIterator{memKeys: memKeys, scannerFiles: scannerFiles, filesToBeClosed: filesToBeClosed, sstable: sstable, memtable: memtable, prefix: prefix, found: found, currentIdx: 0}, record, err
}

// returns next record with given prefix
func (iterator *PrefixIterator) Next() (*models.Data, error) {
	var minCandidate *ScanningCandidate
	var candidates []*ScanningCandidate
	var filesWithoutPrefix []*ScannerFile
	var err error

	alreadyFound := false

	iterator.currentIdx++

	//check if the record was already found
	if iterator.currentIdx < uint64(len(iterator.found)) {
		alreadyFound = true
		minCandidate = iterator.found[iterator.currentIdx]
	} else {
		//find min key with given prefix and all records containing it
		minCandidate, candidates, filesWithoutPrefix, err = iterator.sstable.getCandidates(iterator.memKeys, iterator.scannerFiles, iterator.prefix, "", true)

		if err != nil {
			return nil, err
		}

		//remove files that have no more records with given prefix, so they are not considered in the next iteration
		for _, file := range filesWithoutPrefix {
			iterator.scannerFiles = removeFromSlice(iterator.scannerFiles, file)
		}

		if minCandidate == nil {
			return nil, errors.New("no records left")
		}

		iterator.found = append(iterator.found, minCandidate)
	}

	var record *models.Data

	//get found record
	if minCandidate.file == nil {
		record = iterator.memtable.Get(minCandidate.key)
	} else {
		record, _, err = models.Deserialize(minCandidate.file.mmapFile[minCandidate.offset:], iterator.sstable.compression, iterator.sstable.encoder)
		if err != nil {
			return nil, err
		}
	}

	if !alreadyFound {
		//reposition all files so this key is not considered in next iteration
		iterator.memKeys, iterator.scannerFiles = iterator.sstable.updateScannerFiles(minCandidate, candidates, iterator.memKeys, iterator.scannerFiles)
	}

	return record, nil
}

// returns previous record with given prefix
func (iterator *PrefixIterator) Previous() (*models.Data, error) {
	if iterator.currentIdx == 0 {
		return nil, errors.New("no records left")
	}

	iterator.currentIdx--

	foundCandidate := iterator.found[iterator.currentIdx]

	var record *models.Data
	var err error

	//get found record
	if foundCandidate.file == nil {
		record = iterator.memtable.Get(foundCandidate.key)
	} else {
		record, _, err = models.Deserialize(foundCandidate.file.mmapFile[foundCandidate.offset:], iterator.sstable.compression, iterator.sstable.encoder)
		if err != nil {
			return nil, err
		}
	}

	return record, nil
}

// closes all files used in scanning
func (iterator *PrefixIterator) Stop() error {
	return iterator.sstable.closeScannerFiles(iterator.filesToBeClosed)
}

// finds all keys with given prefix that are on given page
func (sstable *SSTable) PrefixScan(prefix string, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable) (records []*models.Data, err error) {
	records, err, _, _, _, _ = sstable.scanWithConditions(prefix, "", true, pageNumber, pageSize, memtable, false)

	if err != nil {
		records = nil
	}
	return
}

// makes instance iterator for iterating trough records in given range, returns first record found
func (sstable *SSTable) NewRangeIterator(min string, max string, memtable *memtable.Memtable) (*RangeIterator, *models.Data, error) {
	if min > max {
		return nil, nil, errors.New("invalid range given")
	}

	records, err, memKeys, scannerFiles, filesToBeClosed, found := sstable.scanWithConditions(min, max, false, 1, 1, memtable, true)
	if err != nil {
		return nil, nil, err
	}
	var record *models.Data
	if len(records) > 0 {
		record = records[0]
	}
	return &RangeIterator{memKeys: memKeys, scannerFiles: scannerFiles, filesToBeClosed: filesToBeClosed, sstable: sstable, memtable: memtable, min: min, max: max}, record, err
}

func (iterator *RangeIterator) Next() (*models.Data, error) {

	//find min key in given range and all records containing it
	minCandidate, candidates, filesWithoutMatches, err := iterator.sstable.getCandidates(iterator.memKeys, iterator.scannerFiles, iterator.min, iterator.max, false)

	if err != nil {
		return nil, err
	}

	//remove files that have no more records in given range, so they are not considered in the next iteration
	for _, file := range filesWithoutMatches {
		iterator.scannerFiles = removeFromSlice(iterator.scannerFiles, file)
	}

	if minCandidate == nil {
		return nil, errors.New("no records left")
	}

	var record *models.Data

	//get found record
	if minCandidate.file == nil {
		record = iterator.memtable.Get(minCandidate.key)
	} else {
		record, _, err = models.Deserialize(minCandidate.file.mmapFile[minCandidate.offset:], iterator.sstable.compression, iterator.sstable.encoder)
		if err != nil {
			return nil, err
		}
	}

	//reposition all files so this key is not considered in next iteration
	iterator.memKeys, iterator.scannerFiles = iterator.sstable.updateScannerFiles(minCandidate, candidates, iterator.memKeys, iterator.scannerFiles)

	return record, nil
}

// closes all files used in scanning
func (iterator *RangeIterator) Stop() error {
	return iterator.sstable.closeScannerFiles(iterator.filesToBeClosed)
}

// finds all keys in given range that are on given page
func (sstable *SSTable) RangeScan(min string, max string, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable) (records []*models.Data, err error) {
	if min > max {
		return nil, errors.New("invalid range given")
	}

	records, err, _, _, _ = sstable.scanWithConditions(min, max, false, pageNumber, pageSize, memtable, false)

	if err != nil {
		records = nil
	}
	return
}

// returns all records with given conditions on given page and memKeys,scannerFiles,filesToBeClosed,found (needed for iterators)
// conditions can be given in prefix or range
// if prefix is true, param1 is given prefix if prefix is false, param1 is min and param2 is max of the range
func (sstable *SSTable) scanWithConditions(param1 string, param2 string, prefix bool, pageNumber uint64, pageSize uint64, memtable *memtable.Memtable, iterator bool) (records []*models.Data, err error, memKeys []string, scannerFiles []*ScannerFile, filesToBeClosed []*ScannerFile, found []*ScanningCandidate) {
	//get keys from memtable
	if prefix {
		memKeys = memtable.GetKeysWithPrefix(param1)
	} else {
		memKeys = memtable.GetKeysInRange(param1, param2)
	}

	found = make([]*ScanningCandidate, 0)
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

	//in each iteration min key is found and all files containing it
	for uint64(len(found)) < recordsNeeded {
		var filesWithoutMatches []*ScannerFile

		//find candidates
		minCandidate, candidates, filesWithoutMatches, err = sstable.getCandidates(memKeys, scannerFiles, param1, param2, prefix)

		//remove files that have no more matches, so they are not considered in next iteration
		for _, file := range filesWithoutMatches {
			scannerFiles = removeFromSlice(scannerFiles, file)
		}

		if minCandidate == nil {
			break
		}

		found = append(found, minCandidate)

		//update offsets in all files
		memKeys, scannerFiles = sstable.updateScannerFiles(minCandidate, candidates, memKeys, scannerFiles)
	}

	//there isn't enough keys with given prefix to fill pages before requested page
	if uint64(len(found)) < (pageNumber-1)*pageSize {
		if !iterator {
			err = sstable.closeScannerFiles(filesToBeClosed)
		}
		return
	}

	//get found records
	records, err = sstable.getFoundRecords(found, memtable, pageNumber, pageSize)

	if err != nil {
		return
	}

	if !iterator {
		err = sstable.closeScannerFiles(filesToBeClosed)
	}

	return
}

// get all candidates with the smallest key in all files
func (sstable *SSTable) getCandidates(memKeys []string, scannerFiles []*ScannerFile, param1 string, param2 string, prefix bool) (minCandidate *ScanningCandidate, candidates []*ScanningCandidate, filesWithoutMatches []*ScannerFile, err error) {
	err = nil
	minCandidate = nil
	candidates = make([]*ScanningCandidate, 0)

	filesWithoutMatches = make([]*ScannerFile, 0)

	//keys from memtable are considered first, because they are newest
	if len(memKeys) > 0 {
		minCandidate = &ScanningCandidate{memKeys[0], nil, 0, 0, 0}
	}

	//go through all files
	for _, scannerFile := range scannerFiles {
		var record *models.Data
		var recordSize uint64

		record, recordSize, err = models.Deserialize(scannerFile.mmapFile[scannerFile.offset:], sstable.compression, sstable.encoder)

		//when first record that doesn't meet conditions is loaded, there are no more records that meet conditions
		if prefix {
			if !strings.HasPrefix(record.Key, param1) {
				filesWithoutMatches = append(filesWithoutMatches, scannerFile)
				continue
			}
		} else {
			if record.Key > param2 {
				filesWithoutMatches = append(filesWithoutMatches, scannerFile)
				continue
			}
		}

		if err != nil {
			return
		}

		//if there is no minCandidate, comparison isn't needed
		if minCandidate == nil {
			minCandidate = &ScanningCandidate{record.Key, scannerFile, scannerFile.offset, recordSize, record.Timestamp}
			continue
		}
		//if current key is smaller than min key it becomes min key
		if minCandidate.key > record.Key {
			minCandidate = &ScanningCandidate{record.Key, scannerFile, scannerFile.offset, recordSize, record.Timestamp}
			candidates = make([]*ScanningCandidate, 0)
			continue
		}
		//if current key is equal to min key, newer one becomes min key
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

// update offsets on all files
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

// deserialize records if they are in sstable, get them if they are in memtable
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
