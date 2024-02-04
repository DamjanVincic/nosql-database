package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io/fs"
	"os"
	"path/filepath"
)

// sst on lsm lvl is the table we combine with others on next level
func (ss *SSTable) leveled(sstOnLsmLvl fs.DirEntry, lsmLvlDirPath string, lsmLevel uint8) error {
	var dataPrevious mmap.MMap
	var summaryPrevious mmap.MMap
	var toCombine []mmap.MMap
	var data mmap.MMap
	var summaryFilePath string
	var summary mmap.MMap
	var toBeDeleted []string
	if lsmLevel >= ss.maxLevel {
		return errors.New("max number of levels reached")
	}
	// get summary min and max for first table on previous level
	// get its mmap for combineSSTable function
	previousLevelPath := filepath.Join(Path, fmt.Sprintf("%02d", lsmLevel), sstOnLsmLvl.Name())
	// add first sstable from last leveled, it will be combined with others and then deleted
	toBeDeleted = append(toBeDeleted, previousLevelPath)
	previousSSTable, err := os.ReadDir(previousLevelPath)
	if err != nil {
		return err
	}
	// if its multi file
	if len(previousSSTable) != 1 {
		previousLevelPath = filepath.Join(Path, fmt.Sprintf("%02d", lsmLevel), sstOnLsmLvl.Name(), DataFileName)
		previousLevelFile, err := os.OpenFile(previousLevelPath, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		previousMmapFile, err := mmap.Map(previousLevelFile, mmap.RDWR, 0)
		if err != nil {
			return err
		}
		stat, err := previousLevelFile.Stat()
		if err != nil {
			return err
		}
		fileSize := stat.Size()
		dataPrevious, err = mmap.MapRegion(previousLevelFile, int(fileSize), mmap.RDWR, 0, 0)
		if err != nil {
			return err
		}
		copy(dataPrevious, previousMmapFile)
		err = previousMmapFile.Unmap()
		if err != nil {
			return err
		}
		err = previousLevelFile.Close()
		if err != nil {
			return err
		}
		previousLevelPath = filepath.Join(Path, fmt.Sprintf("%02d", lsmLevel), sstOnLsmLvl.Name(), SummaryFileName)
		previousLevelFile, err = os.OpenFile(previousLevelPath, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		previousMmapFile, err = mmap.Map(previousLevelFile, mmap.RDWR, 0)
		if err != nil {
			return err
		}
		stat, err = previousLevelFile.Stat()
		if err != nil {
			return err
		}
		fileSize = stat.Size()
		summaryPrevious, err = mmap.MapRegion(previousLevelFile, int(fileSize), mmap.RDWR, 0, 0)
		if err != nil {
			return err
		}
		copy(summaryPrevious, previousMmapFile)
		err = previousMmapFile.Unmap()
		if err != nil {
			return err
		}
		err = previousLevelFile.Close()
		if err != nil {
			return err
		}
		// if its single file
	} else {
		previousLevelPath = filepath.Join(Path, fmt.Sprintf("%02d", lsmLevel), sstOnLsmLvl.Name(), DataFileName)
		previousLevelFile, err := os.OpenFile(previousLevelPath, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		previousMmapFile, err := mmap.Map(previousLevelFile, mmap.RDWR, 0)
		if err != nil {
			return err
		}
		header := previousMmapFile[:HeaderSize]
		dataSize := binary.BigEndian.Uint64(header[:IndexBlockStart])                      // dataSize
		indexSize := binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart])    // indexSize
		summarySize := binary.BigEndian.Uint64(header[SummaryBlockStart:FilterBlockStart]) // summarySize

		dataStart := uint64(HeaderSize)
		indexStart := dataStart + dataSize
		summaryStart := indexStart + indexSize
		filterStart := summaryStart + summarySize
		stat, err := previousLevelFile.Stat()
		if err != nil {
			return err
		}
		fileSize := stat.Size()
		dataPrevious, err = mmap.MapRegion(previousLevelFile, int(fileSize), mmap.RDWR, 0, 0)
		if err != nil {
			return err
		}
		summaryPrevious, err = mmap.MapRegion(previousLevelFile, int(fileSize), mmap.RDWR, 0, 0)
		if err != nil {
			return err
		}
		copy(summaryPrevious, previousMmapFile[summaryStart:filterStart])
		copy(dataPrevious, previousMmapFile[dataStart:indexStart])
		err = previousMmapFile.Unmap()
		if err != nil {
			return err
		}
		err = previousLevelFile.Close()
		if err != nil {
			return err
		}
	}
	toCombine = append(toCombine, dataPrevious)
	_, minKey, maxKey, _, err := readSummaryHeader(summaryPrevious, ss.compression, ss.encoder)
	lsmLevel++
	lsmLvlDirPathNext := filepath.Join(Path, fmt.Sprintf("%02d", lsmLevel))
	folderEntries, err := os.ReadDir(lsmLvlDirPathNext)
	if os.IsNotExist(err) {
		err := os.Mkdir(lsmLvlDirPathNext, os.ModePerm)
		if err != nil {
			return err
		}
	}
	for _, sst := range folderEntries {
		// get file path for each sstable
		filePath := filepath.Join(lsmLvlDirPathNext, sst.Name())
		// read the dir
		sstFiles, err := os.ReadDir(filePath)
		if os.IsNotExist(err) {
			if err != nil {
				return err
			}
		}
		if len(sstFiles) == 1 {
			singleFilePath := filepath.Join(filePath, SingleFileName)
			singleFile, err := os.OpenFile(singleFilePath, os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			mmapFile, err := mmap.Map(singleFile, mmap.RDWR, 0)
			if err != nil {
				return err
			}
			header := mmapFile[:HeaderSize]
			dataSize := binary.BigEndian.Uint64(header[:IndexBlockStart])                      // dataSize
			indexSize := binary.BigEndian.Uint64(header[IndexBlockStart:SummaryBlockStart])    // indexSize
			summarySize := binary.BigEndian.Uint64(header[SummaryBlockStart:FilterBlockStart]) // summarySize

			dataStart := uint64(HeaderSize)
			indexStart := dataStart + dataSize
			summaryStart := indexStart + indexSize
			filterStart := summaryStart + summarySize
			// we need summary
			stat, err := singleFile.Stat()
			if err != nil {
				return err
			}
			fileSize := stat.Size()
			data, err = mmap.MapRegion(singleFile, int(fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				return err
			}
			summary, err = mmap.MapRegion(singleFile, int(fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				return err
			}
			copy(summary, mmapFile[summaryStart:filterStart])
			copy(data, mmapFile[dataStart:indexStart])
			err = mmapFile.Unmap()
			if err != nil {
				return err
			}
			err = singleFile.Close()
			if err != nil {
				return err
			}
		} else {
			dataFilePath := filepath.Join(filePath, DataFileName)
			summaryFilePath = filepath.Join(filePath, SummaryFileName)
			summaryFile, err := os.OpenFile(summaryFilePath, os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			mmapFile, err := mmap.Map(summaryFile, mmap.RDWR, 0)
			if err != nil {
				return err
			}
			stat, err := summaryFile.Stat()
			if err != nil {
				return err
			}
			fileSize := stat.Size()
			summary, err = mmap.MapRegion(summaryFile, int(fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				return err
			}
			copy(summary, mmapFile)
			err = mmapFile.Unmap()
			if err != nil {
				return err
			}
			err = summaryFile.Close()
			if err != nil {
				return err
			}
			dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			mmapFile, err = mmap.Map(dataFile, mmap.RDWR, 0)
			if err != nil {
				return err
			}
			stat, err = dataFile.Stat()
			if err != nil {
				return err
			}
			fileSize = stat.Size()
			data, err = mmap.MapRegion(summaryFile, int(fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				return err
			}
			copy(data, mmapFile)
			err = mmapFile.Unmap()
			if err != nil {
				return err
			}
			err = dataFile.Close()
			if err != nil {
				return err
			}
		}
		_, summaryMin, summaryMax, _, err := readSummaryHeader(summary, ss.compression, ss.encoder)
		if err != nil {
			return err
		}
		intersectionMin := max(summaryMin, minKey)
		intersectionMax := min(summaryMax, maxKey)

		if intersectionMin <= intersectionMax {
			toCombine = append(toCombine, data)
			toBeDeleted = append(toBeDeleted, filePath)
		}
	}
	err = ss.combineSSTables(toCombine, lsmLevel)
	if err != nil {
		return err
	}
	err = RemoveSSTable(toBeDeleted)
	if err != nil {
		return err
	}
	return nil
}
