package sstable

import (
	"encoding/binary"
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
