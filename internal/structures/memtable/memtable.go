package memtable

import (
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hashmap"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

const (
	maxPartitions = 3 //max number of MemtableData instances
	maxEntries    = 3 //max number of entries in one MemtableData instance
	dataStructure = 3 //implementation od MemtableData to be used (1 for skipList, 2 for BTree, 3 for hashMap)
)

type MemtableData interface {
	Get(key string) (*models.Data, error)
	GetSorted() []*models.MemEntry
	Put(key string, value []byte, tombstone bool, timestamp uint64) error
	Delete(key string) error
	Size() int
}

type Memtable struct {
	partitions       []MemtableData //read-only partitions
	currentPartition MemtableData   //read-write partition
}

func NewMemtable() *Memtable {
	memtable := Memtable{partitions: []MemtableData{}, currentPartition: nil}
	memtable.makePartition()
	return &memtable
}

// creates new partition making it currentPartition, appends previous currentPartition to partitions, deletes oldest partition if needed
func (memtable *Memtable) makePartition() {
	var newPartition MemtableData

	//creating newPartition with given structure
	switch dataStructure {
	case 1:
		newPartition = skiplist.CreateSkipList()
	case 3:
		newPartition = hashmap.CreateHashMap()
	}

	//if there is no currentPartition (during initialization) only currentPartition is created
	if memtable.currentPartition == nil {
		memtable.currentPartition = newPartition
		return
	}

	//delete oldest partition if needed
	if len(memtable.partitions)+1 >= maxPartitions {
		memtable.partitions = memtable.partitions[1:]
	}

	//append current partition to read-only partitions
	memtable.partitions = append(memtable.partitions, memtable.currentPartition)
	memtable.currentPartition = newPartition
}

// puts values in currentPartition, creates new partition if needed, flushes oldest partition if needed
func (memtable *Memtable) Put(key string, value []byte, timestamp uint64, tombstone bool) (toFlush []*models.MemEntry, err error) {
	toFlush = nil
	err = nil

	err = memtable.currentPartition.Put(key, value, tombstone, timestamp)
	if err != nil {
		return
	}

	if memtable.currentPartition.Size() >= maxEntries {
		if len(memtable.partitions)+1 >= maxPartitions {
			toFlush = memtable.partitions[0].GetSorted()
		}
		memtable.makePartition()
	}
	return
}

// returns newest value with given key
func (memtable *Memtable) Get(key string) *models.Data {
	var data *models.Data
	data = nil
	var err error
	err = nil

	data, err = memtable.currentPartition.Get(key)
	if err == nil {
		return data
	}

	for i := len(memtable.partitions) - 1; i >= 0; i-- {
		data, err = memtable.partitions[i].Get(key)
		if err == nil {
			return data
		}
	}
	return nil
}
