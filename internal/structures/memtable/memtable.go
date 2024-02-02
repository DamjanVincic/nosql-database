package memtable

import (
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/b-tree"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hashmap"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

type MemtableData interface {
	Get(key string) *models.Data
	GetSorted() []*models.Data
	Put(key string, value []byte, tombstone bool, timestamp uint64)
	Delete(key string)
	Size() int
}

type Memtable struct {
	partitions       []MemtableData //read-only partitions
	currentPartition MemtableData   //read-write partition

	maxEntries    uint64 // max number of entries in one MemtableData instance
	dataStructure string // implementation od MemtableData to be used (skipList, BTree, or hashMap)
	maxPartitions uint64 // max number of MemtableData instances
}

func NewMemtable(maxEntries uint64, dataStructure string, maxPartitions uint64) *Memtable {
	memtable := Memtable{
		partitions:       []MemtableData{},
		currentPartition: nil,
		maxEntries:       maxEntries,
		dataStructure:    dataStructure,
		maxPartitions:    maxPartitions,
	}
	memtable.makePartition()
	return &memtable
}

// creates new partition making it currentPartition, appends previous currentPartition to partitions, deletes oldest partition if needed
func (memtable *Memtable) makePartition() {
	var newPartition MemtableData

	//creating newPartition with given structure
	switch memtable.dataStructure {
	case "skiplist":
		newPartition = skiplist.CreateSkipList()
	case "btree":
		newPartition = btree.CreateBTree()
	case "hashmap":
		newPartition = hashmap.CreateHashMap()
	}

	//if there is no currentPartition (during initialization) only currentPartition is created
	if memtable.currentPartition == nil || memtable.maxPartitions == 1 {
		memtable.currentPartition = newPartition
		return
	}

	//delete oldest partition if needed
	if uint64(len(memtable.partitions)+1) >= memtable.maxPartitions {
		memtable.partitions = memtable.partitions[1:]
	}

	//append current partition to read-only partitions
	memtable.partitions = append(memtable.partitions, memtable.currentPartition)
	memtable.currentPartition = newPartition
}

// puts values in currentPartition, creates new partition if needed, flushes oldest partition if needed
func (memtable *Memtable) Put(key string, value []byte, timestamp uint64, tombstone bool) (toFlush []*models.Data) {
	toFlush = nil

	memtable.currentPartition.Put(key, value, tombstone, timestamp)

	if uint64(memtable.currentPartition.Size()) >= memtable.maxEntries {
		if uint64(len(memtable.partitions)+1) >= memtable.maxPartitions {
			if len(memtable.partitions) == 0 {
				toFlush = memtable.currentPartition.GetSorted()
			} else {
				toFlush = memtable.partitions[0].GetSorted()
			}
		}
		memtable.makePartition()
	}
	return
}

// returns newest value with given key
func (memtable *Memtable) Get(key string) *models.Data {
	var data *models.Data

	data = memtable.currentPartition.Get(key)
	if data != nil {
		return data
	}

	for i := len(memtable.partitions) - 1; i >= 0; i-- {
		data = memtable.partitions[i].Get(key)
		if data != nil {
			return data
		}
	}
	return nil
}
