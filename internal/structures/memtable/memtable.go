package memtable

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

const (
	maxPartitions = 3 //max number of MemtableData instances
	maxEntries    = 3 //max number of entries in one MemtableData instance
	dataStructure = 1 //implementation od MemtableData to be used (1 for skipList, 2 for BTree, 3 for hashMap)
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

func newMemtable() *Memtable {
	memtable := Memtable{partitions: []MemtableData{}, currentPartition: nil}
	memtable.makePartition()
	return &memtable
}

// creates new partition making it currentPartition, appends previous currentPartition to partitions, deletes oldest partition if needed
func (memtable *Memtable) makePartition() {
	var newPartition MemtableData
	switch dataStructure {
	case 1:
		newPartition = skiplist.CreateSkipList()
	}
	if memtable.currentPartition == nil {
		memtable.currentPartition = newPartition
		return
	}
	if len(memtable.partitions)-1 >= maxPartitions {
		memtable.partitions = memtable.partitions[1:]
	}
	memtable.partitions = append(memtable.partitions, memtable.currentPartition)
	memtable.currentPartition = newPartition
}

func (memtable *Memtable) Put(key string, value []byte, timestamp uint64, tombstone bool) (toFlush []*models.MemEntry, err error) {
	toFlush = nil
	err = nil

	err = memtable.currentPartition.Put(key, value, tombstone, timestamp)
	if err != nil {
		return
	}

	if memtable.currentPartition.Size() >= maxEntries {
		if len(memtable.partitions)-1 >= maxPartitions {
			toFlush = memtable.partitions[maxPartitions-1].GetSorted()
		}
		memtable.makePartition()
	}
	return
}

func Test(choice int) {
	var data MemtableData

	switch choice {
	case 1:
		data = skiplist.CreateSkipList()
		//case 2:
		//	data = hashmap.CreateHashMap()
	}

	err := data.Put("key1", []byte("value1"), false, 123)
	if err != nil {
		return
	}
	err = data.Put("key2", []byte("value2"), false, 124)
	if err != nil {
		return
	}

	value, err := data.Get("key1")
	if err != nil {
		fmt.Println(err)
	}

	// Instead of having 3 values, for b tree, hashmap and skip list, we just need to dereference one and everything else remains the same
	// as if we only had one value
	fmt.Println(fmt.Sprintf("Value: %s, Tombstone: %t, Timestamp: %d", value.Value, value.Tombstone, value.Timestamp))

	fmt.Println(*value)
}
