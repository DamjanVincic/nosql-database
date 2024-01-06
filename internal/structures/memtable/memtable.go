package memtable

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hashmap"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

type MemtableData interface {
	Get(key string) (*interface{}, error)
	Put(key string, value []byte, tombstone bool, timestamp uint64) error
	Delete(key string) error
}

func Test(choice int) {
	var data MemtableData

	switch choice {
	case 1:
		data = skipList.CreateSkipList()
	case 2:
		data = hashmap.CreateHashMap()
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
	memtableValue := (*value).(*models.MemtableValue)
	fmt.Println(fmt.Sprintf("Value: %s, Tombstone: %t, Timestamp: %d", memtableValue.Value, memtableValue.Tombstone, memtableValue.Timestamp))

	fmt.Println(*value)
}
