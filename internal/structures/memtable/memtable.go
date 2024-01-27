package memtable

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/b-tree"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hashmap"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

type MemtableData interface {
	Get(key string) (*models.Data, error)
	Put(key string, value []byte, tombstone bool, timestamp uint64) error
	Delete(key string) error
}

func Test(choice int) {
	var data MemtableData

	switch choice {
	case 1:
		data = skiplist.CreateSkipList()
	case 2:
		data = hashmap.CreateHashMap()
	case 3:
		data = btree.CreateBTree()
	}

	//err := data.Put("key1", []byte("value1"), false, 123)
	//if err != nil {
	//	return
	//}
	//err = data.Put("key2", []byte("value2"), false, 124)
	//if err != nil {
	//	return
	//}
	//
	//value, err := data.Get("key1")
	//if err != nil {
	//	fmt.Println(err)
	//}
	//
	//// Instead of having 3 values, for b tree, hashmap and skip list, we just need to dereference one and everything else remains the same
	//// as if we only had one value
	//fmt.Println(fmt.Sprintf("Value: %s, Tombstone: %t, Timestamp: %d", value.Value, value.Tombstone, value.Timestamp))
	//
	//fmt.Println(*value)

	for i := 0; i < 100; i++ {
		err := data.Put(fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)), false, uint64(i))
		if err != nil {
			return
		}
	}

	for i := 0; i < 100; i++ {
		value, err := data.Get(fmt.Sprintf("key%d", i))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(fmt.Sprintf("Value: %s, Tombstone: %t, Timestamp: %d", value.Value, value.Tombstone, value.Timestamp))
	}
}
