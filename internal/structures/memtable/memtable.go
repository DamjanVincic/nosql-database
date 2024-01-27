package memtable

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/b-tree"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hashmap"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

type MemtableData interface {
	Get(key string) *models.Data
	Put(key string, value []byte, tombstone bool, timestamp uint64)
	Delete(key string)
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

	for i := 0; i < 100; i++ {
		data.Put(fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)), false, uint64(i))
	}

	for i := 0; i < 103; i++ {
		value := data.Get(fmt.Sprintf("key%d", i))
		if value == nil {
			fmt.Println(value)
		} else {
			fmt.Println(fmt.Sprintf("Key: key%d, Value: %s, Tombstone: %t, Timestamp: %d", i, value.Value, value.Tombstone, value.Timestamp))
		}
	}
}
