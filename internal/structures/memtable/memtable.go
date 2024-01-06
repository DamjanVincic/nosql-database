package memtable

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
)

type MemtableData interface {
	Get(key string) (*interface{}, error)
	Put(key string, value []byte, tombstone bool, timestamp uint64) error
	Delete(key string) error
}

func Test() {
	var data MemtableData = skipList.CreateSkipList()
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
	fmt.Println(*value)
}
