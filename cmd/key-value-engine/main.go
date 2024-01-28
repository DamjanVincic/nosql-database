package main

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/sstable"
	"time"
)

func main() {
	memEntries := make([]*sstable.MemEntry, 0, 50)

	for i := 0; i < 26; i++ {
		key := string('A' + i)
		fmt.Println(key)
		value := &models.Data{
			Value:     []byte(fmt.Sprintf("Value%d", i)),
			Tombstone: false,
			Timestamp: uint64(time.Now().UnixNano()),
		}

		entry := &sstable.MemEntry{
			Key:   key,
			Value: value,
		}

		memEntries = append(memEntries, entry)
	}
	ss, err := sstable.NewSSTable(memEntries, false)
	fmt.Println(ss)
	//err = lsm.CompactLogic(1, ss)
	if err != nil {
		fmt.Println("greska")
	}
	for i := 0; i < 26; i++ {
		key := string('A' + i)
		value, err := sstable.Get(key)
		if err != nil {
			fmt.Println("greska")
		} else {
			fmt.Println("key:", key, ",value:", value.Value, ",tombstone:", value.Tombstone, ",timestamp:", value.Timestamp)
		}
	}
}
