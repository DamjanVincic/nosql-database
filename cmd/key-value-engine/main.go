package main

import (
	"fmt"
	skipList "github.com/DamjanVincic/key-value-engine/internal/structures/skiplist"
	"github.com/DamjanVincic/key-value-engine/internal/structures/sstable"
)

func main() {
	memEntries := getMemEntry()
	sstable.NewSSTable2(memEntries, 50)

}
func getMemEntry() []sstable.MemEntry {
	sampleSkipListValue := &skipList.SkipListValue{
		Value:     []byte("example value"),
		Tombstone: false,
		Timestamp: 123456789,
	}

	// Create 50 MemEntry instances with the same SkipListValue
	memEntries := make([]sstable.MemEntry, 50)
	for i := 0; i < 50; i++ {
		memEntries[i] = sstable.MemEntry{
			Key:   fmt.Sprintf("Key%d", i),
			Value: sampleSkipListValue,
		}
	}

	return memEntries
}
