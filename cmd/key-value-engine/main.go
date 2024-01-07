package main

import (
	"fmt"
	btree "github.com/DamjanVincic/key-value-engine/internal/structures/b-tree"
)

func main() {
	bTree := btree.CreateBTree()

	// Adding 10 elements to the B-tree
	keys := []string{"apple", "banana", "cherry", "date", "fig", "grape", "kiwi", "lemon", "mango", "orange"}
	for _, key := range keys {
		bTree.Put(key, []byte("value"), false, 0)
	}

	// Get and print the sorted list of keys
	sortedKeys := bTree.GetSortedList()
	fmt.Println("Sorted Keys:", sortedKeys)
}
