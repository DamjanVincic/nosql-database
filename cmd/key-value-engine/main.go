package main

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/structures/merkle"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skipList"
)

func main() {
	key1 := "1"
	key2 := "2"
	key3 := "3"
	key4 := "4"
	key5 := "5"
	value1 := skipList.SkipListValue{[]byte{1}, true, 1}
	value2 := skipList.SkipListValue{[]byte{2}, true, 2}
	value3 := skipList.SkipListValue{[]byte{3}, true, 3}
	value4 := skipList.SkipListValue{[]byte{4}, true, 4}
	value5 := skipList.SkipListValue{[]byte{5}, true, 5}

	dataDict := map[string]skipList.SkipListValue{
		key1: value1,
		key2: value2,
		key3: value3,
		key4: value4,
		key5: value5,
	}
	tree := merkle.CreateMerkleTree(dataDict)
	fmt.Println(tree)
}
