package merkle

import (
	"encoding/binary"
)

type MerkleTree struct {
	root *Node
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func createDataForNode(key string, value []byte, tombstone bool, timestamp uint64) []byte {
	keyBytes := []byte(key)
	result := keyBytes

	result = append(result, value...)

	tombstoneByte := boolToByte(tombstone)
	result = append(result, tombstoneByte...)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	result = append(result, timestampBytes...)

	return result
}

func boolToByte(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func createNewNode(key string, value []byte, tombstone bool, timestamp uint64) *Node {
	newData := createDataForNode(key, value, tombstone, timestamp)
	return &Node{left: nil, right: nil, data: newData[:]}
}
