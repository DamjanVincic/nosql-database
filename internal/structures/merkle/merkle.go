package merkle

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/structures/skipList"
	"math"
)

type MerkleTree struct {
	root *Node
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func createDataForNode(key string, data skipList.SkipListValue) []byte {
	value := data.Value
	tombstone := data.Tombstone
	timestamp := data.Timestamp

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

func createNewNode(key string, value skipList.SkipListValue) *Node {
	newData := createDataForNode(key, value)
	return &Node{left: nil, right: nil, data: Hash(newData)}
}
func isWholeNumber(n float64) bool {
	return math.Mod(n, 1) == 0
}
func Hash(data []byte) []byte {
	hashed := sha1.Sum(data)
	return hashed[:]
}
func CreateMerkleTree(allData map[string]skipList.SkipListValue) *MerkleTree {
	var nodes []*Node
	var merkleTree MerkleTree

	// creating all the end nodes
	for key, data := range allData {
		node := createNewNode(key, data)
		nodes = append(nodes, node)
	}

	// if number of nodes is not 2**n add empty nodes
	n := math.Log2(float64(len(nodes)))
	fmt.Println(len(nodes))
	if !isWholeNumber(n) {
		for i := 0; i < int(math.Pow(2, math.Ceil(n)))-len(nodes); i++ {
			nodes = append(nodes, createNewNode("", skipList.SkipListValue{Value: []byte{}, Tombstone: true, Timestamp: 0}))
		}
	}
	fmt.Println(len(nodes))

	for len(nodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(nodes); i += 2 {
			newNode := &Node{
				left:  nodes[i],
				right: nodes[i+1],
				data:  Hash(append(nodes[i].data, nodes[i+1].data...)),
			}
			newLevel = append(newLevel, newNode)
		}
		nodes = newLevel
	}

	merkleTree.root = nodes[len(nodes)-1]
	return &merkleTree
}
