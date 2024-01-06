package merkle

import (
	"encoding/binary"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
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
	hashFunc := hash.CreateHashFunctions(1)[0]
	values, _ := hashFunc.Hash(newData)
	valuesBinary := make([]byte, 8)
	binary.BigEndian.PutUint64(valuesBinary, values)
	return &Node{left: nil, right: nil, data: valuesBinary}
}
func isWholeNumber(n float64) bool {
	return math.Mod(n, 1) == 0
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
	degree := math.Ceil(n)
	fmt.Println(len(nodes))
	fmt.Println(degree)
	if !isWholeNumber(n) {
		targetSize := int(math.Pow(2, degree))
		for i := len(nodes); i < targetSize; i++ {
			nodes = append(nodes, createNewNode("", skipList.SkipListValue{Value: []byte{}, Tombstone: true, Timestamp: 0}))
		}
	}
	fmt.Println(len(nodes))

	for len(nodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(nodes); i += 2 {
			hashFunc := hash.CreateHashFunctions(1)[0]
			values, _ := hashFunc.Hash(append(nodes[i].data, nodes[i+1].data...))
			valuesBinary := make([]byte, 8)
			binary.BigEndian.PutUint64(valuesBinary, values)
			newNode := &Node{
				left:  nodes[i],
				right: nodes[i+1],
				data:  valuesBinary,
			}
			newLevel = append(newLevel, newNode)
		}
		nodes = newLevel
	}

	merkleTree.root = nodes[len(nodes)-1]
	return &merkleTree
}
