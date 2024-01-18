package sstable

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
)

const (
	// number of bytes used for writting the size of hashWithSeed in file
	HashWithSeedSizeSize = 8
	// number of bytes stored in Node data after hashing
	HashedNodesSize = 8
)

type MerkleTree struct {
	Root         *Node
	HashWithSeed hash.HashWithSeed
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

/*
create data for node
get binary value of value tombtone and timestamp
append it to result and return serialized data
*/
func createDataForNode(key string, data *models.Data) []byte {
	value := data.Value
	tombstone := data.Tombstone
	timestamp := data.Timestamp
	var tombstoneByte []byte
	if tombstone {
		tombstoneByte = []byte{1}
	} else {
		tombstoneByte = []byte{0}
	}
	keyBytes := []byte(key)
	result := keyBytes

	result = append(result, value...)

	result = append(result, tombstoneByte...)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	result = append(result, timestampBytes...)

	return result
}

/*
get binary data and hash function and return new node with no children and hashed values
*/
func (merkleTree *MerkleTree) createNewNode(key string, value *models.Data) (*Node, error) {
	newData := createDataForNode(key, value)
	values, err := merkleTree.HashWithSeed.Hash(newData)
	if err != nil {
		return nil, err
	}
	valuesBinary := make([]byte, 8)
	binary.BigEndian.PutUint64(valuesBinary, values)
	return &Node{left: nil, right: nil, data: valuesBinary}, nil
}

/*
since merkle tree is build from bottom up we need all data as leafs
if number of leafs is not 2**n we need to add empty nodes
there hash wont change anything
*/
func CreateMerkleTree(allData []*MemEntry) (*MerkleTree, error) {
	var nodes []*Node
	var merkleTree MerkleTree
	merkleTree.HashWithSeed = hash.CreateHashFunctions(1)[0]

	// creating all the end nodes
	for _, memEntry := range allData {
		node, err := merkleTree.createNewNode(memEntry.Key, memEntry.Value)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	// if number of nodes is not 2**n add empty nodes
	n := math.Log2(float64(len(nodes)))
	degree := math.Ceil(n)
	if math.Mod(n, 1) != 0 { // check if hole number
		targetSize := int(math.Pow(2, degree)) // ex. n is 2.3 then degree is 3 and we need 8 nodes since 2**3
		for i := len(nodes); i < targetSize; i++ {
			// add number of empty nodes that is needed
			node, err := merkleTree.createNewNode("", &models.Data{Value: []byte{}, Tombstone: false, Timestamp: 0})
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
	}

	for len(nodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(nodes); i += 2 {
			// add two array together and hash
			values, err := merkleTree.HashWithSeed.Hash(append(nodes[i].data, nodes[i+1].data...))
			if err != nil {
				return nil, err
			}
			valuesBinary := make([]byte, 8)
			binary.BigEndian.PutUint64(valuesBinary, values)
			// create new node with nodes at index i and i + 1, parent-left and right child
			// add new hash value of two nodes as data of new node
			newNode := &Node{
				left:  nodes[i],
				right: nodes[i+1],
				data:  valuesBinary,
			}
			newLevel = append(newLevel, newNode)
		}
		nodes = newLevel
	}

	merkleTree.Root = nodes[len(nodes)-1]
	return &merkleTree, nil
}
func (tree *MerkleTree) Serialize() []byte {
	//add size of hashWithSeed to byte array
	bytes := make([]byte, HashWithSeedSizeSize)
	serializedHash := hash.Serialize([]hash.HashWithSeed{tree.HashWithSeed})
	hashFuncSize := uint64(len(serializedHash))
	binary.BigEndian.PutUint64(bytes[:HashWithSeedSizeSize], hashFuncSize)
	//append hashWithSeed
	bytes = append(bytes, serializedHash...)
	bytes = append(bytes, MerkleBFS(tree.Root)...)
	return bytes
}

// BFS traversal on a binary tree; creates list of nodes
func MerkleBFS(root *Node) []byte {
	if root == nil {
		return []byte{}
	}
	result := make([]byte, 0)
	nodes := []*Node{root}
	for len(nodes) > 0 {
		current := nodes[0] // get next in line
		nodes = nodes[1:]
		result = append(result, current.data...)
		if current.left != nil {
			nodes = append(nodes, current.left)
		}
		if current.right != nil {
			nodes = append(nodes, current.right)
		}
	}
	return result
}

func DeserializeMerkle(data []byte) (*MerkleTree, error) {
	//deserialize hash func
	hashWithSeedSize := binary.BigEndian.Uint64(data[:HashWithSeedSizeSize])
	hashWithSeed := hash.Deserialize(data[HashWithSeedSizeSize : HashWithSeedSizeSize+hashWithSeedSize])[0]
	//deserialize nodes
	var nodes []byte
	for offset := HashWithSeedSizeSize + hashWithSeedSize; offset < uint64(len(data)); offset += HashedNodesSize {
		nodes = append(nodes, data[offset:offset+HashedNodesSize]...)
	}
	//constructs merkle from list of nodes and returns root node
	root := binaryTree(nodes, 0)
	return &MerkleTree{
		Root:         root,
		HashWithSeed: hashWithSeed,
	}, nil
}

// recursive function used to construct a binary tree from a given byte slice data and an initial index index
// returns Root Node
func binaryTree(data []byte, index int) *Node {
	if index*HashedNodesSize+HashedNodesSize <= len(data) {
		node := &Node{
			data:  data[index*HashedNodesSize : index*HashedNodesSize+HashedNodesSize],
			left:  nil,
			right: nil,
		}
		node.left = binaryTree(data, 2*index+1)
		node.right = binaryTree(data, 2*index+2)
		return node
	}
	return nil
}

func (merkleTree *MerkleTree) IsEqualTo(comparableTree *MerkleTree) bool {

	//compare hash functions (must serialize them)
	serializedHash := hash.Serialize([]hash.HashWithSeed{merkleTree.HashWithSeed})
	serializedComparableHash := hash.Serialize([]hash.HashWithSeed{comparableTree.HashWithSeed})
	if !bytes.Equal(serializedHash, serializedComparableHash) {
		return false
	}

	// compare roots
	root := merkleTree.Root.data
	comparableRoot := comparableTree.Root.data

	if !bytes.Equal(root, comparableRoot) {
		return false
	}

	return true
}