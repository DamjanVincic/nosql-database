package merkle

import (
	"encoding/binary"
	"math"
	"os"

	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
	"github.com/edsrzf/mmap-go"
)

const (
	// number of bytes used for writting the size of hashWithSeed in file
	HashWithSeedSizeSize = 8
)

type MerkleTree struct {
	Root         *Node
	hashWithSeed []hash.HashWithSeed
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
func createNewNode(key string, value *models.Data) *Node {
	newData := createDataForNode(key, value)
	hashFunc := hash.CreateHashFunctions(1)[0]
	values, _ := hashFunc.Hash(newData)
	valuesBinary := make([]byte, 8)
	binary.BigEndian.PutUint64(valuesBinary, values)
	return &Node{left: nil, right: nil, data: valuesBinary}
}

/*
since merkle tree if build from bottom up we need all data as leafs
if number of leafs is not 2**n we need to add empty nodes
there hash wont change anything
*/
func CreateMerkleTree(allData map[string]*models.Data) *MerkleTree {
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
	if math.Mod(n, 1) != 0 { // check if hole number
		targetSize := int(math.Pow(2, degree)) // ex. n is 2.3 then degree is 3 and we need 8 nodes since 2**3
		for i := len(nodes); i < targetSize; i++ {
			// add number of empty nodes that is needed
			nodes = append(nodes, createNewNode("", &models.Data{Value: []byte{}, Tombstone: true, Timestamp: 0}))
		}
	}

	for len(nodes) > 1 {
		var newLevel []*Node

		merkleTree.hashWithSeed = hash.CreateHashFunctions(1)
		for i := 0; i < len(nodes); i += 2 {
			hashFunc := merkleTree.hashWithSeed[0]
			// add two array together and hash
			values, _ := hashFunc.Hash(append(nodes[i].data, nodes[i+1].data...))
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
	return &merkleTree
}
func (tree *MerkleTree) WriteInFile(data []byte, filePath string) error {
	// necessary for mapping the file on disc, we have to take care of memory space
	// initialize to 8 because the hashWithSeed size is variable
	totalBytes := int64(8)
	// all data that needs to be written to disc
	// append serialized hash function
	serializedHash := hash.Serialize(tree.hashWithSeed)
	hashFuncSize := uint64(len(serializedHash))
	totalBytes += int64(hashFuncSize)
	// save size of hash function with seeds
	flatData := make([]byte, 8)
	binary.BigEndian.PutUint64(flatData[:8], hashFuncSize)
	// append hashWithSeed
	flatData = append(flatData, serializedHash...)
	// append merkleTree data
	flatData = append(flatData, data...)
	totalBytes += int64(len(data))

	// open file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()

	// ensure that the file has enough space to accommodate the new data
	if err = file.Truncate(totalBytes + fileSize); err != nil {
		return err
	}
	// mapping the file
	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapFile[fileSize:], flatData)
	err = mmapFile.Unmap()
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func MerkleBFS(root *Node) []byte {
	if root == nil {
		return []byte{}
	}
	result := []byte{}
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
func (tree *MerkleTree) ReadFromFile(filePath string) (*MerkleTree, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	mmapFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	// read hashWithSeed len
	hashWithSeedSize := binary.BigEndian.Uint64(mmapFile[:HashWithSeedSizeSize])
	//read and deserialize hashWithSeed
	tree.hashWithSeed = hash.Deserialize(mmapFile[HashWithSeedSizeSize : HashWithSeedSizeSize+hashWithSeedSize])

	// read merkleTree data, every node contains hashed array of 8 bytes
	data := make([]byte, 0)
	for offset := uint64(HashWithSeedSizeSize + hashWithSeedSize); offset < uint64(len(mmapFile)); offset += 8 {
		data = append(data, mmapFile[offset:offset+8]...)
	}

	tree.Root = tree.Root.binaryTree(data, 0)
	err = mmapFile.Unmap()
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	return tree, nil
}
func (node *Node) binaryTree(data []byte, index int) *Node {
	if index < len(data) {
		node := &Node{
			data:  data,
			left:  nil,
			right: nil,
		}
		node.left = node.binaryTree(data, 2*index+1)
		node.right = node.binaryTree(data, 2*index+2)
		return node
	}
	return nil
}
