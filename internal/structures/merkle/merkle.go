package merkle

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
	"github.com/edsrzf/mmap-go"
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

func createNewNode(key string, value *models.Data) *Node {
	newData := createDataForNode(key, value)
	hashFunc := hash.CreateHashFunctions(1)[0]
	values, _ := hashFunc.Hash(newData)
	valuesBinary := make([]byte, 8)
	binary.BigEndian.PutUint64(valuesBinary, values)
	return &Node{left: nil, right: nil, data: valuesBinary}
}
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
	fmt.Println(len(nodes))
	fmt.Println(degree)
	if math.Mod(n, 1) != 0 {
		targetSize := int(math.Pow(2, degree))
		for i := len(nodes); i < targetSize; i++ {
			nodes = append(nodes, createNewNode("", &models.Data{Value: []byte{}, Tombstone: true, Timestamp: 0}))
		}
	}
	fmt.Println(len(nodes))

	for len(nodes) > 1 {
		var newLevel []*Node

		merkleTree.hashWithSeed = hash.CreateHashFunctions(1)
		for i := 0; i < len(nodes); i += 2 {
			hashFunc := merkleTree.hashWithSeed[0]
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

	merkleTree.Root = nodes[len(nodes)-1]
	return &merkleTree
}
func (tree *MerkleTree) WriteInFile(data [][]byte, filePath string) error {
	// necessary for mapping the file on disc, we have to take care of memory space
	// initialize to 8 because the hashWithSeed size is variable
	totalBytes := int64(8)
	// all data that needs to be written to disc
	// append serialized hash function
	serializedHash := hash.Serialize(tree.hashWithSeed)
	hashFuncSize := uint64(len(serializedHash))
	totalBytes += int64(hashFuncSize)
	// first save size of hash function with seeds
	flatData := make([]byte, 8)

	binary.BigEndian.PutUint64(flatData[:8], hashFuncSize)
	flatData = append(flatData, serializedHash...)

	for _, row := range data {
		flatData = append(flatData, row...)
		totalBytes += int64(len(row))
	}
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileStat.Size()
	if err = file.Truncate(totalBytes + fileSize); err != nil {
		return err
	}

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
func LevelOrder(root *Node) [][]byte {
	if root == nil {
		return [][]byte{}
	}
	result := [][]byte{}
	nodes := []*Node{root}
	for len(nodes) > 0 {
		current := nodes[0] // get next in line
		nodes = nodes[1:]
		result = append(result, current.data)
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

	data := make([][]byte, 0)
	//var offset uint64
	for i := uint64(0); i < uint64(len(mmapFile)); i += 8 {
		//offset = binary.BigEndian.Uint64([]byte{mmapFile[i]})
		//data = append(data, mmapFile[i:offset])
		//i += offset
		data = append(data, mmapFile[i:i+8])
	}

	fmt.Println(data)
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
func (node *Node) binaryTree(data [][]byte, index int) *Node {
	if index < len(data) {
		node := &Node{
			data:  data[0],
			left:  nil,
			right: nil,
		}
		node.left = node.binaryTree(data, 2*index+1)
		node.right = node.binaryTree(data, 2*index+2)
		return node
	}
	return nil
}
