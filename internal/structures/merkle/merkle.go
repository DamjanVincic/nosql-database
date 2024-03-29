package merkle

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/DamjanVincic/key-value-engine/internal/structures/keyencoder"
	"math"

	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
)

const (
	// number of bytes used for writting the size of hashWithSeed in file
	HashWithSeedSizeSize = 8
	// number of bytes stored in Node data after hashing
	HashedNodesSize = 8
	// Size of the 'size' attribute in bytes
	SizeSize = 8
)

type MerkleTree struct {
	Root         *Node
	HashWithSeed *hash.HashWithSeed
	size         uint64
}

type Node struct {
	Data  []byte
	left  *Node
	right *Node
}

/*
get hashed data for leaves
*/
func (merkleTree *MerkleTree) CreateNodeData(value *models.Data, compression bool, encoder *keyencoder.KeyEncoder) ([]byte, error) {
	newData := value.Serialize(compression, encoder)
	values, err := merkleTree.HashWithSeed.Hash(newData)

	if err != nil {
		return nil, err
	}
	valuesBinary := make([]byte, 8)
	binary.BigEndian.PutUint64(valuesBinary, values)

	return valuesBinary, nil
}

/*
get binary data and hash function and return new node with no children and hashed values
*/

func NewMerkle(hashFunc *hash.HashWithSeed) *MerkleTree {
	if hashFunc == nil {
		hashFunc = &hash.CreateHashFunctions(1)[0]
	}
	return &MerkleTree{
		Root:         nil,
		HashWithSeed: hashFunc,
		size:         0,
	}
}

/*
since merkle tree is build from bottom up we need all data as leafs
if number of leafs is not 2**n we need to add empty nodes
there hash wont change anything
*/
func (merkleTree *MerkleTree) CreateMerkleTree(hashedData []byte, compression bool, encoder *keyencoder.KeyEncoder) error {
	var nodes []*Node

	// creating all the end nodes
	const NodeDataSize = 8
	offset := 0
	for offset != len(hashedData) {
		node := &Node{left: nil, right: nil, Data: hashedData[offset : offset+NodeDataSize]}
		nodes = append(nodes, node)
		offset += NodeDataSize
	}

	// if number of nodes is not 2**n add empty nodes
	n := math.Log2(float64(len(nodes)))
	degree := math.Ceil(n)
	targetSize := uint64(math.Pow(2, degree)) // ex. n is 2.3 then degree is 3 and we need 8 nodes since 2**3
	merkleTree.size = targetSize
	for i := uint64(len(nodes)); i < targetSize; i++ {
		// add number of empty nodes that is needed
		empty, err := merkleTree.CreateNodeData(models.NewData("", []byte{}, false, 0), compression, encoder)
		if err != nil {
			return err
		}
		node := &Node{left: nil, right: nil, Data: empty}
		nodes = append(nodes, node)
		offset += NodeDataSize
	}

	for len(nodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(nodes); i += 2 {
			// add two array together and hash
			values, err := merkleTree.HashWithSeed.Hash(append(nodes[i].Data, nodes[i+1].Data...))
			if err != nil {
				return err
			}
			valuesBinary := make([]byte, 8)
			binary.BigEndian.PutUint64(valuesBinary, values)
			// create new node with nodes at index i and i + 1, parent-left and right child
			// add new hash value of two nodes as data of new node
			newNode := &Node{
				left:  nodes[i],
				right: nodes[i+1],
				Data:  valuesBinary,
			}
			newLevel = append(newLevel, newNode)
		}
		nodes = newLevel
	}

	merkleTree.Root = nodes[len(nodes)-1]
	return nil
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
		result = append(result, current.Data...)
		if current.left != nil {
			nodes = append(nodes, current.left)
		}
		if current.right != nil {
			nodes = append(nodes, current.right)
		}
	}
	return result
}

func (merkleTree *MerkleTree) Serialize() []byte {
	//add size of `size` attribute and hashWithSeed size to byte array
	bytes := make([]byte, SizeSize+HashWithSeedSizeSize)
	binary.BigEndian.PutUint64(bytes[:SizeSize], merkleTree.size)
	serializedHash := hash.Serialize([]hash.HashWithSeed{*merkleTree.HashWithSeed})
	hashFuncSize := uint64(len(serializedHash))
	binary.BigEndian.PutUint64(bytes[SizeSize:SizeSize+HashWithSeedSizeSize], hashFuncSize)
	//append hashWithSeed
	bytes = append(bytes, serializedHash...)
	bytes = append(bytes, MerkleBFS(merkleTree.Root)...)
	return bytes
}

func DeserializeMerkle(data []byte) *MerkleTree {
	size := binary.BigEndian.Uint64(data[:SizeSize])
	//deserialize hash func
	hashWithSeedSize := binary.BigEndian.Uint64(data[SizeSize : SizeSize+HashWithSeedSizeSize])
	hashWithSeed := hash.Deserialize(data[SizeSize+HashWithSeedSizeSize : SizeSize+HashWithSeedSizeSize+hashWithSeedSize])[0]
	//deserialize nodes
	var nodes []byte
	for offset := SizeSize + HashWithSeedSizeSize + hashWithSeedSize; offset < uint64(len(data)); offset += HashedNodesSize {
		nodes = append(nodes, data[offset:offset+HashedNodesSize]...)
	}
	//constructs merkle from list of nodes and returns root node
	root := binaryTree(nodes, 0)
	return &MerkleTree{
		Root:         root,
		HashWithSeed: &hashWithSeed,
		size:         size,
	}
}

// recursive function used to construct a binary tree from a given byte slice data and an initial index index
// returns Root Node
func binaryTree(data []byte, index int) *Node {
	if index*HashedNodesSize+HashedNodesSize <= len(data) {
		node := &Node{
			Data:  data[index*HashedNodesSize : index*HashedNodesSize+HashedNodesSize],
			left:  nil,
			right: nil,
		}
		node.left = binaryTree(data, 2*index+1)
		node.right = binaryTree(data, 2*index+2)
		return node
	}
	return nil
}

func (merkleTree *MerkleTree) CompareTrees(otherMerkleTree *MerkleTree) ([]uint64, error) {
	if merkleTree.size != otherMerkleTree.size {
		return nil, errors.New("too many deleted entries")
	}

	var corruptedNodes []uint64
	compareTrees(merkleTree.Root, otherMerkleTree.Root, &corruptedNodes, 0, merkleTree.size/2)
	return corruptedNodes, nil
}

func compareTrees(node1 *Node, node2 *Node, corruptedNodes *[]uint64, index uint64, tempSize uint64) {
	// Compare the hash values of the nodes
	if (node1 == nil || node2 == nil || !bytes.Equal(node1.Data, node2.Data)) && node1.left == nil && node1.right == nil && node2.right == nil && node2.left == nil {
		// Nodes are different or one is nil, consider it corrupted
		*corruptedNodes = append(*corruptedNodes, index)
	} else {
		if !bytes.Equal(node1.left.Data, node2.left.Data) {
			//index++
			compareTrees(node1.left, node2.left, corruptedNodes, index, tempSize/2)
		}
		if !bytes.Equal(node1.right.Data, node2.right.Data) {
			index += tempSize
			compareTrees(node1.right, node2.right, corruptedNodes, index, tempSize/2)
		}
	}
}
