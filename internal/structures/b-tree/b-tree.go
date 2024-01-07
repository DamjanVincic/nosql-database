package btree

import (
	"errors"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/models"
)

const (
	T = 2 // degree of nodes
)

type BTreeNode struct {
	t        int          // minimum degree
	keys     []string     // list of keys
	children []*BTreeNode // list of child pointers
	leaf     bool         // is node a leaf
	data     map[string]*models.Data
}
type BTree struct {
	root *BTreeNode
}

func CreateBTree() *BTree {
	root := &BTreeNode{
		t:        T,
		keys:     []string{},
		children: []*BTreeNode{},
		leaf:     true,
		data:     make(map[string]*models.Data),
	}
	tree := &BTree{
		root: root,
	}
	return tree
}

/*
function that checks whether key is in the node or not
*/
func contains(list []string, element string) bool {
	for _, value := range list {
		if value == element {
			return true
		}
	}
	return false
}

/*
searches whether the key is in the tree
optimize to not check branch that cant possibly have the key
*/
func (tree *BTree) Get(key string) (*models.Data, error) {
	found, node := search(key, tree.root)
	if found {
		return node.data[key], nil
	}
	return nil, errors.New("key not found")
}

func search(key string, node *BTreeNode) (bool, *BTreeNode) {
	if contains(node.keys, key) {
		return true, node
	}

	index := 0
	for i := 0; i < len(node.keys); i++ {
		if node.keys[i] < key {
			index++
		}
	}
	if index < len(node.keys) && node.keys[index] == key {
		return true, node
	}

	// iterate through children only if its not a leaf node
	if !node.leaf {
		return search(key, node.children[index])
	}

	return false, nil
}

/*
after failed search,
if root is not initialized, it initializes it
if the root is full its split,
if not key is added in empty space
*/
func (tree *BTree) Put(key string, dataValue []byte, tombstone bool, timestamp uint64) error {
	value := &models.Data{Value: dataValue, Tombstone: tombstone, Timestamp: timestamp}
	root := tree.root
	found, _ := search(key, root)
	if found {
		return errors.New("key already in tree")
	}
	// if root is empty we need to initialize the tree
	if len(root.keys) == 2*T-1 {
		// when split, middle element goes to parent node
		// create pseudo-parent so it has somewhere to 'spil'
		newNode := &BTreeNode{
			t:        T,
			keys:     []string{},
			children: []*BTreeNode{},
			leaf:     false, // new root
			data:     make(map[string]*models.Data),
		}
		newNode.children = append(newNode.children, root)
		newNode = split(0, root, newNode)
		insertInNodeThatHasRoom(key, value, newNode)
		tree.root = newNode
	} else {
		insertInNodeThatHasRoom(key, value, root)
		tree.root = root
	}
	return nil
}

func insertInNodeThatHasRoom(key string, value *models.Data, node *BTreeNode) {
	i := len(node.keys)
	// if node is leaf just add and sort
	if node.leaf {
		for i > 0 && node.keys[i-1] > key {
			node.keys = append(node.keys, "")
			node.data[key] = value
			node.keys[i] = node.keys[i-1]
			//node.children[i] = node.children[i-1]
			i--
		}
		if len(node.keys) != 0 && node.keys[len(node.keys)-1] == "" {
			node.keys = deleteAtIndex(len(node.keys)-1, node.keys)
		}
		if i+1 < len(node.keys) {
			node.keys[i] = key
			node.data[key] = value
		} else {
			node.keys = append(node.keys, key)
			node.data[key] = value
		}
	} else {
		for i > 0 && node.keys[i-1] > key {
			i--
		}
		// check overflow
		if len(node.children[i].keys) == 2*node.t-1 {
			node = split(i, node.children[i], node)
			if node.keys[i] < key {
				i++
			}
		}
		insertInNodeThatHasRoom(key, value, node.children[i])
	}
}

// child if full
/*
create a new node and move half the entries
from the overflowing node to the new node
then insert the pointer to the new node into the
upper neighbor
i - index of a nodes child that is to be split
*/
func split(i int, child *BTreeNode, parent *BTreeNode) *BTreeNode {
	keyToMove := child.keys[T-1]
	dataToMove := child.data[keyToMove]
	newNode := &BTreeNode{
		t:        T,
		keys:     []string{},
		children: []*BTreeNode{},
		leaf:     child.leaf, // if child is leaf so is new node
		data:     make(map[string]*models.Data),
	}
	// add all keys and data to new node
	newNode.keys = append(newNode.keys, child.keys[T:]...)
	for _, key := range child.keys[T:] {
		newNode.data[key] = child.data[key]
	}

	// remove from new sibling node
	for _, key := range child.keys[T:] {
		delete(child.data, key)
	}
	child.keys = child.keys[:T-1]
	// remove the one that goes to parent
	delete(child.data, keyToMove)

	if !child.leaf {
		for j := 0; j < child.t; j++ {
			// move children if node is not a leaf
			newNode.children = append(newNode.children, nil)
			newNode.children[j] = child.children[j+T]
		}
		child.children = child.children[:T]
	}

	for j := len(parent.keys); j >= i; j-- {
		parent.children = append(parent.children, nil) //create space for new node, fix index out of range
		parent.children[j+1] = parent.children[j]
	}
	parent.children[i+1] = newNode

	for j := len(parent.keys); j >= i; j-- {
		parent.keys = append(parent.keys, "")
		if len(parent.keys) == 1 { // special case in the begining
			parent.keys[i] = keyToMove
		} else {
			j--
			parent.keys[j+1] = parent.keys[j]
		}
	}
	parent.keys[i] = keyToMove
	parent.data[keyToMove] = dataToMove
	return parent
}

/*
search will return node that has the key that needs to be deleted
 1. deletion of key from leaf node
    are keys in node > min
    if yes, delete the key and shift the other keys of the node
    if no, check siblings and borrow from them (check if left (then right) > min)
    if cant borrow from sibling, combine nodes
 2. deletion of key from non leaf node
    replace the key by its successor and delete successor (always left node) then case 1.

get index of the next node and call function again if
node doesnt contain key
*/
//func Delete(key string, node *BTreeNode) *BTreeNode {
//	index := find(key, node)
//	// key is in this node
//	if index < len(node.keys) && node.keys[index] == key {
//		if node.leaf {
//			removeFromLeaf(index, node)
//		} else {
//			removeFromInternal(key, index, node)
//		}
//		// look for key in child with given index
//	} else {
//		// key is not in the tree
//		if node.leaf {
//			return node
//		}
//		// if key is not in this node we do further
//		// need to check whether the node in the recursion path
//		// has minimal number of keys
//		if len(node.children[index].keys) == T-1 {
//			borrow := false
//			merge := false
//			indexLeft := index
//			indexRight := index
//			if index+1 == len(node.children) {
//				// then node is the last one
//				indexLeft--
//				if len(node.children[indexLeft].keys) > T-1 {
//					// sibling has more than minimum number of keys
//					// we can borrow
//					borrow = true
//				} else {
//					merge = true
//				}
//			} else if index == 0 {
//				// then node is first
//				indexRight++
//				if len(node.children[indexRight].keys) > T-1 {
//					borrow = true
//				} else {
//					merge = true
//				}
//			} else if index != 0 && index+1 < len(node.children)-1 {
//				// node is in the middle, second to last at most
//				// we can get from both right and left
//				if len(node.children[index-1].keys) > T-1 {
//					// we borrow from left child
//					indexLeft--
//					borrow = true
//				} else if len(node.children[index+1].keys) > T-1 {
//					// we borrow from right child
//					indexRight++
//					borrow = true
//				} else {
//					merge = true
//					indexRight++
//				}
//			}
//			if borrow {
//				node = borrowKeyFromSibling(node, indexLeft, indexRight)
//			} else if merge {
//				node = mergeWithSibling(node, indexLeft, indexRight, key)
//				// again delete because its moved
//				Delete(key, node)
//			}
//		}
//		Delete(key, node.children[index])
//	}
//	return node
//}

// Mock delete function to implement the interface
func (tree *BTree) Delete(key string) error {
	return nil
}

//func borrowKeyFromSibling(node *BTreeNode, indexLeft, indexRight int) *BTreeNode {
//	leftChild := node.children[indexLeft]
//	rightChild := node.children[indexRight]
//
//	leftChild.keys = append(leftChild.keys, node.keys[indexLeft])
//	if !leftChild.leaf {
//		leftChild.children = append(leftChild.children, rightChild.children[0])
//	}
//
//	node.keys[indexLeft] = rightChild.keys[0]
//	for j := 0; j < len(rightChild.keys)-1; j++ {
//		rightChild.keys[j] = rightChild.keys[j+1]
//	}
//	rightChild.keys = deleteAtIndex(len(rightChild.keys)-1, rightChild.keys)
//	if !rightChild.leaf {
//		for j := 0; j < len(rightChild.keys); j++ {
//			rightChild.children[j] = rightChild.children[j+1]
//		}
//	}
//	leftChild.keys = deleteAtIndex(indexLeft-(T-1), leftChild.keys)
//	return node
//}

//	func mergeWithSibling(node *BTreeNode, indexLeft, indexRight int, key string) *BTreeNode {
//		leftChild := node.children[indexLeft]
//		rightChild := node.children[indexRight]
//		leftChild.keys = append(leftChild.keys, node.keys[indexLeft])
//		node.keys = deleteAtIndex(indexLeft, node.keys)
//		// move keys and children
//		for j := 0; j <= len(rightChild.keys); j++ {
//			if j != len(rightChild.keys) { // index out of range
//				leftChild.keys = append(leftChild.keys, rightChild.keys[j])
//			}
//			if !leftChild.leaf {
//				leftChild.children = append(leftChild.children, rightChild.children[j])
//			}
//		}
//		node.children = deleteAtIndexNode(indexRight, node.children)
//		// move parents keys and children
//		for j := indexRight; j <= len(node.keys); j++ {
//			if j != len(node.keys) {
//				node.keys[j-1] = node.keys[j]
//			}
//		}
//		if len(node.keys) == 0 {
//			// remove the root
//			node = leftChild
//		}
//		Delete(key, node)
//		return node
//	}
//func find(key string, root *BTreeNode) int {
//	index := 0 // if the key is smaller than first key in node
//	for j := 0; j < len(root.keys); j++ {
//		if root.keys[index] < key {
//			index++
//		}
//	}
//	return index
//}
//func removeFromLeaf(index int, node *BTreeNode) {
//	if index < len(node.keys) {
//		node.keys = deleteAtIndex(index, node.keys)
//	}
//}

//	func removeFromInternal(key string, index int, node *BTreeNode) {
//		if node.leaf {
//			removeFromLeaf(index, node)
//		}
//		// delete from internal node
//		// find child that has above minimum number of keys
//		// get predecessor of the key
//		// check first left child
//		if len(node.children[index].keys) > (T - 1) { // bigger that minimum
//			node.keys[index] = switchPredecessor(node.children[index])
//			// check right child next
//		} else if len(node.children[index+1].keys) > (T - 1) {
//			node.keys[index] = switchSuccessor(node.children[index+1])
//			// combine nodes
//		} else {
//			mergeWithSibling(node, index, index+1, key)
//		}
//	}

func deleteAtIndex(index int, list []string) []string {
	if index < 0 || index >= len(list) {
		return list
	}

	return append(list[:index], list[index+1:]...)
}

//func deleteAtIndexNode(index int, list []*BTreeNode) []*BTreeNode {
//	if index < 0 || index >= len(list) {
//		return list
//	}
//
//	return append(list[:index], list[index+1:]...)
//}

//func switchPredecessor(node *BTreeNode) string {
//	if node.leaf {
//		last := node.keys[len(node.keys)-1]
//		node.keys = deleteAtIndex(len(node.keys)-1, node.keys) // remove predecessor
//		return last                                            // return last
//	}
//	return ""
//}
//func switchSuccessor(node *BTreeNode) string {
//	if node.leaf {
//		first := node.keys[0]
//		node.keys = deleteAtIndex(0, node.keys) // remove predecessor
//		return first                            // return last
//	}
//	return ""
//}

/*
helper function to see structure of the tree
*/
func PrintBTree(node *BTreeNode, level int) {
	if node != nil {
		fmt.Printf("Level %d: ", level)
		fmt.Println(node.keys)

		for _, child := range node.children {
			PrintBTree(child, level+1)
		}
	}
}

func (tree *BTree) GetSortedList() map[string]*models.Data {
	result := make(map[string]*models.Data)
	tree.root.traverse(result)
	return result
}

func (node *BTreeNode) traverse(result map[string]*models.Data) {
	for i := 0; i < len(node.keys); i++ {
		if !node.leaf {
			node.children[i].traverse(result)
		}
		result[node.keys[i]] = node.data[node.keys[i]]
	}
	if !node.leaf {
		node.children[len(node.keys)].traverse(result)
	}
}
