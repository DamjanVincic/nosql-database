package main

import "fmt"

var T = 2

type BTreeNode struct {
	parent      *BTreeNode
	t           int // minimum degree
	maxChildren int
	currentKeys int
	maxKeys     int
	keys        []int        // list of keys
	children    []*BTreeNode // list of child pointers
	leaf        bool         // is node a leaf
}

/*
function that checks whether key is in the node or not
*/
func contains(list []int, element int) bool {
	for _, value := range list {
		if value == element {
			return true
		}
	}
	return false
}
func initTree(key int) *BTreeNode {
	return &BTreeNode{
		parent:      nil,
		t:           T,
		maxChildren: 2*T + 1,
		currentKeys: 1, // initialize with one key
		maxKeys:     2*T - 1,
		keys:        []int{key},
		children:    []*BTreeNode{},
		leaf:        true,
	}
}

/*
searches whether the key is in the tree
optimize to not check branch that cant possible have the key
*/
func search(key int, node *BTreeNode) (bool, *BTreeNode) {
	if contains(node.keys, key) {
		return true, node
	}

	// check if the key is less than the minimum key in the current nodes keys
	if len(node.keys) > 0 && key < node.keys[0] {
		return false, nil
	}

	// check if the key is greater than the maximum key in the current nodes keys
	if len(node.keys) > 0 && key > node.keys[len(node.keys)-1] {
		return false, nil
	}

	// iterate through children only if its not a leaf node
	if !node.leaf {
		for _, child := range node.children {
			found, foundNode := search(key, child)
			if found {
				return true, foundNode
			}
		}
	}

	return false, nil
}

/*
after failed searched,
if root is not initialized, it initializes it
if the root is full its split,
if not key is added in empty space
*/
func Insert(key int, root *BTreeNode) *BTreeNode {
	found, _ := search(key, root)
	if found {
		return nil
	}
	// if root is empty we need to initialize the tree
	if root.currentKeys == 0 || root == nil {
		return initTree(key)
		// if root is filled max
	} else if root.currentKeys == 2*root.t-1 {
		// when split, middle element goes to parent node
		// create pseudo-parent so it has somewhere to 'spil'
		newNode := &BTreeNode{
			parent:      nil,
			t:           2,
			maxChildren: 3,
			currentKeys: 0,
			maxKeys:     2,
			keys:        []int{},
			children:    []*BTreeNode{},
			leaf:        false, // new root
		}
		newNode.children = append(newNode.children, root)
		root.parent = newNode
		split(0, root, newNode)
		insertInNodeThatHasRoom(key, newNode)
		return newNode
	} else {
		insertInNodeThatHasRoom(key, root)
		return root
	}
}
func insertInNodeThatHasRoom(key int, node *BTreeNode) {
	i := node.currentKeys
	// if node is leaf just add and sort
	if node.leaf {
		for i > 0 && node.keys[i-1] > key {
			node.keys = append(node.keys, -1)
			node.keys[i] = node.keys[i-1]
			//node.children[i] = node.children[i-1]
			i--
		}
		if i+1 < len(node.keys) {
			node.keys[i+1] = key
		} else {
			node.keys = append(node.keys, key)
		}
		node.currentKeys += 1
	} else {
		for i > 0 && node.keys[i-1] > key {
			i -= 1
		}
		// check overflow
		if node.children[i].currentKeys == 2*node.t-1 {
			split(i, node.children[i], node)
			if node.keys[i] < key {
				i += 1
			}
		}
		insertInNodeThatHasRoom(key, node.children[i])
	}
}

// child if full
/*
create a new node and move half the entries
from the overflowing node to the new node
then insert the pointer to the new node into the
upper neighbor
*/
func split(i int, child *BTreeNode, parent *BTreeNode) {
	keyToMove := child.keys[T-1]
	newNode := &BTreeNode{
		t:           T,
		maxChildren: 2*T + 1,
		currentKeys: T - 1,
		maxKeys:     2*T - 1,
		keys:        []int{},
		children:    []*BTreeNode{},
		leaf:        child.leaf, // if child is leaf so is new node
	}
	newNode.keys = append(newNode.keys, child.keys[T:]...)
	newNode.currentKeys = len(newNode.keys)
	child.keys = child.keys[:T-1]
	child.currentKeys = T - 1

	if !child.leaf {
		for j := 0; j < child.t; j++ {
			// move children if node is not a leaf
			newNode.children = append(newNode.children, nil)
			newNode.children[j] = child.children[j+T]
		}
		child.children = child.children[:T]
	}

	for j := parent.currentKeys; j >= i; j-- {
		parent.children = append(parent.children, nil) //create space for new node, fix index out of range
		parent.children[j+1] = parent.children[j]
	}
	parent.children[i+1] = newNode

	for j := len(parent.keys); j >= i; j-- {
		parent.keys = append(parent.keys, -1)
		if len(parent.keys) == 1 { // special case in the begining
			parent.keys[i] = keyToMove
		} else {
			j--
			parent.keys[j+1] = parent.keys[j]
		}
	}
	parent.keys[i] = keyToMove
	parent.currentKeys++
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
func Delete(key int, node *BTreeNode) {
	index := find(key, node)
	// key is in this node
	if index < node.currentKeys && node.keys[index] == key {
		if node.leaf {
			removeFromLeaf(index, node)
		} else {
			removeFromInternal(index, node)
		}
		// look for key in child with given index
	} else {
		// key is not in the tree
		if node.leaf {
			return
			// check next node
		}
		Delete(key, node.children[index])
	}
}
func find(key int, root *BTreeNode) int {
	index := 0 // if the key is smaller than first key in node
	for j := 0; j < root.currentKeys; j++ {
		if root.keys[index] < key {
			index++
		}
	}
	return index
}
func removeFromLeaf(index int, node *BTreeNode) {
	for j := index; j < node.currentKeys; j++ { // we shift one place at the time
		if node.currentKeys == 1 {
			node = nil // needs to be removed from parent, will never get to this case
		} else {
			node.keys = deleteAtIndex(index, node.keys)
		}
	}
	node.currentKeys--
}
func removeFromInternal(index int, node *BTreeNode) {
	if node.leaf {
		removeFromLeaf(index, node)
	}
	// delete from internal node
	// find child that has above minimum number of keys
	// get predecessor of the key
	// check first left child
	if node.children[index].currentKeys > (T - 1) { // bigger that minimum
		node.keys[index] = swichPredecessor(node.children[index])
		// check right child next
	} else if node.children[index+1].currentKeys > (T - 1) {
		node.keys[index] = swichSuccessor(node.children[index+1])
		// combine nodes
	} else {
		combineNodes(node, index, index+1)
		removeFromInternal(T-1, node.children[index])
	}
}
func deleteAtIndex(index int, list []int) []int {
	if index < 0 || index >= len(list) {
		return list
	}

	return append(list[:index], list[index+1:]...)
}
func combineNodes(node *BTreeNode, indexLeft, indexRight int) {

}
func swichPredecessor(node *BTreeNode) int {
	if node.leaf {
		last := node.keys[node.currentKeys-1]
		node.keys = deleteAtIndex(node.currentKeys-1, node.keys) // remove predecessor
		node.currentKeys--
		return last // return last
	}
	return -1
}
func swichSuccessor(node *BTreeNode) int {
	if node.leaf {
		first := node.keys[0]
		node.keys = deleteAtIndex(0, node.keys) // remove predecessor
		node.currentKeys--
		return first // return last
	}
	return -1
}

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
