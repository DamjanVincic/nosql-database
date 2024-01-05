package main

import "fmt"

var T = 3

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
func newNode(keys []int) *BTreeNode {
	return &BTreeNode{
		parent:      nil,
		t:           T,
		maxChildren: 2*T + 1,
		currentKeys: 1, // initialize with one key
		maxKeys:     2*T - 1,
		keys:        keys,
		children:    []*BTreeNode{},
		leaf:        false,
	}
}
func newLeaf(keys []int) *BTreeNode {
	return &BTreeNode{
		parent:      nil,
		t:           T,
		maxChildren: 2*T + 1,
		currentKeys: 1, // initialize with one key
		maxKeys:     2*T - 1,
		keys:        keys,
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
	if len(root.keys) == 0 || root == nil {
		return initTree(key)
		// if root is filled max
	} else if len(root.keys) == 2*root.t-1 {
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
	i := len(node.keys)
	// if node is leaf just add and sort
	if node.leaf {
		for i > 0 && node.keys[i-1] > key {
			node.keys = append(node.keys, -1)
			node.keys[i] = node.keys[i-1]
			//node.children[i] = node.children[i-1]
			i--
		}
		if i+1 < len(node.keys) {
			node.keys[i] = key
		} else {
			node.keys = append(node.keys, key)
		}
	} else {
		for i > 0 && node.keys[i-1] > key {
			i -= 1
		}
		// check overflow
		if len(node.children[i].keys) == 2*node.t-1 {
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
	child.keys = child.keys[:T-1]

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
		parent.keys = append(parent.keys, -1)
		if len(parent.keys) == 1 { // special case in the begining
			parent.keys[i] = keyToMove
		} else {
			j--
			parent.keys[j+1] = parent.keys[j]
		}
	}
	parent.keys[i] = keyToMove
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
func Delete(key int, node *BTreeNode) *BTreeNode {
	index := find(key, node)
	// key is in this node
	if index < len(node.keys) && node.keys[index] == key {
		if node.leaf {
			removeFromLeaf(index, node)
		} else {
			removeFromInternal(key, index, node)
		}
		// look for key in child with given index
	} else {
		// key is not in the tree
		if node.leaf {
			return node
		}
		// if key is not in this node we do further
		// need to check whether the node in the recursion path
		// has minimal number of keys
		if len(node.children[index].keys) == T-1 {
			borrow := false
			merge := false
			indexLeft := index
			indexRight := index
			if index+1 == len(node.children) {
				// then node is the last one
				indexLeft--
				if len(node.children[indexLeft].keys) > T-1 {
					// sibling has more than minimum number of keys
					// we can borrow
					borrow = true
				} else {
					merge = true
				}
			} else if index == 0 {
				// then node is first
				indexRight++
				if len(node.children[indexRight].keys) > T-1 {
					borrow = true
				} else {
					merge = true
				}
			} else if index != 0 && index+1 < len(node.children)-1 {
				// node is in the middle, second to last at most
				// we can get from both right and left
				if len(node.children[index-1].keys) > T-1 {
					// we borrow from left child
					indexLeft--
					borrow = true
				} else if len(node.children[index+1].keys) > T-1 {
					// we borrow from right child
					indexRight++
					borrow = true
				} else {
					merge = true
					indexRight++
				}
			}
			if borrow {
				node = borrowKeyFromSibling(node, indexLeft, indexRight)
			} else if merge {
				node = mergeWithSibling(node, indexLeft, indexRight, key)
				// again delete because its moved
				Delete(key, node)
			}
		}
		Delete(key, node.children[index])
	}
	return node
}
func borrowKeyFromSibling(node *BTreeNode, indexLeft, indexRight int) *BTreeNode {
	leftChild := node.children[indexLeft]
	rightChild := node.children[indexRight]

	leftChild.keys = append(leftChild.keys, node.keys[indexLeft])
	if !leftChild.leaf {
		leftChild.children = append(leftChild.children, rightChild.children[0])
	}

	node.keys[indexLeft] = rightChild.keys[0]
	for j := 0; j < len(rightChild.keys)-1; j++ {
		rightChild.keys[j] = rightChild.keys[j+1]
	}
	rightChild.keys = deleteAtIndex(len(rightChild.keys)-1, rightChild.keys)
	if !rightChild.leaf {
		for j := 0; j < len(rightChild.keys); j++ {
			rightChild.children[j] = rightChild.children[j+1]
		}
	}
	leftChild.keys = deleteAtIndex(indexLeft-(T-1), leftChild.keys)
	return node
}
func mergeWithSibling(node *BTreeNode, indexLeft, indexRight, key int) *BTreeNode {
	leftChild := node.children[indexLeft]
	rightChild := node.children[indexRight]
	leftChild.keys = append(leftChild.keys, node.keys[indexLeft])
	node.keys = deleteAtIndex(indexLeft, node.keys)
	// move keys and children
	for j := 0; j <= len(rightChild.keys); j++ {
		if j != len(rightChild.keys) { // index out of range
			leftChild.keys = append(leftChild.keys, rightChild.keys[j])
		}
		if !leftChild.leaf {
			leftChild.children = append(leftChild.children, rightChild.children[j])
		}
	}
	// move parents keys and children
	for j := indexRight; j <= len(node.keys); j++ {
		if j != len(node.keys) {
			node.keys[j-1] = node.keys[j]
		}
	}
	Delete(key, node)
	return node
}
func find(key int, root *BTreeNode) int {
	index := 0 // if the key is smaller than first key in node
	for j := 0; j < len(root.keys); j++ {
		if root.keys[index] < key {
			index++
		}
	}
	return index
}
func removeFromLeaf(index int, node *BTreeNode) {
	if index < len(node.keys) {
		node.keys = deleteAtIndex(index, node.keys)
	}
}
func removeFromInternal(key, index int, node *BTreeNode) {
	if node.leaf {
		removeFromLeaf(index, node)
	}
	// delete from internal node
	// find child that has above minimum number of keys
	// get predecessor of the key
	// check first left child
	if len(node.children[index].keys) > (T - 1) { // bigger that minimum
		node.keys[index] = swichPredecessor(node.children[index])
		// check right child next
	} else if len(node.children[index+1].keys) > (T - 1) {
		node.keys[index] = swichSuccessor(node.children[index+1])
		// combine nodes
	} else {
		mergeWithSibling(node, index, index+1, key)
	}
}
func deleteAtIndex(index int, list []int) []int {
	if index < 0 || index >= len(list) {
		return list
	}

	return append(list[:index], list[index+1:]...)
}
func deleteAtIndexNode(index int, list []*BTreeNode) []*BTreeNode {
	if index < 0 || index >= len(list) {
		return list
	}

	return append(list[:index], list[index+1:]...)
}
func swichPredecessor(node *BTreeNode) int {
	if node.leaf {
		last := node.keys[len(node.keys)-1]
		node.keys = deleteAtIndex(len(node.keys)-1, node.keys) // remove predecessor
		return last                                            // return last
	}
	return -1
}
func swichSuccessor(node *BTreeNode) int {
	if node.leaf {
		first := node.keys[0]
		node.keys = deleteAtIndex(0, node.keys) // remove predecessor
		return first                            // return last
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
func main() {
	// Initialize an empty root node
	root := initTree(40)

	leaf1 := newLeaf([]int{1, 9})
	leaf2 := newLeaf([]int{17, 19, 21})
	leaf3 := newLeaf([]int{23, 25, 27})
	leaf4 := newLeaf([]int{31, 32, 39})

	leaf5 := newLeaf([]int{41, 47, 50})
	leaf6 := newLeaf([]int{56, 60})
	leaf7 := newLeaf([]int{72, 90})

	node1 := newNode([]int{15, 22, 30})
	node1.children = append(node1.children, leaf1)
	node1.children = append(node1.children, leaf2)
	node1.children = append(node1.children, leaf3)
	node1.children = append(node1.children, leaf4)

	node2 := newNode([]int{55, 63})
	node2.children = append(node2.children, leaf5)
	node2.children = append(node2.children, leaf6)
	node2.children = append(node2.children, leaf7)

	root.children = append(root.children, node1)
	root.children = append(root.children, node2)
	PrintBTree(root, 0)

	root.leaf = false

	fmt.Println("delete 21--------------")
	Delete(21, root)
	PrintBTree(root, 0)
	fmt.Println("delete 30--------------")
	Delete(30, root)
	PrintBTree(root, 0)
	fmt.Println("delete 27--------------")
	Delete(27, root)
	PrintBTree(root, 0)
	fmt.Println("delete 22--------------")
	Delete(22, root)
	PrintBTree(root, 0)
}
