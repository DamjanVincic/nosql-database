package b_tree

/*
implement 2-3 B tree
every node has at most two keys and three children
*/
type BTreeNode struct {
	keys     []int        // list of keys
	children []*BTreeNode // list of child pointers
	leaf     bool         // is node a leaf
}

func contains(list []int, element int) bool {
	for _, value := range list {
		if value == element {
			return true
		}
	}
	return false
}
func initTree() *BTreeNode {
	return &BTreeNode{
		keys:     []int{},
		children: []*BTreeNode{},
		leaf:     true,
	}
}

// root at first and then recursion, until if finds bigger key
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
func Insert(key int, root *BTreeNode) {
	found, _ := search(key, root)
	if found {
		return
	}
}
func Delete() {

}
