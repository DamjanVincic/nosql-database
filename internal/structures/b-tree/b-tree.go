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
	for _, child := range node.children {
		found, foundNode := search(key, child)
		if found {
			return true, foundNode
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
