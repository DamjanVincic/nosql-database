package main

import (
	"math"
	"math/rand"
)

const maxHeight = 10

type SkipListValue struct {
	value     []byte
	tombstone bool
	timestamp string
}

type SkipListNode struct {
	key      float64
	value    SkipListValue
	previous *SkipListNode
	next     *SkipListNode
	below    *SkipListNode
	above    *SkipListNode
}

type SkipList struct {
	heads  []*SkipListNode
	tails  []*SkipListNode
	height int
	size   int
}

func (skipList *SkipList) isEmpty() bool {
	return skipList.size == 0
}

func roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= maxHeight {
			return level
		}
	}
	return level
}

func CreateSkipList() SkipList {
	s := SkipList{
		heads:  make([]*SkipListNode, 0),
		tails:  make([]*SkipListNode, 0),
		height: 1,
		size:   0,
	}
	s.heads = append(s.heads, &SkipListNode{key: math.Inf(-1)})
	s.tails = append(s.tails, &SkipListNode{key: math.Inf(1), previous: s.heads[0]})
	s.heads[0].next = s.tails[0]
	return s
}

func (skipList *SkipList) find(key float64, findClosest bool) (ok bool, found *SkipListNode) {
	ok = false
	found = nil

	current := skipList.heads[skipList.height-1]

	for {
		next := current.next
		if next.key == key {
			ok = true
			found = next
			return
		} else if next.key < key {
			current = current.next
		} else if next.key > key {
			if current.below != nil {
				current = current.below
			} else {
				if findClosest {
					ok = true
					found = current
				} else {
					ok = false
				}
				return
			}
		}
	}
}

func (skipList *SkipList) Get(key float64) SkipListValue {
	_, elem := skipList.find(key, false)
	return elem.value
}

func (skipList *SkipList) Add(key float64, value SkipListValue) {
	ok, closestNode := skipList.find(key, true)

	if !ok {
		panic("Error when trying to find closest node")
	}

	//if node already exists, update value
	if closestNode.key == key {
		closestNode.value = value
		//update values on all levels
		for closestNode.below != nil {
			closestNode = closestNode.below
			closestNode.value = value
		}
		return
	}

	level := roll()

	//create new level if necessary
	if level > skipList.height {
		level = skipList.height + 1
		skipList.height = level

		skipList.heads[level-1] = &SkipListNode{key: math.Inf(-1), below: skipList.heads[level-2]}
		skipList.heads[level-2].above = skipList.heads[level-1]
		skipList.tails[level-1] = &SkipListNode{key: math.Inf(1), below: skipList.tails[level-2], previous: skipList.heads[level-1]}
		skipList.heads[level-1].next = skipList.tails[level-1]
		skipList.tails[level-2].above = skipList.tails[level-1]
	}

	var lastNewNode *SkipListNode
	lastNewNode = nil

	for i := 0; i < level; i++ {

		newNode := &SkipListNode{previous: closestNode, next: closestNode.next, below: lastNewNode, above: nil, key: key, value: value}
		newNode.next.previous = newNode
		closestNode.next = newNode
		if lastNewNode != nil {
			lastNewNode.above = newNode
		}
		lastNewNode = newNode

		if i != level-1 { //there is no need to find closest node above if this is the last level
			for closestNode.above == nil {
				if closestNode.previous != nil {
					closestNode = closestNode.previous
				}
			}

			closestNode = closestNode.above
		}
	}

	skipList.size++
}

func (skipList *SkipList) Remove(key float64) bool {
	ok, found := skipList.find(key, false)
	if !ok {
		return false
	}

	found.previous.next = found.next
	found.next.previous = found.previous
	for found.below != nil {
		found = found.below
		found.previous.next = found.next
		found.next.previous = found.previous
	}
	skipList.size--
	return true
}

func (skipList *SkipList) LogicallyRemove(key float64) bool {
	ok, found := skipList.find(key, false)
	if !ok {
		return false
	}
	newValue := found.value
	newValue.tombstone = false
	skipList.Add(key, newValue)
	return true
}
