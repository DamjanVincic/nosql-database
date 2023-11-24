package main

import (
	"math"
	"math/rand"
)

const maxHeight = 10

type SkipListNode struct {
	key      float64
	value    []byte
	previous *SkipListNode
	next     *SkipListNode
	below    *SkipListNode
	above    *SkipListNode
}

type SkipList struct {
	heads  [maxHeight]*SkipListNode
	tails  [maxHeight]*SkipListNode
	height int
	size   int
}

func (skipList *SkipList) isEmpty() bool {
	return skipList.size == 0
}

func roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= maxHeight {
			return level
		}
	}
	return level
}

func createSkipList() SkipList {
	s := SkipList{
		heads:  [maxHeight]*SkipListNode{},
		tails:  [maxHeight]*SkipListNode{},
		height: 1,
		size:   0,
	}
	s.heads[0] = &SkipListNode{key: math.Inf(-1)}
	s.tails[0] = &SkipListNode{key: math.Inf(1), previous: s.heads[0]}
	s.heads[0].next = s.tails[0]
	return s
}

//func (skipList *SkipList)findEntryPoint()int{
//	for i:=skipList.height
//}

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

func (skipList *SkipList) get(key float64) []byte {
	_, elem := skipList.find(key, false)
	return elem.value
}

func (skipList *SkipList) add(key float64, value []byte) {
	ok, closestNode := skipList.find(key, true)

	if !ok {
		panic("Error when trying to find closest node")
	}

	if closestNode.key == key {
		closestNode.value = value
		for closestNode.below != nil {
			closestNode = closestNode.below
			closestNode.value = value
		}
		return
	}

	level := roll()
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

func (skipList *SkipList) remove(key float64) bool {
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
