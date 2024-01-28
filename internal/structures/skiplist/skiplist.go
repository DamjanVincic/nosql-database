package skiplist

import (
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"math/rand"
)

const (
	maxHeight        = 10
	positiveInfinity = "+∞"
	negativeInfinity = "-∞"
)

type SkipListNode struct {
	data     *models.Data
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

func CreateSkipList() *SkipList {
	s := SkipList{
		heads:  make([]*SkipListNode, 0),
		tails:  make([]*SkipListNode, 0),
		height: 1,
		size:   0,
	}
	s.heads = append(s.heads, &SkipListNode{data: &models.Data{Key: negativeInfinity}})
	s.tails = append(s.tails, &SkipListNode{data: &models.Data{Key: positiveInfinity}, previous: s.heads[0]})
	s.heads[0].next = s.tails[0]
	return &s
}

func (skipList *SkipList) find(key string, findClosest bool) (found *SkipListNode) {
	found = nil

	current := skipList.heads[skipList.height-1] //starting search from top level

	for {
		next := current.next
		if next.data.Key == key {
			found = next
			return
		} else if next.data.Key < key && next.data.Key != positiveInfinity {
			current = current.next
		} else if next.data.Key > key || next.data.Key == positiveInfinity { //key is not on this level, if possible go below
			if current.below != nil {
				current = current.below
			} else { //key does not exist
				if findClosest {
					found = current
				}
				return
			}
		}
	}
}

func (skipList *SkipList) Get(key string) (found *models.Data) {
	elem := skipList.find(key, false)
	if elem != nil {
		found = elem.data
	} else {
		found = nil
	}
	return
}

func (skipList *SkipList) Put(key string, value []byte, tombstone bool, timestamp uint64) {
	closestNode := skipList.find(key, true)

	skipListValue := &models.Data{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	//if node already exists, update values in Value field
	if closestNode.data.Key == key {
		closestNode.data = skipListValue
		return
	}

	level := roll()

	//create new level if necessary
	if level > skipList.height {
		level = skipList.height + 1
		skipList.height = level

		skipList.heads = append(skipList.heads, &SkipListNode{data: &models.Data{Key: negativeInfinity}, below: skipList.heads[level-2]})
		skipList.heads[level-2].above = skipList.heads[level-1]
		skipList.tails = append(skipList.tails, &SkipListNode{data: &models.Data{Key: positiveInfinity}, below: skipList.tails[level-2], previous: skipList.heads[level-1]})
		skipList.heads[level-1].next = skipList.tails[level-1]
		skipList.tails[level-2].above = skipList.tails[level-1]
	}

	var lastNewNode *SkipListNode
	lastNewNode = nil

	for i := 0; i < level; i++ { //create new node on all levels needed
		newNode := &SkipListNode{previous: closestNode, next: closestNode.next, below: lastNewNode, above: nil, data: skipListValue}
		newNode.next.previous = newNode
		closestNode.next = newNode
		if lastNewNode != nil { //connect new node to nodes below
			lastNewNode.above = newNode
		}
		lastNewNode = newNode

		//find the closest node on level above
		if i != level-1 { //there is no need to find the closest node above if this is the last level
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

func (skipList *SkipList) Delete(key string) {
	found := skipList.find(key, false)
	if found == nil {
		return
	}

	//remove node on the highest level
	found.previous.next = found.next
	found.next.previous = found.previous
	for found.below != nil { //remove node on lower levels
		found = found.below
		found.previous.next = found.next
		found.next.previous = found.previous
	}
	skipList.size--
}

// returns all values sorted
func (skipList *SkipList) GetSorted() []*models.Data {
	entries := make([]*models.Data, 0)
	current := skipList.heads[0]
	for current.next.data.Key != positiveInfinity {
		current = current.next
		entries = append(entries, current.data)
	}
	return entries
}

func (skipList *SkipList) Size() int {
	return skipList.size
}
