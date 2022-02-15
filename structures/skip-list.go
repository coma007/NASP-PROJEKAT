package main

import (
	"math/rand"
)

// SkipListNode

type SkipListNode struct {
	key       string
	value     []byte
	next      []*SkipListNode
}

func (skiplist *SkipListNode) Key() string {
	return skiplist.key
}

func (skiplist *SkipListNode) Value() []byte {
	return skiplist.value
}

////////////////////////////////////////////


// SkipList


type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

func  CreateSkipList(maxHeight int) *SkipList {
	root := SkipListNode{"head", nil, make([]*SkipListNode, maxHeight+1)}
	skiplist := SkipList{maxHeight, 1, 1, &root}
	return &skiplist
}

func (skiplist *SkipList) roll() int {
	level := 0 // alwasy start from level 0

	// We roll until we don't get 1 from rand function and we did not
	// outgrow maxHeight. BUT rand can give us 0, and if that is the case
	// than we will just increase level, and wait for 1 from rand!
	for ; rand.Int31n(2) == 1; level++ {
		if level > skiplist.height {
			// When we get 1 from rand function and we did not
			// outgrow maxHeight, that number becomes new height
			skiplist.height = level
			return level
		}
	}
	return level
}

func (skiplist *SkipList) Add(key string, value []byte) *SkipListNode{

	level := skiplist.roll()
	node := &SkipListNode{key, value,  make([]*SkipListNode, level+1)}

	current := skiplist.head
	for i := skiplist.height-1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if next == nil || next.key > key {
				break
			}
		}
		if i <= level {
			skiplist.size++
			node.next[i] = next
			current.next[i] = node
		}
	}
	return node
}

func (skiplist *SkipList) Find(key string) *SkipListNode {

	current := skiplist.head
	for i := skiplist.height-1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if next == nil || current.key > key {
				break
			}
			if current.key == key {
				return current
			}
		}
	}

	return nil
}

func (skiplist *SkipList) Remove(key string) *SkipListNode {

	current := skiplist.head
	for i := skiplist.height-1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if next == nil || current.key > key {
				break
			}
			if current.key == key {
				// kod memetablea promijeniti tombstone ?
				// TODO dodati tombstone i time
				skiplist.size--
				tmp := current
				current = current.next[i]
				return tmp
			}
		}
	}

	return nil


}

func main() {


		sl := CreateSkipList(25)
		sl.Add("aca", []byte("123"))
		sl.Add("djura", []byte("kas"))
		ga := sl.Find("aca")
		sl.Remove(ga.Key())
		sl.roll()
}
