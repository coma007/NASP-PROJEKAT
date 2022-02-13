package main

import(
	"math/rand"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

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

func (skiplist *SkipList) Add(key string, value []byte)  {

	level := skiplist.roll()
	node := &SkipListNode{key, value,  make([]*SkipListNode, level+1)}

	current := skiplist.head
	for i := skiplist.height-1; i >= 0; i-- {
		next := current.next[i]
		for ; next != nil; current = next {
			next = current.next[i]
			if next.key > key {
				break
			}
		}
		if i <= level {
			node.next[i] = next
			current.next[i] = node
			skiplist.size++
		}
	}

}

func main() {

		root := SkipListNode{"start", nil, make([]*SkipListNode, 2)}
		sl := SkipList{25, 1, 1, &root}
		sl.Add("aca", []byte("123"))
}
