package structures

import (
	"math/rand"
	"time"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *Element
}

func CreateSkipList(maxHeight int) *SkipList {
	bytes := []byte("head")
	crc := CRC32(bytes)
	root := Element{"head", nil, make([]*Element, maxHeight+1), time.Now().String(),
		false, crc}
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

func (skiplist *SkipList) Add(key string, value []byte, tombstone bool) *Element {

	level := skiplist.roll()
	bytes := []byte(key)
	crc := CRC32(bytes)
	node := &Element{key, value, make([]*Element, level+1), time.Now().String(),
		tombstone, crc}
	//current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		current := skiplist.head
		next := current.Next[i]
		for next != nil {
			if next == nil || next.Key > key {
				break
			}
			current = next
			next = current.Next[i]

		}
		if i <= level {
			skiplist.size++
			node.Next[i] = next
			current.Next[i] = node
		}
	}
	return node
}

func (skiplist *SkipList) Find(key string) *Element {

	current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		next := current.Next[i]
		for next != nil {
			current = next
			next = current.Next[i]
			if current.Key == key {
				return current
			}
			if next == nil || current.Key > key {
				break
			}
		}
	}

	return nil
}

func (skiplist *SkipList) Remove(key string) *Element {

	current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		next := current.Next[i]
		for next != nil {
			current = next
			next = current.Next[i]
			if current.Key == key {
				current.Tombstone = true
				current.Timestamp = time.Now().String()
				tmp := current
				current = current.Next[i]
				return tmp
			}
			if next == nil || current.Key > key {
				break
			}
		}
	}

	return nil

}

