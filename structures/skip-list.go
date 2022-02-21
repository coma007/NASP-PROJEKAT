package main

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
	root := Element{"head", nil, make([]*Element, maxHeight+1), time.Time{}.String(),
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
	node := &Element{key, value, make([]*Element, level+1), time.Time{}.String(),
		tombstone, crc}
	//current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		current := skiplist.head
		next := current.next[i]
		for next != nil {
			if next == nil || next.key > key {
				break
			}
			current = next
			next = current.next[i]

		}
		if i <= level {
			skiplist.size++
			node.next[i] = next
			current.next[i] = node
		}
	}
	return node
}

func (skiplist *SkipList) Find(key string) *Element {

	current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if current.key == key {
				return current
			}
			if next == nil || current.key > key {
				break
			}
		}
	}

	return nil
}

func (skiplist *SkipList) Remove(key string) *Element {

	current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if next == nil || current.key > key {
				break
			}
			if current.key == key {
				current.tombstone = true
				current.timestamp = time.Now().String()
				tmp := current
				current = current.next[i]
				return tmp
			}
		}
	}

	return nil

}

//
//func main() {
//		sl := CreateSkipList(25)
//		sl.Add("aca", []byte("123"))
//		sl.Add("djura", []byte("kas"))
//		ga := sl.Find("aca")
//		sl.Remove(ga.Key())
//		sl.roll()
//}
