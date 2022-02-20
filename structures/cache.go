package main

import (
	"fmt"
)

type CacheNode struct {
	key   string
	value []byte
	next  *CacheNode
}

func CreateNode(data string, value []byte) *CacheNode {
	n := CacheNode{
		key:   data,
		value: value,
		next:  nil,
	}
	return &n
}

type LinkedList struct {
	length    int
	head      *CacheNode
	tail      *CacheNode
	maxLength int
}

type Cache struct {
	linkedList *LinkedList
	mapOfData  map[string][]byte
}

func CreateCache(max int) *Cache {
	l := LinkedList{
		length:    0,
		head:      nil,
		tail:      nil,
		maxLength: max,
	}

	m := make(map[string][]byte)

	cache := Cache{
		linkedList: &l,
		mapOfData:  m,
	}
	return &cache
}

// Add - dodavanje novog (na pocetak liste)
func (c *Cache) Add(n *CacheNode) {
	l := c.linkedList

	// provjera da li ovakav cvor vec postoji u cache-u
	_, ok := c.mapOfData[n.key]
	if ok == true {
		current := l.head
		if current.key == n.key { // ako pretrazujemo posljednji dodani
			delete(c.mapOfData, n.key)
			c.mapOfData[n.key] = n.value
			return
		}
		for current.next.key != n.key { // ide do elementa koji je prije onog koji se trazi
			current = current.next
		}

		currentPrevious := current
		current = current.next
		nextOfCurrent := current.next
		current.next = nil
		head := l.head
		l.head = current
		l.head.next = head
		currentPrevious.next = nextOfCurrent

		// u slucaju da se pristupalo izmijenjenom cvoru
		delete(c.mapOfData, n.key)
		c.mapOfData[n.key] = n.value
		return
	}

	// ako ne postoji kljuc
	c.mapOfData[n.key] = n.value

	if l.length == l.maxLength {
		head := l.head
		l.head = n
		n.next = head

		current := l.head.next

		// prolazak kroz listu dok se ne dodje do pretposljednjeg elementa
		for current.next.next != nil {
			current = current.next
		}
		l.tail = current
		delete(c.mapOfData, l.tail.next.key)
		l.tail.next = nil

	} else {
		if l.head == nil {
			l.head = n
			l.tail = n
			l.length++
		} else {
			// dodajemo na pocetak liste
			head := l.head
			l.head = n
			l.head.next = head
			l.length++
		}
	}
}

func (c *Cache) Print() {
	l := c.linkedList
	fmt.Println("\nSpregunta lista")

	current := l.head
	fmt.Println(current.key)

	for (current.next) != nil {
		fmt.Println(current.next.key)
		current = current.next
	}

	fmt.Println("\nMapa")
	mapa := c.mapOfData
	for key, value := range mapa {
		fmt.Println("Kljuc: ", key, ", value: ", value)
	}
}

func (c *Cache) DeleteNode(n *CacheNode) {
	_, ok := c.mapOfData[n.key]
	l := c.linkedList

	if ok == true {
		delete(c.mapOfData, n.key)
		current := l.head
		if current.key == n.key {
			l.head = current.next
			l.length--
			return
		}
		// ako ne brisemo head
		previous := current
		current = current.next
		next := current.next
		for current != nil {
			if current.key == n.key {
				previous.next = next
				l.length--
				return
			}
			previous = current
			current = current.next
			next = current.next
		}
	}
}

func (c *Cache) Get(key string) (bool, []byte) {
	current := c.linkedList.head
	for current.key != key {
		if current.key == c.linkedList.tail.key {
			return false, nil
		}
		current = current.next
	}
	return true, current.value
}

//func main()  {
//	fmt.Println("Pocetak...")
//
//	cache := CreateCache(4)
//
//	node := CreateNode("Katarina", []byte("necega"))
//	cache.Add(node)
//	node3 := CreateNode("Milica", []byte("nesto"))
//	cache.Add(node3)
//	node2 := CreateNode("Bojan", []byte("blabla"))
//	cache.Add(node2)
//	node = CreateNode("Sara", []byte("nesto"))
//	cache.Add(node)
//	node = CreateNode("Sara" , []byte("0"))
//	cache.Add(node)
//	node4 := CreateNode("Mico", []byte("necega"))
//	cache.Add(node4)
//
//	cache.Print()
//
//	cache.DeleteNode(node3)
//
//	cache.Print()
//}
