package main

import (
	"fmt"
)


type Node struct {
	key   string
	value []byte
	next  *Node
}


func CreateNode(data string, value []byte) *Node {
	n := Node{
		key:   data,
		value: value,
		next:  nil,
	}
	return &n
}


type LinkedList struct {
	length int
	head *Node
	tail *Node
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
	return  &cache
}

// Add - dodavanje novog (na pocetak liste)
func (c *Cache) Add(n *Node) {
	l := c.linkedList

	// provjera da li ovakav cvor vec postoji u cache-u
	_, ok := c.mapOfData[n.key]
	if ok == true {
		current := l.head
		if current.key == n.key {  // ako pretrazujemo posljednji dodani
			delete(c.mapOfData, n.key)
			c.mapOfData[n.key] = n.value
			return
		}
		for current.next.key != n.key {  // ide do elementa koji je prije onog koji se trazi
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


//func main()  {
//	fmt.Println("Pocetak...")
//
//	cache := CreateCache(4)
//
//	node := CreateNode("Katarina", []byte("necega"))
//	cache.Add(node)
//	node = CreateNode("Milica", []byte("nesto"))
//	cache.Add(node)
//	node = CreateNode("Bojan", []byte("blabla"))
//	cache.Add(node)
//	node = CreateNode("Sara", []byte("nesto"))
//	cache.Add(node)
//	node = CreateNode("Sara" , []byte("0"))
//	cache.Add(node)
//	node = CreateNode("Mico", []byte("necega"))
//	cache.Add(node)
//
//	cache.Print()
//}
