package structures

import (
	"fmt"
)

type CacheNode struct {
	Key   string
	Value []byte
	Next  *CacheNode
}

func CreateNode(data string, value []byte) *CacheNode {
	n := CacheNode{
		Key:   data,
		Value: value,
		Next:  nil,
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
	_, ok := c.mapOfData[n.Key]
	if ok == true {
		current := l.head
		if current.Key == n.Key { // ako pretrazujemo posljednji dodani
			delete(c.mapOfData, n.Key)
			c.mapOfData[n.Key] = n.Value
			return
		}
		for current.Next.Key != n.Key { // ide do elementa koji je prije onog koji se trazi
			current = current.Next
		}

		currentPrevious := current
		current = current.Next
		nextOfCurrent := current.Next
		current.Next = nil
		head := l.head
		l.head = current
		l.head.Next = head
		currentPrevious.Next = nextOfCurrent

		// u slucaju da se pristupalo izmijenjenom cvoru
		delete(c.mapOfData, n.Key)
		c.mapOfData[n.Key] = n.Value
		return
	}

	// ako ne postoji kljuc
	c.mapOfData[n.Key] = n.Value

	if l.length == l.maxLength {
		head := l.head
		l.head = n
		n.Next = head

		current := l.head.Next

		// prolazak kroz listu dok se ne dodje do pretposljednjeg elementa
		for current.Next.Next != nil {
			current = current.Next
		}
		l.tail = current
		delete(c.mapOfData, l.tail.Next.Key)
		l.tail.Next = nil

	} else {
		if l.head == nil {
			l.head = n
			l.tail = n
			l.length++
		} else {
			// dodajemo na pocetak liste
			head := l.head
			l.head = n
			l.head.Next = head
			l.length++
		}
	}
}

func (c *Cache) Print() {
	l := c.linkedList
	fmt.Println("\nSpregunta lista")

	current := l.head
	fmt.Println(current.Key)

	for (current.Next) != nil {
		fmt.Println(current.Next.Key)
		current = current.Next
	}

	fmt.Println("\nMapa")
	mapa := c.mapOfData
	for key, value := range mapa {
		fmt.Println("Kljuc: ", key, ", Value: ", value)
	}
}

func (c *Cache) DeleteNode(n *CacheNode) {
	_, ok := c.mapOfData[n.Key]
	l := c.linkedList

	if ok == true {
		delete(c.mapOfData, n.Key)
		current := l.head
		if current.Key == n.Key {
			l.head = current.Next
			l.length--
			return
		}
		// ako ne brisemo head
		previous := current
		current = current.Next
		next := current.Next
		for current != nil {
			if current.Key == n.Key {
				previous.Next = next
				l.length--
				return
			}
			previous = current
			current = current.Next
			next = current.Next
		}
	}
}

func (c *Cache) Get(key string) (bool, []byte) {
	current := c.linkedList.head
	for current.Key != key {
		if current.Key == c.linkedList.tail.Key {
			return false, nil
		}
		current = current.Next
	}
	return true, current.Value
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
