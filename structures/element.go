package main


type Element struct {
	key       string
	value     []byte
	next      []*Element
	timestamp string
	tombstone bool
	checksum  uint32
}

func (elem *Element) Key() string {
	return elem.key
}

func (elem *Element) Value() []byte {
	return elem.value
}
