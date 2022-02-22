package structures


type Element struct {
	Key   string
	Value     []byte
	Next      []*Element
	Timestamp string
	Tombstone bool
	Checksum  uint32
}
