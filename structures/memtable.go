package main

type MemTable struct {
	data SkipList
	size uint
	threshold uint
	maxSize uint
}

func CreateMemTable(height int, maxSize, threshold uint) *MemTable {
	sl := CreateSkipList(height)
	mt := MemTable{*sl, 0, threshold, maxSize}
	return &mt
}

func (mt *MemTable) Add(key string, value []byte, tombstone bool) {
	mt.size += 1
	mt.data.Add(key, value, tombstone)
}

func (mt *MemTable) Remove(key string) bool {
	element := mt.data.Remove(key)
	if element == nil {
		return false
	}
	return true
}

func (mt *MemTable) Change(key string, value []byte, tombstone bool) {
	node := mt.data.Find(key)
	if node == nil {
		mt.Add(key, value, tombstone)
	} else {
		node.value = value
	}
}

func (mt *MemTable) Find(key string) (ok, deleted bool, value []byte) {
	node := mt.data.Find(key)
	if node == nil {
		ok = false
		deleted = false
		value = nil
	} else if node.tombstone {
		ok = true
		deleted = true
		value = nil
	} else {
		ok = true
		deleted = false
		value = node.value
	}
	return
}

func (mt *MemTable) Size() uint {
	return mt.size
}

func (mt *MemTable) CheckFlush() bool {
	if (float64(mt.size)/float64(mt.maxSize)) * 100 >= float64(mt.threshold) {
		return true
	}
	return false
}

func (mt *MemTable) Flush() {
	filename := findSSTableFilename("1")
	CreateSStable(*mt, filename)
}
