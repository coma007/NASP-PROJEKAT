package main

type MemTable struct {
	data	SkipList
	size 	uint
}

func CreateMemTable(height int) *MemTable{
	sl := CreateSkipList(height)
	mt := MemTable{*sl, 0}
	return &mt
}

func (mt *MemTable) Add(key string, value []byte) {
	mt.size += 1
	mt.data.Add(key, value)
}

func (mt *MemTable) Remove(key string) {
	mt.data.Remove(key)
}

func (mt *MemTable) Change(key string, value []byte) {
	node := mt.data.Find(key)
	if node == nil {
		mt.Add(key, value)
	} else {
		node.value = value
	}
}

func (mt *MemTable) Find(key string) (ok bool, value []byte) {
	node := mt.data.Find(key)
	if node == nil {
		ok = false
		value = nil
	} else {
		ok = true
		value = node.value
	}
	return
}

func (mt *MemTable) Size() uint {
	return mt.size
}

func (mt *MemTable) Flush() {
	filename := findSSTableFilename("1")
	CreateSStable(*mt, filename)
}