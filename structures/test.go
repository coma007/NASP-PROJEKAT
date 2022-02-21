package main

import (
	"time"
)

type System struct {
	wal         *Wal
	memTable    *MemTable
	cache       *Cache
	lsm         *LSM
	tokenBucket *TokenBucket
}

func (s *System) Init() {
	s.wal = CreateWal(WAL_PATH)
	s.memTable = CreateMemTable(5)
	s.cache = CreateCache(5)
	s.lsm = CreateLsm(3, 2)
	rate := int64(1000)
	s.tokenBucket = NewTokenBucket(rate, 100)

}

func (s *System) Put(key string, data string, tombstone bool) bool {
	request := s.tokenBucket.CheckRequest()
	if !request {
		return false
	}

	value := []byte(data)
	elem := Element{
		key:       key,
		value:     value,
		next:      nil,
		timestamp: time.Now().String(),
		tombstone: tombstone,
		checksum:  CRC32(value),
	}
	s.wal.Put(&elem)
	s.memTable.Add(key, value)
	cacheNode := CreateNode(key, value)
	s.cache.Add(cacheNode)

	if s.memTable.size >= 3 {
		s.memTable.Flush() // TODO dodati pravljenje merkle stabla
		// TODO isto dodati kod
		s.wal.RemoveSegments()
		s.lsm.DoCompaction("./data/sstable/", 1)
		s.memTable = CreateMemTable(5)
	}

	return true
}

func (s *System) Get(key string) (bool, []byte) {
	ok, deleted, value := s.memTable.Find(key)
	if ok && deleted {
		return false, nil
	} else if ok {
		return true, value
	}
	ok, value = s.cache.Get(key)
	if ok {
		return true, value
	}
	ok, value = SearchThroughSSTables(key)
	if ok {
		return true, value
	}
	return false, nil
}

func (s *System) Delete(key string) bool {
	if s.memTable.Remove(key) {
		s.cache.DeleteNode(CreateNode(key, nil))
		return true
	}
	ok, value := s.Get(key)
	if !ok {
		return false
	}
	s.Put(key, string(value), true)
	s.cache.DeleteNode(CreateNode(key, value))
	return true
}

func main() {
	system := new(System)
	system.Init()
	system.Put("Milica", "Maca", false)
	system.Put("ad", "Peccca", false)
	system.Put("aa", "Macca", false)
	system.Put("laaa", "Maca", false)
	system.Put("bee", "Peccca", false)
	system.Put("ccc", "Macca", false)
	system.Put("Jecika", "Maca", false)
	system.Put("Pecika", "Peccca", false)
	system.Put("Necika", "Macca", false)
	//_, value := system.Get("aa")
	//fmt.Println("value")
	//fmt.Println(value)
	//_, value = system.Get("aa")
	//fmt.Println("value")
	//fmt.Println(value)
	//fmt.Println("delete")
	//fmt.Println(system.Delete("aa"))
	//fmt.Println("opet get")
	//fmt.Println(system.Get("aa"))
}
