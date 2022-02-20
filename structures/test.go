package main

import "time"

type System struct {
	wal *Wal
	memTable *MemTable
	cache *Cache
	lsm *LSM
	tokenBucket *TokenBucket
}

func (s *System) Init() {
	s.wal = CreateWal(WAL_PATH)
	s.memTable = CreateMemTable(5)
	s.cache = CreateCache(5)
	s.lsm = CreateLsm(4, 4)
	rate := int64(5)
	s.tokenBucket = NewTokenBucket(rate, 10)

}


func (s *System) Put(key string, data string) bool {
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
		tombstone: false,
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

func main() {
	system := new(System)
	system.Init()
	system.Put("Milica", "Maca")
}
