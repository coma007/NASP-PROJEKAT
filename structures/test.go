package main

import (
	"fmt"
	"time"
)

type System struct {
	wal         *Wal
	memTable    *MemTable
	cache       *Cache
	lsm         *LSM
	tokenBucket *TokenBucket
	config 		*Config
}

func (s *System) Init() {
	s.config = GetSystemConfig()
	s.wal = CreateWal(WAL_PATH)
	s.memTable = CreateMemTable(s.config.MemTableParameters.SkipListMaxHeight,
		uint(s.config.MemTableParameters.MaxMemTableSize),
		uint(s.config.MemTableParameters.MemTableThreshold))
	s.cache = CreateCache(s.config.CacheParameters.CacheMaxData)
	s.lsm = CreateLsm(s.config.LSMParameters.LSMMaxLevel, s.config.LSMParameters.LSMLevelSize)
	rate := int64(s.config.TokenBucketParameters.TokenBucketInterval)
	s.tokenBucket = NewTokenBucket(rate, s.config.TokenBucketParameters.TokenBucketMaxTokens)

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
	s.memTable.Add(key, value, tombstone)
	cacheNode := CreateNode(key, value)
	s.cache.Add(cacheNode)

	if s.memTable.CheckFlush() {
		s.memTable.Flush()
		s.wal.RemoveSegments()
		s.lsm.DoCompaction("data/sstable/", 1)
		s.memTable = CreateMemTable(s.config.MemTableParameters.SkipListMaxHeight,
			uint(s.config.MemTableParameters.MaxMemTableSize),
			uint(s.config.MemTableParameters.MemTableThreshold))
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
	// TODO zasto je zakomentarisano
	//ok, value = s.cache.Get(key)
	//if ok {
	//	return true, value
	//}
	ok, value = SearchThroughSSTables(key, s.config.LSMParameters.LSMMaxLevel)
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


func (s *System) Edit(key string, data string)  {
	value := []byte(data)
	s.memTable.Change(key, value, false)
	elem := Element{
		key:       key,
		value:     value,
		next:      nil,
		timestamp: time.Now().String(),
		tombstone: false,
		checksum:  CRC32(value),
	}

	s.wal.Put(&elem)

	cacheNode := CacheNode{
		key:   key,
		value: value,
		next:  nil,
	}
	s.cache.Add(&cacheNode)

}


func main() {
	system := new(System)
	system.Init()
	system.Put("Milica", "Maca", false)
	system.Put("ad", "Peccca", false)
	system.Put("aa", "Macca", false)
	_, value := system.Get("aa")
	fmt.Println("value")
	fmt.Println(value)
	_, value = system.Get("aa")
	fmt.Println("value")
	fmt.Println(value)
	fmt.Println("delete")
	fmt.Println(system.Delete("aa"))
	fmt.Println("opet get")
	fmt.Println(system.Get("aa"))

	// EDIT
	//system.Edit("aa", "tralala")
	//_, value = system.Get("aa")
	//fmt.Println("Testiranje edita: ")
	//fmt.Println(value)
}
