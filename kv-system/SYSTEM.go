package kv_system

import (
	"Key-Value-Engine/config"
	"Key-Value-Engine/kv-system/structures"
	"time"
)

type System struct {
	wal         *structures.Wal
	memTable    *structures.MemTable
	cache       *structures.Cache
	lsm         *structures.LSM
	tokenBucket *structures.TokenBucket
	config 		*config.Config
}

func (s *System) Init() {
	s.config = config.GetSystemConfig()
	s.wal = structures.CreateWal(structures.WAL_PATH)
	s.memTable = structures.CreateMemTable(s.config.MemTableParameters.SkipListMaxHeight,
		uint(s.config.MemTableParameters.MaxMemTableSize),
		uint(s.config.MemTableParameters.MemTableThreshold))
	s.cache = structures.CreateCache(s.config.CacheParameters.CacheMaxData)
	s.lsm = structures.CreateLsm(s.config.LSMParameters.LSMMaxLevel, s.config.LSMParameters.LSMLevelSize)
	rate := int64(s.config.TokenBucketParameters.TokenBucketInterval)
	s.tokenBucket = structures.NewTokenBucket(rate, s.config.TokenBucketParameters.TokenBucketMaxTokens)
}

func (s *System) Put(key string, value []byte, tombstone bool) bool {
	request := s.tokenBucket.CheckRequest()
	if !request {
		return false
	}

	elem := structures.Element{
		Key:       key,
		Value:     value,
		Next:      nil,
		Timestamp: time.Now().String(),
		Tombstone: tombstone,
		Checksum:  structures.CRC32(value),
	}
	s.wal.Put(&elem)
	s.memTable.Add(key, value, tombstone)
	cacheNode := structures.CreateNode(key, value)
	s.cache.Add(cacheNode)

	if s.memTable.CheckFlush() {
		s.memTable.Flush()
		s.wal.RemoveSegments()
		s.lsm.DoCompaction("kv-system/data/sstable/", 1)
		s.memTable = structures.CreateMemTable(s.config.MemTableParameters.SkipListMaxHeight,
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
	ok, value = s.cache.Get(key)
	if ok {
		return true, value
	}
	ok, value = structures.SearchThroughSSTables(key, s.config.LSMParameters.LSMMaxLevel)
	if ok {
		return true, value
	}
	return false, nil
}

func (s *System) Delete(key string) bool {
	if s.memTable.Remove(key) {
		s.cache.DeleteNode(structures.CreateNode(key, nil))
		return true
	}
	ok, value := s.Get(key)
	if !ok {
		return false
	}
	s.Put(key, value, true)
	s.cache.DeleteNode(structures.CreateNode(key, value))
	return true
}


func (s *System) Edit(key string, value []byte) bool {
	request := s.tokenBucket.CheckRequest()
	if !request {
		return false
	}
	s.memTable.Change(key, value, false)
	elem := structures.Element{
		Key:       key,
		Value:     value,
		Next:      nil,
		Timestamp: time.Now().String(),
		Tombstone: false,
		Checksum:  structures.CRC32(value),
	}

	s.wal.Put(&elem)

	cacheNode := structures.CacheNode{
		Key:   key,
		Value: value,
		Next:  nil,
	}
	s.cache.Add(&cacheNode)

	return true
}

