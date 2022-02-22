package kv_system

import (
	"Key-Value-Engine/config"
	"Key-Value-Engine/kv-system/structures"
	"time"
)

type System struct {
	wal         *structures.Wal
	memTable *structures.MemTable
	Cache    *structures.Cache
	lsm      *structures.LSM
	tokenBucket *structures.TokenBucket
	Config      *config.Config
}

func (s *System) Init() {
	s.Config = config.GetSystemConfig()
	s.wal = structures.CreateWal(structures.WAL_PATH)
	s.memTable = structures.CreateMemTable(s.Config.MemTableParameters.SkipListMaxHeight,
		uint(s.Config.MemTableParameters.MaxMemTableSize),
		uint(s.Config.MemTableParameters.MemTableThreshold))
	s.Cache = structures.CreateCache(s.Config.CacheParameters.CacheMaxData)
	s.lsm = structures.CreateLsm(s.Config.LSMParameters.LSMMaxLevel, s.Config.LSMParameters.LSMLevelSize)
	rate := int64(s.Config.TokenBucketParameters.TokenBucketInterval)
	s.tokenBucket = structures.NewTokenBucket(rate, s.Config.TokenBucketParameters.TokenBucketMaxTokens)
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
	s.Cache.Add(key, value)

	if s.memTable.CheckFlush() {
		s.memTable.Flush()
		s.wal.RemoveSegments()
		s.lsm.DoCompaction("kv-system/data/sstable/", 1)
		s.memTable = structures.CreateMemTable(s.Config.MemTableParameters.SkipListMaxHeight,
			uint(s.Config.MemTableParameters.MaxMemTableSize),
			uint(s.Config.MemTableParameters.MemTableThreshold))
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
	ok, value = s.Cache.Get(key)
	if ok {
		return true, value
	}
	ok, value = structures.SearchThroughSSTables(key, s.Config.LSMParameters.LSMMaxLevel)
	if ok {
		return true, value
	}
	return false, nil
}

func (s *System) Delete(key string) bool {
	if s.memTable.Remove(key) {
		s.Cache.DeleteNode(key)
		return true
	}
	ok, value := s.Get(key)
	if !ok {
		return false
	}
	s.Put(key, value, true)
	s.Cache.DeleteNode(key)
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

	s.Cache.Add(key, value)

	return true
}

