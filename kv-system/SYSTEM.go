package kv_system

import (
	"Key-Value-Engine/config"
	"Key-Value-Engine/kv-system/structures"
	"fmt"
	"time"
)

type System struct {
	Wal      *structures.Wal
	memTable *structures.MemTable
	cache    *structures.Cache
	lsm      *structures.LSM
	TokenBucket *structures.TokenBucket
	Config      *config.Config
}

func (s *System) Init() {
	s.Config = config.GetSystemConfig()
	s.Wal = structures.CreateWal(structures.WAL_PATH)
	s.memTable = structures.CreateMemTable(s.Config.MemTableParameters.SkipListMaxHeight,
		uint(s.Config.MemTableParameters.MaxMemTableSize),
		uint(s.Config.MemTableParameters.MemTableThreshold))
	s.cache = structures.CreateCache(s.Config.CacheParameters.CacheMaxData)
	s.lsm = structures.CreateLsm(s.Config.LSMParameters.LSMMaxLevel, s.Config.LSMParameters.LSMLevelSize)
	rate := int64(s.Config.TokenBucketParameters.TokenBucketInterval)
	s.TokenBucket = structures.NewTokenBucket(rate, s.Config.TokenBucketParameters.TokenBucketMaxTokens)
}

func (s *System) Put(key string, value []byte, tombstone bool) bool {

	elem := structures.Element{
		Key:       key,
		Value:     value,
		Next:      nil,
		Timestamp: time.Now().String(),
		Tombstone: tombstone,
		Checksum:  structures.CRC32(value),
	}
	s.Wal.Put(&elem)
	s.memTable.Add(key, value, tombstone)
	s.cache.Add(key, value)

	if s.memTable.CheckFlush() {
		s.memTable.Flush()
		s.Wal.RemoveSegments()
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
		s.cache.Add(key, value)
		fmt.Println("Pronadjen u memtable-u.")
		return true, value
	}
	ok, value = s.cache.Get(key)
	if ok {
		fmt.Println("Pronadjen u cache-u.")
		s.cache.Add(key, value)
		return true, value
	}
	ok, value = structures.SearchThroughSSTables(key, s.Config.LSMParameters.LSMMaxLevel)
	if ok {
		fmt.Println("Pronadjen u sstable-u.")
		s.cache.Add(key, value)
		//if strings.Trim(string(value), " ") != "" {
		//	return true, value
		//}
		return true, value
	}
	return false, nil
}

func (s *System) Delete(key string) bool {
	if s.memTable.Remove(key) {
		s.cache.DeleteNode(key)
		return true
	}
	if s.memTable.Remove("hll-" + key) {
		s.cache.DeleteNode("hll-" + key)
		return true
	}
	if s.memTable.Remove("cms-" + key) {
		s.cache.DeleteNode("cms-" + key)
		return true
	}
	ok, value := s.Get(key)
	if !ok {
		keyHLL := "hll-" + key
		ok, value = s.Get(keyHLL)
		if !ok {
			keyCMS := "cms-" + key
			ok, value = s.Get(keyCMS)
			if !ok {
				return false
			} else {
				key = keyCMS
			}
		} else {
			key = keyHLL
		}
	}
	s.Put(key, value, true)
	s.cache.DeleteNode(key)
	return true
}


func (s *System) Edit(key string, value []byte) bool {
	s.memTable.Change(key, value, false)
	elem := structures.Element{
		Key:       key,
		Value:     value,
		Next:      nil,
		Timestamp: time.Now().String(),
		Tombstone: false,
		Checksum:  structures.CRC32(value),
	}

	s.Wal.Put(&elem)

	s.cache.Add(key, value)

	return true
}

func (s *System) GetAsString(key string) string {
	ok, val := s.Get(key)
	var value string
	if !ok {
		ok, val = s.Get("hll-" + key)
		if ok {
			hll := structures.DeserializeHLL(val)
			value = "It's a HLL with Estimation: " + fmt.Sprintf("%f", hll.Estimate())
		} else {
			ok, val = s.Get("cms-" + key)
			if ok {
				value = "It's a CMS"
			}
			if !ok {
				value = "Data with given key does not exist !"
			}
		}
	} else {
		value = string(val)
	}
	return value
}

