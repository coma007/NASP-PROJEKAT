package main

import (
	"hash/crc32"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE

	SEGMENT_SIZE = 2
	LOW_WATER_MARK = 0
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Segment struct {
	index uint
	data []byte
	// path je data/wal/wal_index.gob
}

type Wal struct {
	path string // data/wal
	currentFile uint
	lowWaterMark uint
	segmentSize uint
	segments []*Segment
}

func CreateWal(path string) *Wal{
	wal := Wal{path, 0, SEGMENT_SIZE, LOW_WATER_MARK, make([]*Segment, 0)}
	return &wal
}

func Put(key string, value []byte) {
	// TODO Put in WAL
}

// TODO write segment to wal dir
// TODO recover from wal dir
// TODO remove segments from 0 to lowWaterMark
// TODO read segments from wal dir or from memory



