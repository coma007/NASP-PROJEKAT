package main

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"time"
	"unsafe"
)

const (
	WAL_PATH = "data/wal/"

	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE

	SEGMENT_CAPACITY = 50
	LOW_WATER_MARK   = 0
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

////////////////////////

type Segment struct {
	index uint64
	data []byte
	size uint64
	capacity uint64

}

func (s *Segment) Index() uint64 {
	return s.index
}

func (s *Segment) Data() []byte {
	return s.data
}

func (s *Segment) addData(elemData []byte) int {
	for i := 0; i < len(elemData); i++ {
		if s.size > s.capacity {
			return i
		}
		s.data = append(s.data, elemData[i])
		s.size++
	}
	return -1
}

func (s *Segment) Dump(walPath string) {

	path := walPath + "wal" + strconv.FormatUint(s.index, 10) + ".gob"
	nwf, _ := os.Create(path)
	nwf.Close()

	file, _ := os.OpenFile(path, os.O_RDWR, 0666)
	defer file.Close()
	encoder := gob.NewEncoder(file)
	fmt.Println(s.data)
	err := encoder.Encode(s.data)
	if err != nil {
		fmt.Println(err)
	}

}

/////////////////////////


type Wal struct {
	path string // data/wal
	lowWaterMark uint
	segmentSize uint
	segments []*Segment
	currentSegment *Segment
}

func (w *Wal) CurrentSegment() *Segment {
	return w.currentSegment
}


func (w *Wal) Path() string {
	return w.path
}


func CreateWal(path string) *Wal{
	wal := Wal{path, LOW_WATER_MARK, SEGMENT_CAPACITY,  make([]*Segment, 0), &Segment{
		index:    0,
		data:     nil,
		size:     0,
		capacity: SEGMENT_CAPACITY,
	}}
	return &wal
}

func (w *Wal) Dump() {
	w.currentSegment.Dump(w.path)
}

func (w *Wal) NewSegment() {
	newSegm := Segment{
		index:    w.currentSegment.index + 1,
		data:     make([]byte, w.currentSegment.capacity),
		size:     0,
		capacity: w.currentSegment.capacity,
	}
	w.Dump()
	w.segments = append(w.segments, &newSegm)
	w.currentSegment = &newSegm
	w.Dump()

}

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

func (w  *Wal) Put(elem *Element) {

	crc := elem.checksum
	timestamp := []byte(strconv.FormatInt(time.Now().Unix(), 10))
	tombstone := []byte(strconv.FormatBool(elem.tombstone))
	key_size := make([]byte, KEY_SIZE)
	value_size := make([]byte, VALUE_SIZE)
	switch KEY_SIZE {
	case 4:
		binary.LittleEndian.PutUint32(key_size, uint32(unsafe.Sizeof(elem.key)))
	case 8:
		binary.LittleEndian.PutUint64(key_size, uint64(unsafe.Sizeof(elem.key)))
	}
	switch VALUE_SIZE {
	case 4:
		binary.LittleEndian.PutUint32(value_size, uint32(unsafe.Sizeof(elem.value)))
	case 8:
		binary.LittleEndian.PutUint64(value_size, uint64(unsafe.Sizeof(elem.value)))
	}
	key := []byte(elem.key)
	value := elem.value

	elemData := []byte{}
	elemData = append(elemData, crc...)
	elemData = append(elemData, timestamp...)
	elemData = append(elemData, tombstone...)
	elemData = append(elemData, key_size...)
	elemData = append(elemData, value_size...)
	elemData = append(elemData, key...)
	elemData = append(elemData, value...)

	fmt.Println(len(elemData))
	start := 0
	for start >= 0 {
		start = w.CurrentSegment().addData(elemData[start:])
		if start != -1 {
			w.NewSegment()
		}
	}

}

// TODO recover from wal dir
// TODO remove segments from 0 to lowWaterMark


func main() {
	w:= CreateWal(WAL_PATH)
	w.Put(&Element{
		key:       "keke",
		value:     []byte("asdd"),
		next:      nil,
		timestamp: "",
		tombstone: false,
		checksum:  nil,
	})
}
