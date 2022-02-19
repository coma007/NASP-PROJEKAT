package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	WAL_PATH = "data/wal/"

	// VELICINE SU U BAJTOVIMA
	CRC_SIZE       = 4
	TIMESTAMP_SIZE = 8
	TOMBSTONE_SIZE = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

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
		if s.size >= s.capacity {
			return i
		}
		s.data = append(s.data, elemData[i])
		s.size++
	}
	return -1
}

func (s *Segment) Dump(walPath string) {

	path := walPath + "wal" + strconv.FormatUint(s.index, 10) + ".log"
	nwf, _ := os.Create(path)
	err := nwf.Close()
	if err != nil {
		fmt.Println(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bufferedWriter := bufio.NewWriter(file)
	err = bufferedWriter.Flush()
	if err != nil {
		return
	}
	_, err = bufferedWriter.Write(s.data)
	err = bufferedWriter.Flush()
	if err != nil {
		return
	}
	//fmt.Println(s.data)
	if err != nil {
		log.Fatal(err)
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
		data:     make([]byte, 0, SEGMENT_CAPACITY),
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

	crc := make([]byte, CRC_SIZE) // 32 bit
	binary.LittleEndian.PutUint32(crc, elem.checksum)
	timestamp := make([]byte, TIMESTAMP_SIZE) // 64 bit - unsafe.Sizeof(time.Now().Unix()) size je vracalo vrijednost 8, pa bolje 64 bita nego 32
	binary.LittleEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	tombstone := make([]byte, TOMBSTONE_SIZE) // 8 bit
	switch (elem.tombstone) {
		case true: tombstone = []byte{1}
		case false: tombstone = []byte{0}
	}
	key_size := make([]byte, KEY_SIZE_SIZE)
	value_size := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(key_size, uint64(len(elem.key)))
	binary.LittleEndian.PutUint64(value_size, uint64(len(elem.value)))

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

	start := 0
	for start >= 0 {
		fmt.Println(elemData[start:])
		start = w.CurrentSegment().addData(elemData[start:])
		if start != -1 {
			w.NewSegment()
		}
	}

}


func Recover(path string) *Wal{
	wal := CreateWal(path)

	// citanje fajlova iz direktorijuma wal
	files, err := ioutil.ReadDir(WAL_PATH[:len(WAL_PATH) - 1])  // skidanje "/" sa kraja
	if err != nil {
		fmt.Println(err)
	}

	// vracanje posljednjeg segmenta u memoriju
	current := files[len(files) - 1].Name()

	// pronalazak indeksa posljednjeg dodanog segmenta
	index_str := strings.Split(current, "wal")[1]
	index_str = strings.Split(index_str, ".log")[0]
	index, err := strconv.ParseUint(index_str, 10, 64)
	if err != nil {
		fmt.Println(err)
	}

	// citanje posljednjeg dodanog fajla
	file, err := os.Open(path + current)
	if err != nil {
		fmt.Println(err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		fmt.Println(err)
	}


	currentSegment := Segment{
		index:    index,
		data:     nil,
		size:     0,
		capacity: SEGMENT_CAPACITY,
	}

	// upis podataka u memoriju
	currentSegment.addData(data)

	wal.currentSegment = &currentSegment
	wal.segments = append(wal.segments, &currentSegment)

	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}

	return wal
}


func (w *Wal) RemoveSegments() {

	files, err := ioutil.ReadDir(WAL_PATH[:len(WAL_PATH) - 1])  // skidanje "/" sa kraja
	if err != nil {
		fmt.Println(err)
	}

	for _, file := range files {
		index_str := strings.Split(file.Name(), "wal")[1]
		index_str = strings.Split(index_str, ".log")[0]
		index, err := strconv.ParseUint(index_str, 10, 64)
		if err != nil {
			fmt.Println(err)
		}
		index2 := uint(index)
		if index2 <= w.lowWaterMark {
			err = os.Remove(WAL_PATH + file.Name())
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

// TODO uklanjanje kose
// TODO prodiskutovati recovery i remove segments
// TODO wal_index.log
// TODO recovery/scan -> struktura index: putanja

// TODO recovery -> sta je rekao na predavanjima:
// npr. upisuju se 3 podatka u wal, zatim 4. i svi su u istom segmentu,
// ova prva tri su stigla da se upisu na disk a 4. nije
// prilikom recovery-a se uzimaju podaci iz tekuceg segmenta, provjerava se da li su na disku
// moze npr da se segment strpa u memoriju, i kad se izvrsi provjera, pravi se novi segment i
// u njega se stavlja samo 4. element jer on nije na disku, a stari segment se brise
// ovo moze da se koristi kad se u sred put-a prekine program, a mem table nije pun
// TODO moramo posljednji segment dumpovati na kraju !!!


//
//func main() {
//	w:= CreateWal(WAL_PATH)
//	w.Put(&Element{
//		key:       "keke",
//		value:     []byte("asdd"),
//		next:      nil,
//		timestamp: "",
//		tombstone: false,
//		checksum:  CRC32([]byte("keke")),
//	})
//	w.Put(&Element{
//		key:       "meke",
//		value:     []byte("asdd"),
//		next:      nil,
//		timestamp: "",
//		tombstone: false,
//		checksum:  CRC32([]byte("meke")),
//	})
//
//	//Recover(WAL_PATH)
//	//w.RemoveSegments()
//
//	w.Dump()
//}

