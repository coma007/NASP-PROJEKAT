package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
)

type Index interface {
	Add()
	Find()
	Write(filename string)
}


type SSIndex struct {
	OffsetSize	uint
	KeySizeNumber	uint
	DataKeys	[]string
	DataOffset	[]uint
	filename	string
}

func CreateIndex(keys []string, offset []uint, filename string) *SSIndex {
	// We need to check if data is sorted
	index := SSIndex{filename: filename}
	for i, key := range keys{
		index.Add(key, offset[i])
	}
	return &index
}

func (index *SSIndex) Add(key string, offset uint) {
	index.DataKeys = append(index.DataKeys, key)
	index.DataOffset = append(index.DataOffset, offset)
}

func FindIndex(key string, offset int64, filename string) (ok bool, dataOffset int64) {
	ok = false
	dataOffset = 0

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(bytes)
	//println(fileLen)

	_, err = file.Seek(offset, 0)
	if err != nil {
		return false, 0
	}

	var i uint64
	for i = 0; i < fileLen; i++ {
		bytes := make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(bytes)
		//println(keyLen)

		bytes = make([]byte, keyLen)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(bytes[:])
		//println(nodeKey)

		if nodeKey == key {
			ok = true
		}

		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)
		//println(newOffset)

		if ok {
			dataOffset = int64(newOffset)
			break
		}
	}

	return
}

func (index *SSIndex) Write() (keys []string, offsets []uint){
	currentOffset := uint(0)
	file, err := os.Create(index.filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, uint64(len(index.DataKeys)))
	bytesWritten, err := writer.Write(bytesLen)
	if err != nil {
		log.Fatal(err)
	}

	currentOffset += uint(bytesWritten)

	err = writer.Flush()
	if err != nil {
		return
	}

	rangeKeys := make([]string, 0)
	rangeOffsets := make([]uint, 0)
	sampleKeys := make([]string, 0)
	sampleOffsets := make([]uint, 0)
	for i := range index.DataKeys {
		key := index.DataKeys[i]
		offset := index.DataOffset[i]
		if i == 0 || i == (len(index.DataKeys) - 1) {
			rangeKeys = append(rangeKeys, key)
			rangeOffsets = append(rangeOffsets, currentOffset)
		} else if rand.Intn(100) > 50 {
			sampleKeys = append(sampleKeys, key)
			sampleOffsets = append(sampleOffsets, currentOffset)
		}
		//log.Printf("Key: %d\n", key)
		bytes := []byte(key)
		//println(keyLen(bytes))
		//println(bytes)

		keyLen := uint64(len(bytes))
		bytesLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLen, keyLen)
		bytesWritten, err := writer.Write(bytesLen)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(offset))
		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)
		//log.Printf("Written: %d\n", bytesWritten + bytesWritten1 + bytesWritten2)

	}
	err = writer.Flush()
	if err != nil {
		return
	}

	keys = append(rangeKeys, sampleKeys...)
	offsets = append(rangeOffsets, sampleOffsets...)
	return
	//err = file.Close()
	//if err != nil {
	//	return
	//}
}

//func main() {
//
//	root := SkipListNode{"start", nil, make([]*SkipListNode, 2)}
//	sl := SkipList{25, 1, 1, &root}
//	sl.Add("kopitaneskita", []byte("123"))
//	sl.Add("joca", []byte("123"))
//	sl.Add("mica", []byte("123"))
//	sl.Add("maca", []byte("123"))
//	sl.Add("zeljko", []byte("123"))
//	sl.Add("zdravomir", []byte("123"))
//	index := Create(sl, "index.db")
//
//	println(index)
//	//index := SSIndex{OffsetSize: 8, KeySizeNumber: 2, filename: "index.db"}
//	//index.Add("Marko", 0)
//	//index.Add("Zivodrag", 10)
//	//index.Add("Jovan", 20)
//	//index.Add("Mirko", 30)
//	//index.Add("Petar", 40)
//	//
//	//
//	//index.Write()
//	//
//	//fmt.Println(index.Find("Zivodrag"))
//}
