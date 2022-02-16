package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

type Table interface {
	Add()
	Find()
	Write()
}

func CreateSStable(data MemTable, filename string)  (keys []string, offset []uint){
	keys = make([]string, 0)
	offset = make([]uint, 0)
	currentOffset := uint(0)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// file length
	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, uint64(data.Size()))
	bytesWritten, err := writer.Write(bytesLen)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	// We need to check if data is sorted
	for node := data.data.head.next[0]; node != nil; node = node.next[0]{
		key := node.key
		value := node.value
		keys = append(keys, key)
		offset = append(offset, currentOffset)

		//crc
		crc := CRC32(value)
		crcBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcBytes, crc)
		bytesWritten, err := writer.Write(crcBytes)
		currentOffset += uint(bytesWritten)
		if err != nil {
			return
		}

		//timestamp
		//??

		//tombstone
		tombstone := node.Tombstone()
		tombstoneInt := uint8(0)
		if tombstone {
			tombstoneInt = 1
		}

		err = writer.WriteByte(tombstoneInt)
		currentOffset += 1
		if err != nil {
			return
		}

		//log.Printf("Key: %d\n", key)
		keyBytes := []byte(key)
		//println(keyLen(bytes))
		//println(bytes)

		keyLen := uint64(len(keyBytes))
		keyLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLenBytes, keyLen)
		bytesWritten, err = writer.Write(keyLenBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		valueLen := uint64(len(value))
		valueLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueLenBytes, valueLen)
		bytesWritten, err = writer.Write(valueLenBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(keyBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(value)
		if err != nil {
			return
		}
		currentOffset += uint(bytesWritten)

		err = writer.Flush()
		if err != nil {
			return
		}
	}
	return
}

func SStableFind(filename string, key string)  (ok bool, value []byte){
	ok = false

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
	println(fileLen)

	var i uint64
	for i = 0; i < fileLen; i++ {
		deleted := false
		// crc
		crcBytes := make([]byte, 4)
		_, err = reader.Read(crcBytes)
		if err != nil {
			panic(err)
		}
		crcValue := binary.LittleEndian.Uint32(crcBytes)

		// timestamp
		// ??

		//tombstone

		tombstone, err := reader.ReadByte()
		if err != nil {
			panic(err)
		}

		if tombstone == 1 {
			deleted = true
		}


		// keyLen
		keyLenBytes := make([]byte, 8)
		_, err = reader.Read(keyLenBytes)
		if err != nil {
			panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(keyLenBytes)
		println(keyLen)

		valueLenBytes := make([]byte, 8)
		_, err = reader.Read(valueLenBytes)
		if err != nil {
			panic(err)
		}
		valueLen := binary.LittleEndian.Uint64(valueLenBytes)
		//println(valueLen)

		keyBytes := make([]byte, keyLen)
		_, err = reader.Read(keyBytes)
		if err != nil {
			panic(err)
		}
		nodeKey := string(keyBytes[:])
		println(nodeKey)

		if nodeKey == key {
			ok = true
		}

		valueBytes := make([]byte, valueLen)
		_, err = reader.Read(valueBytes)
		if err != nil {
			panic(err)
		}

		if ok && !deleted && CRC32(valueBytes) == crcValue {
			value = valueBytes
			break
		}
	}

	return ok, value
}

func main() {

	mt := CreateMemTable(25)
	mt.Add("kopitaneskita", []byte("123"))
	mt.Add("joca", []byte("123"))
	mt.Add("mica", []byte("123"))
	mt.Add("maca", []byte("123"))
	mt.Add("zeljko", []byte("123"))
	mt.Add("zdravomir", []byte("123"))
	mt.Change("zeljko", []byte("234"))
	keys, offsets := CreateSStable(*mt, "sstable.db")
	println(SStableFind("sstable.db", "zeljko"))
	index := Create(keys, offsets, "index.db")
	keys, offsets = index.Write()
	WriteSummary(keys, offsets, "summary.db")
	FindSummary("maca", "summary.db")


	//println(index)
	//index := SSIndex{OffsetSize: 8, KeySizeNumber: 2, filename: "index.db"}
	//index.Add("Marko", 0)
	//index.Add("Zivodrag", 10)
	//index.Add("Jovan", 20)
	//index.Add("Mirko", 30)
	//index.Add("Petar", 40)
	//
	//
	//index.Write()
	//
	//fmt.Println(index.Find("Zivodrag"))
}
