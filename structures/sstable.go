package main

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"os"
)

type Table interface {
	Add()
	Find()
	Write()
}

type SSTable struct {
	generalFilename	string
	SSTableFilename	string
	indexFilename	string
	summaryFilename	string
	filterFilename	string
}

func CreateSStable(data MemTable, filename string)  (table *SSTable){
	generalFilename := "data/sstable/usertable-data-ic-" + filename + "-lev1-"
	table = &SSTable{generalFilename, generalFilename + "Data.db", generalFilename + "Index.db",
		generalFilename + "Summary.db", generalFilename + "Filter.gob"}


	filter := CreateBloomFilter(data.Size(), 2)
	keys := make([]string, 0)
	offset := make([]uint, 0)
	currentOffset := uint(0)
	file, err := os.Create(table.SSTableFilename)
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

		filter.Add(*node)
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
		timestamp := node.timestamp
		timestampBytes := make([]byte, 16)
		copy(timestampBytes, timestamp)
		//println(timestampBytes)
		bytesWritten, err = writer.Write(timestampBytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		//tombstone
		tombstone := node.tombstone
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

	index := CreateIndex(keys, offset, table.indexFilename)
	keys, offsets := index.Write()
	WriteSummary(keys, offsets, table.summaryFilename)
	table.WriteTOC()
	return
}

func (st *SSTable) SStableFind(key string, offset int64)  (ok bool, value []byte){
	ok = false

	file, err := os.Open(st.SSTableFilename)
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

	_, err = file.Seek(offset, 0)
	if err != nil {
		return false, nil
	}
	reader = bufio.NewReader(file)

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
		timestampBytes := make([]byte, 16)
		_, err = reader.Read(timestampBytes)
		if err != nil {
			panic(err)
		}
		timestamp := string(timestampBytes[:])
		println(timestamp)

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

func (st *SSTable) WriteTOC() {
	filename := st.generalFilename + "TOC.txt"
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	_, err = writer.WriteString(st.generalFilename + "\n")
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.SSTableFilename + "\n")
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.indexFilename + "\n")
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.summaryFilename + "\n")
	if err != nil {
		return
	}
	_, err = writer.WriteString(st.filterFilename)
	if err != nil {
		return
	}

	err = writer.Flush()
	if err != nil {
		return
	}
}

func readSSTable(filename string) (table *SSTable) {
	filename = "data/sstable/usertable-data-ic-" + filename + "-lev1-TOC.txt"

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	generalFilename, _:= reader.ReadString('\n')
	SSTableFilename, _:= reader.ReadString('\n')
	indexFilename, _:= reader.ReadString('\n')
	summaryFilename, _:= reader.ReadString('\n')
	filterFilename, _:= reader.ReadString('\n')


	table = &SSTable{generalFilename: generalFilename[:len(generalFilename)-1],
		SSTableFilename: SSTableFilename[:len(SSTableFilename)-1], indexFilename: indexFilename[:len(indexFilename)-1],
		summaryFilename: summaryFilename[:len(summaryFilename)-1], filterFilename: filterFilename[:len(filterFilename)-1]}
	return
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
	table := CreateSStable(*mt, "1")
	table = readSSTable("1")
	ok, offset := FindSummary("zeljko", table.summaryFilename)
	if ok {
		ok, offset = FindIndex("zeljko", offset, table.indexFilename)
		if ok {
			println(table.SStableFind("zeljko", offset))
		}
	}
}
