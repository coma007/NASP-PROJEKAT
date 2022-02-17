package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

func FindSummary(key, filename string) (ok bool, offset int64){
	ok = false
	offset = int64(0)

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

	//start key
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	keyLen := binary.LittleEndian.Uint64(bytes)

	bytes = make([]byte, keyLen)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	startKey := string(bytes[:])
	println(startKey)

	if key < startKey {
		return false, 0
	}

	//end key
	bytes = make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	keyLen = binary.LittleEndian.Uint64(bytes)

	bytes = make([]byte, keyLen)
	_, err = reader.Read(bytes)
	if err != nil {
		panic(err)
	}
	endKey := string(bytes[:])
	println(endKey)

	if key > endKey {
		return false, 0
	}

	var i uint64
	for i = 0; i < fileLen-2; i++ {
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
		println(nodeKey)

		if nodeKey <= key {
			ok = true
		}

		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)
		println(newOffset)

		if ok {
			offset = int64(newOffset)
		} else if !ok {
			break
		}
	}

	return
}

func WriteSummary(keys []string, offsets []uint, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		return
	}

	writer := bufio.NewWriter(file)

	fileLen := uint64(len(keys))
	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, fileLen)
	_, err = writer.Write(bytesLen)
	if err != nil {
		log.Fatal(err)
	}

	for i := range keys {
		key := keys[i]
		offset := offsets[i]

		bytes := []byte(key)


		keyLen := uint64(len(bytes))
		bytesLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLen, keyLen)
		_, err := writer.Write(bytesLen)
		if err != nil {
			log.Fatal(err)
		}

		_, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}

		if i < 2 {
			bytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(offset))
			_, err = writer.Write(bytes)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = writer.Flush()
		if err != nil {
			return
		}
	}

}

