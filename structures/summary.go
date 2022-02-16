package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

func FindSummary(key, filename string) (ok bool, offset uint){
	ok = false
	offset = uint(0)

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)


	for i := 0; i < 2; i++ {
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

		//if nodeKey == key {
		//	ok = true
		//}

		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)
		println(newOffset)

		//if ok {
		//	offset = uint(newOffset)
		//	break
		//}
	}

	return
}

func WriteSummary(keys []string, offsets []uint, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		return
	}

	writer := bufio.NewWriter(file)

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


		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(offset))
		_, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}

		err = writer.Flush()
		if err != nil {
			return
		}
	}

}

