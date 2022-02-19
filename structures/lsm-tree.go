package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type LSM struct {
	maxLevel int
	maxSize  int
	helper   []int // pomocna struktura - trenutno stanje stabla na osnovu popunjenosti
}

func CreateLsm(maxL, maxS int) *LSM {
	return &LSM{
		maxLevel: maxL,
		maxSize:  maxS,
		helper:   make([]int, maxL, maxL),
	}
}

func (lsm LSM) IsCompactionNeeded(level int) bool {
	if level == 1 {
		lsm.helper[1]++ // cim dodamo novu sstabelu prvi nivo se povecava za 1
	}
	return lsm.helper[level] == lsm.maxSize
}

func (lsm LSM) DoCompaction(dir string, level int) {
	if level >= lsm.maxLevel { // nema spajanja tabela nakon poslednjeg nivoa
		return
	}

	if !lsm.IsCompactionNeeded(level) {
		return
	}

	dataFiles, indexFiles, summaryFiles := ReadFiles(dir, level)

	i := 0

	for i >= lsm.maxSize { // while petlja
		fDataFile := dataFiles[i]
		fIndexFile := indexFiles[i]
		fSummaryFile := summaryFiles[i]
		sDataFile := dataFiles[i+1]
		sIndexFile := indexFiles[i+1]
		sSummaryFile := summaryFiles[i+1]
		Merge(dir, fDataFile, fIndexFile, fSummaryFile, sDataFile, sIndexFile, sSummaryFile, level)
		lsm.helper[level]-- // broj elemenata na prosledjenom nivou se smanjuje za 2
		lsm.helper[level]-- // jer smo spojili 2 sstabele
		lsm.helper[level+1]++
		i = i + 2
	}

	lsm.DoCompaction(dir, level+1) // provera da li je na narednom nivou potrebna kompakcija
}

func Merge(dir, fDFile, fIFile, fSFile, sDFile, sIFile, sSFile string, level int) {
	strLevel := strconv.Itoa(level + 1)

	// kreiranje nove sstabele
	newData, _ := os.Create(dir + fDFile + sDFile + "lev" + strLevel + "-Data.db")
	// novi indeks i summary fajl pravimo pomocu data fajla

	currentOffset := uint(0)  // trenutrni offset u novom data fajlu
	currentOffset1 := uint(0) // trenutni offset u data1 fajlu
	currentOffset2 := uint(0) // trenutni offset u data2 fajlu

	writer := bufio.NewWriter(newData)

	// file length (na pocetku je 0 jer ne znamo jos koja je duzina fajla)
	bytesLen := make([]byte, 8)
	bytesWritten, err := writer.Write(bytesLen)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	fDataFile, err := os.Open(dir + fDFile) // otvoren prvi data fajl
	if err != nil {
		panic(err)
	}
	defer fDataFile.Close()

	sDataFile, err := os.Open(dir + sDFile) // otvoren drugi data fajl
	if err != nil {
		panic(err)
	}
	defer sDataFile.Close()

	reader1 := bufio.NewReader(fDataFile)
	bytes := make([]byte, 8)
	_, err = reader1.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen1 := binary.LittleEndian.Uint64(bytes)
	currentOffset1 += 8

	reader2 := bufio.NewReader(sDataFile)
	bytes = make([]byte, 8)
	_, err = reader2.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen2 := binary.LittleEndian.Uint64(bytes)
	currentOffset2 += 8

	// TODO redosledno citanje datoteka
	ReadAndWrite(currentOffset, currentOffset1, currentOffset2, newData, fDataFile, sDataFile, fileLen1, fileLen2)

	// brisanje starih sstabela
	os.Remove(dir + fDFile)
	os.Remove(dir + fIFile)
	os.Remove(dir + fSFile)
	os.Remove(dir + sDFile)
	os.Remove(dir + sIFile)
	os.Remove(dir + sSFile)
}

func ReadAndWrite(currentOffset, currentOffset1, currentOffset2 uint,
	newData, fDataFile, sDataFile *os.File, fileLen1, fileLen2 uint64) {

	filter := CreateBloomFilter(uint(fileLen1+fileLen2), 2)
	keys := make([]string, 0)
	offset := make([]uint, 0)

	crc1, timestamp1, tombstone1, keyLen1, valueLen1,
		key1, value1, currentOffset1 := ReadData(fDataFile, currentOffset1)

	crc2, timestamp2, tombstone2, keyLen2, valueLen2,
		key2, value2, currentOffset2 := ReadData(sDataFile, currentOffset2)

	for {
		// sigurno su vec upisani svi podaci bar jednog fajla
		if fileLen1 < uint64(currentOffset1) || fileLen2 < uint64(currentOffset2) {
			break
		}

		if key1 == key2 {
			// biramo onog sa kasnijim vremenom
			if timestamp1 > timestamp2 {
				// prvi se upisuje, drugi se preskace
				currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
					tombstone1, keyLen1, valueLen1, key1, value1)
				filter.Add(Element{key1, nil, nil, timestamp1, false, nil})
				keys = append(keys, key1)
				offset = append(offset, currentOffset)
			} else {
				// drugi se upisuje, prvi se preskace
				currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
					tombstone2, keyLen2, valueLen2, key2, value2)
				filter.Add(Element{key2, nil, nil, timestamp2, false, nil})
				keys = append(keys, key2)
				offset = append(offset, currentOffset)
			}
			crc1, timestamp1, tombstone1, keyLen1, valueLen1,
				key1, value1, currentOffset1 = ReadData(fDataFile, currentOffset1)

			crc2, timestamp2, tombstone2, keyLen2, valueLen2,
				key2, value2, currentOffset2 = ReadData(sDataFile, currentOffset2)

		} else if key1 < key2 {
			// samo prvi se upisuje
			currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
				tombstone1, keyLen1, valueLen1, key1, value1)
			filter.Add(Element{key1, nil, nil, timestamp1, false, nil})
			keys = append(keys, key1)
			offset = append(offset, currentOffset)

			crc1, timestamp1, tombstone1, keyLen1, valueLen1,
				key1, value1, currentOffset1 = ReadData(fDataFile, currentOffset1)
		} else {
			// samo drugi se upisuje
			currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
				tombstone2, keyLen2, valueLen2, key2, value2)
			filter.Add(Element{key2, nil, nil, timestamp2, false, nil})
			keys = append(keys, key2)
			offset = append(offset, currentOffset)

			crc2, timestamp2, tombstone2, keyLen2, valueLen2,
				key2, value2, currentOffset2 = ReadData(sDataFile, currentOffset2)
		}
	}

	// ako je prvi dosao do kraja drugi treba da iscitamo
	if fileLen1 < uint64(currentOffset1) {
		for fileLen2 != uint64(currentOffset2) {
			currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
				tombstone2, keyLen2, valueLen2, key2, value2)
			filter.Add(Element{key2, nil, nil, timestamp2, false, nil})
			keys = append(keys, key2)
			offset = append(offset, currentOffset)

			crc2, timestamp2, tombstone2, keyLen2, valueLen2,
				key2, value2, currentOffset2 = ReadData(sDataFile, currentOffset2)
		}
	} else if fileLen2 < uint64(currentOffset2) {
		for fileLen1 != uint64(currentOffset1) {
			currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
				tombstone1, keyLen1, valueLen1, key1, value1)
			filter.Add(Element{key1, nil, nil, timestamp1, false, nil})
			keys = append(keys, key1)
			offset = append(offset, currentOffset)

			crc1, timestamp1, tombstone1, keyLen1, valueLen1,
				key1, value1, currentOffset1 = ReadData(sDataFile, currentOffset2)
		}
	}

	return
}

func WriteData(file *os.File, currentOffset uint, crcBytes []byte, timestamp string, tombstone byte,
	keyLen, valueLen uint64, key, value string) uint {

	if tombstone == 1 {
		return currentOffset
	}

	file.Seek(int64(currentOffset), 0)
	writer := bufio.NewWriter(file)

	//crc
	bytesWritten, err := writer.Write(crcBytes)
	currentOffset += uint(bytesWritten)
	if err != nil {
		log.Fatal(err)
	}

	//timestamp
	timestampBytes := make([]byte, 16)
	copy(timestampBytes, timestamp)
	bytesWritten, err = writer.Write(timestampBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// tombstone
	tombstoneInt := uint8(0)
	err = writer.WriteByte(tombstoneInt)
	currentOffset += 1
	if err != nil {
		log.Fatal(err)
	}

	// keyLen
	keyLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLenBytes, keyLen)
	bytesWritten, err = writer.Write(keyLenBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// valueLen
	valueLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLenBytes, valueLen)
	bytesWritten, err = writer.Write(valueLenBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// key
	keyBytes := []byte(key)
	bytesWritten, err = writer.Write(keyBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	// value
	valueBytes := []byte(value)
	bytesWritten, err = writer.Write(valueBytes)
	if err != nil {
		log.Fatal(err)
	}
	currentOffset += uint(bytesWritten)

	err = writer.Flush()
	if err != nil {
		log.Fatal(err)
	}

	return currentOffset
}

func ReadData(file *os.File, currentOffset uint) ([]byte, string, byte,
	uint64, uint64, string, string, uint) {

	file.Seek(int64(currentOffset), 0)
	reader := bufio.NewReader(file)

	// crc
	crcBytes := make([]byte, 4)
	_, err := reader.Read(crcBytes)
	if err != nil {
		panic(err)
	}
	currentOffset += 4

	// timestamp
	timestampBytes := make([]byte, 16)
	_, err = reader.Read(timestampBytes)
	if err != nil {
		panic(err)
	}
	timestamp := string(timestampBytes[:])
	currentOffset += 16

	// tombstone
	tombstone, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}
	currentOffset += 1

	// keyLen
	keyLenBytes := make([]byte, 8)
	_, err = reader.Read(keyLenBytes)
	if err != nil {
		panic(err)
	}
	keyLen := binary.LittleEndian.Uint64(keyLenBytes)
	currentOffset += 8

	// valueLen
	valueLenBytes := make([]byte, 8)
	_, err = reader.Read(valueLenBytes)
	if err != nil {
		panic(err)
	}
	valueLen := binary.LittleEndian.Uint64(valueLenBytes)
	currentOffset += 8

	// key
	keyBytes := make([]byte, keyLen)
	_, err = reader.Read(keyBytes)
	if err != nil {
		panic(err)
	}

	key := string(keyBytes[:])
	println(key)
	currentOffset += uint(keyLen)

	// value
	valueBytes := make([]byte, valueLen)
	_, err = reader.Read(valueBytes)
	if err != nil {
		panic(err)
	}

	value := string(valueBytes[:])
	currentOffset += uint(valueLen)

	return crcBytes, timestamp, tombstone, keyLen, valueLen, key, value, currentOffset
}

func ReadFiles(dir string, level int) ([]string, []string, []string) {
	substr := strconv.Itoa(level)

	files, err := ioutil.ReadDir(dir) // lista svih fajlova iz direktorijuma
	if err != nil {
		fmt.Println("greska!")
		return nil, nil, nil
	}

	var dataFiles []string
	var indexFiles []string
	var summaryFiles []string

	for _, f := range files {
		if strings.Contains(f.Name(), substr+"-Data") {
			dataFiles = append(dataFiles, f.Name())
		}
		if strings.Contains(f.Name(), substr+"-Index") {
			indexFiles = append(dataFiles, f.Name())
		}
		if strings.Contains(f.Name(), substr+"-Summary") {
			summaryFiles = append(dataFiles, f.Name())
		}
	}

	return dataFiles, indexFiles, summaryFiles
}

func main() {
	var lsm = CreateLsm(4, 4)
	lsm.IsCompactionNeeded(2)
	ReadFiles("./structures/", 1)
}
