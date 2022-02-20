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
}

func CreateLsm(maxL, maxS int) *LSM {
	return &LSM{
		maxLevel: maxL,
		maxSize:  maxS,
	}
}

func (lsm LSM) IsCompactionNeeded(dir string, level int) (bool, []string, []string, []string, []string, []string) {
	dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles := FindFiles(dir, level)
	return len(dataFiles) == lsm.maxSize, dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles
}

func (lsm LSM) DoCompaction(dir string, level int) {
	if level >= lsm.maxLevel { // nema spajanja tabela nakon poslednjeg nivoa
		return
	}

	compaction, dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles := lsm.IsCompactionNeeded(dir, level)
	if !compaction {
		return
	}

	i := 0
	var numFile int
	if len(dataFiles) == lsm.maxLevel {
		numFile = 1
	} else {
		numFile = 3
	}

	for i < lsm.maxSize { // while petlja
		fDataFile := dataFiles[i]
		fIndexFile := indexFiles[i]
		fSummaryFile := summaryFiles[i]
		fTocFile := tocFiles[i]
		fFilterFile := filterFiles[i]
		sDataFile := dataFiles[i+1]
		sIndexFile := indexFiles[i+1]
		sSummaryFile := summaryFiles[i+1]
		sTocFile := tocFiles[i+1]
		sFilterFile := filterFiles[i+1]
		Merge(dir, fDataFile, fIndexFile, fSummaryFile, fTocFile, fFilterFile, sDataFile, sIndexFile,
			sSummaryFile, sTocFile, sFilterFile, level, numFile)
		i = i + 2
		numFile++
	}

	lsm.DoCompaction(dir, level+1) // provera da li je na narednom nivou potrebna kompakcija
}

func Merge(dir, fDFile, fIFile, fSFile, fTFile, fFFile, sDFile, sIFile, sSFile,
	sTFile, sFFile string, level, numFile int) {
	strLevel := strconv.Itoa(level + 1)

	// kreiranje nove sstabele
	generalFilename := dir + "usertable-data-ic-" + strconv.Itoa(numFile) + "-lev" + strLevel + "-"
	table := &SSTable{generalFilename, generalFilename + "Data.db",
		generalFilename + "Index.db", generalFilename + "Summary.db",
		generalFilename + "Filter.gob"}

	newData, _ := os.Create(generalFilename + "Data.db")

	currentOffset := uint(0)  // trenutni offset u novom data fajlu
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

	sDataFile, err := os.Open(dir + sDFile) // otvoren drugi data fajl
	if err != nil {
		panic(err)
	}

	// file length prvog data fajla
	reader1 := bufio.NewReader(fDataFile)
	bytes := make([]byte, 8)
	_, err = reader1.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen1 := binary.LittleEndian.Uint64(bytes)
	currentOffset1 += 8

	// file length drugog data fajla
	reader2 := bufio.NewReader(sDataFile)
	bytes = make([]byte, 8)
	_, err = reader2.Read(bytes)
	if err != nil {
		panic(err)
	}
	fileLen2 := binary.LittleEndian.Uint64(bytes)
	currentOffset2 += 8

	fileLen := ReadAndWrite(currentOffset, currentOffset1, currentOffset2, newData, fDataFile, sDataFile,
		fileLen1, fileLen2, table)

	// upis duzine fajla (broj kljuceva)
	FileSize(generalFilename+"Data.db", fileLen)

	// brisanje starih sstabela
	fDataFile.Close()
	sDataFile.Close()
	fmt.Println(fDFile)
	os.Remove(dir + fDFile)
	os.Remove(dir + fIFile)
	os.Remove(dir + fSFile)
	os.Remove(dir + fTFile)
	os.Remove(dir + fFFile)
	os.Remove(dir + sDFile)
	os.Remove(dir + sIFile)
	os.Remove(dir + sSFile)
	os.Remove(dir + sTFile)
	os.Remove(dir + sFFile)
}

func ReadAndWrite(currentOffset, currentOffset1, currentOffset2 uint, newData, fDataFile, sDataFile *os.File, fileLen1,
	fileLen2 uint64, table *SSTable) uint64 {

	filter := CreateBloomFilter(uint(fileLen1+fileLen2), 2)
	keys := make([]string, 0)
	offset := make([]uint, 0)

	crc1, timestamp1, tombstone1, keyLen1, valueLen1,
		key1, value1, currentOffset1 := ReadData(fDataFile, currentOffset1)

	crc2, timestamp2, tombstone2, keyLen2, valueLen2,
		key2, value2, currentOffset2 := ReadData(sDataFile, currentOffset2)

	first := uint64(1)
	second := uint64(1)

	for {
		// sigurno su vec upisani svi podaci bar jednog fajla
		if fileLen1 == first || fileLen2 == second {
			break
		}

		if key1 == key2 {
			// biramo onog sa kasnijim vremenom
			if timestamp1 > timestamp2 {
				// prvi se upisuje, drugi se preskace
				currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
					tombstone1, keyLen1, valueLen1, key1, value1)
				filter.Add(Element{key1, nil, nil, timestamp1, false, 0})
				keys = append(keys, key1)
				offset = append(offset, currentOffset)
			} else {
				// drugi se upisuje, prvi se preskace
				currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
					tombstone2, keyLen2, valueLen2, key2, value2)
				filter.Add(Element{key2, nil, nil, timestamp2, false, 0})
				keys = append(keys, key2)
				offset = append(offset, currentOffset)
			}
			crc1, timestamp1, tombstone1, keyLen1, valueLen1,
				key1, value1, currentOffset1 = ReadData(fDataFile, currentOffset1)
			first++

			crc2, timestamp2, tombstone2, keyLen2, valueLen2,
				key2, value2, currentOffset2 = ReadData(sDataFile, currentOffset2)
			second++

		} else if key1 < key2 {
			// samo prvi se upisuje
			currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
				tombstone1, keyLen1, valueLen1, key1, value1)
			filter.Add(Element{key1, nil, nil, timestamp1, false, 0})
			keys = append(keys, key1)
			offset = append(offset, currentOffset)

			crc1, timestamp1, tombstone1, keyLen1, valueLen1,
				key1, value1, currentOffset1 = ReadData(fDataFile, currentOffset1)
			first++

		} else {
			// samo drugi se upisuje
			currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
				tombstone2, keyLen2, valueLen2, key2, value2)
			filter.Add(Element{key2, nil, nil, timestamp2, false, 0})
			keys = append(keys, key2)
			offset = append(offset, currentOffset)

			crc2, timestamp2, tombstone2, keyLen2, valueLen2,
				key2, value2, currentOffset2 = ReadData(sDataFile, currentOffset2)
			second++
		}
	}

	// ako je prvi dosao do kraja drugi treba da iscitamo do kraja
	if fileLen1 == first && fileLen2 != second {
		for fileLen2 != second {
			currentOffset = WriteData(newData, currentOffset, crc2, timestamp2,
				tombstone2, keyLen2, valueLen2, key2, value2)
			filter.Add(Element{key2, nil, nil, timestamp2, false, 0})
			keys = append(keys, key2)
			offset = append(offset, currentOffset)

			crc2, timestamp2, tombstone2, keyLen2, valueLen2,
				key2, value2, currentOffset2 = ReadData(sDataFile, currentOffset2)
			second++
		}
		// ako je drugi dosao do kraja prvi treba da iscitamo do kraja
	} else if fileLen2 == second && fileLen1 != first {
		for fileLen1 != first {
			currentOffset = WriteData(newData, currentOffset, crc1, timestamp1,
				tombstone1, keyLen1, valueLen1, key1, value1)
			filter.Add(Element{key1, nil, nil, timestamp1, false, 0})
			keys = append(keys, key1)
			offset = append(offset, currentOffset)

			crc1, timestamp1, tombstone1, keyLen1, valueLen1,
				key1, value1, currentOffset1 = ReadData(sDataFile, currentOffset2)
			first++
		}
	}
	//kreiranje ostalih delova sstabele
	index := CreateIndex(keys, offset, table.indexFilename)
	keys, offsets := index.Write()
	WriteSummary(keys, offsets, table.summaryFilename)
	table.WriteTOC()
	writeBloomFilter(table.filterFilename, filter)

	return uint64(len(keys))
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

func FileSize(filename string, len uint64) {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	_, err = file.Seek(0, 0)

	writer := bufio.NewWriter(file)

	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, len)
	_, err = writer.Write(bytesLen)

	if err != nil {
		log.Println(err)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	file.Close()
}

func FindFiles(dir string, level int) ([]string, []string, []string, []string, []string) {
	substr := strconv.Itoa(level)

	files, _ := ioutil.ReadDir(dir) // lista svih fajlova iz direktorijuma

	var dataFiles []string
	var indexFiles []string
	var summaryFiles []string
	var tocFiles []string
	var filterFiles []string

	for _, f := range files {
		if strings.Contains(f.Name(), "lev"+substr+"-Data.db") {
			dataFiles = append(dataFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-Index.db") {
			indexFiles = append(indexFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-Summary.db") {
			summaryFiles = append(summaryFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-TOC.txt") {
			tocFiles = append(tocFiles, f.Name())
		}
		if strings.Contains(f.Name(), "lev"+substr+"-Filter.gob") {
			filterFiles = append(filterFiles, f.Name())
		}
	}

	return dataFiles, indexFiles, summaryFiles, tocFiles, filterFiles
}

func main() {
	var lsm = CreateLsm(4, 4)
	lsm.DoCompaction("./data/sstable/", 1)
}

//
//	currentOffset1 := uint(0)
//	fDataFile, err := os.Open("./data/sstable/usertable-data-ic-1-lev1-Data.db") // otvoren drugi data fajl
//	if err != nil {
//		panic(err)
//	}
//	defer fDataFile.Close()
//
//	// provera da li radi citanje duzine fajla  RADI
//	reader1 := bufio.NewReader(fDataFile)
//	bytes := make([]byte, 8)
//	_, err = reader1.Read(bytes)
//	if err != nil {
//		panic(err)
//	}
//	fileLen1 := binary.LittleEndian.Uint64(bytes)
//	fmt.Println(fileLen1)
//	currentOffset1 += 8 // dobije se 6, to je broj kljuceva
//
//	// citanje iz fajla - RADI
//	crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 := ReadData(fDataFile, currentOffset1)
//	fmt.Println(crc1)
//	fmt.Println(timestamp1)
//	fmt.Println(tombstone1)
//	fmt.Println(keyLen1)
//	fmt.Println(valueLen1)
//	fmt.Println(key1)
//	fmt.Println(value1)
//	fmt.Println(currentOffset1)
//
//	currentOffset := uint(0)
//	newData, _ := os.Create("./data/sstable/usertable-data-ic-2-lev1-Data.db")
//
//	writer := bufio.NewWriter(newData)
//
//	//file length (na pocetku je 0 jer ne znamo jos koja je duzina fajla)
//	bytesLen := make([]byte, 8)
//	bytesWritten, err := writer.Write(bytesLen)
//	currentOffset += uint(bytesWritten)
//	if err != nil {
//		log.Fatal(err)
//	}
//	err = writer.Flush()
//	if err != nil {
//		return
//	}
//
//	// pisanje - RADI
//	_ = WriteData(newData, currentOffset, crc1, timestamp1,
//		tombstone1, keyLen1, valueLen1, key1, value1)
//	fmt.Println("========================")
//
//	// provera citanja napisanog  RADI
//	crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 = ReadData(newData, currentOffset)
//	fmt.Println(crc1)
//	fmt.Println(timestamp1)
//	fmt.Println(tombstone1)
//	fmt.Println(keyLen1)
//	fmt.Println(valueLen1)
//	fmt.Println(key1)
//	fmt.Println(value1)
//	fmt.Println(currentOffset1)
//	newData.Close()
//
//	// upis duzine fajla na pocetak
//	file, err := os.OpenFile("./data/sstable/usertable-data-ic-2-lev1-Data.db", os.O_WRONLY, 0644)
//	if err != nil {
//		log.Println(err)
//	}
//	_, err = file.Seek(0, 0)
//
//	writer = bufio.NewWriter(file)
//
//	bytesLen = make([]byte, 8)
//	binary.LittleEndian.PutUint64(bytesLen, uint64(5))
//	fmt.Println(bytesLen)
//	s, err := writer.Write(bytesLen)
//	fmt.Println(s)
//
//	if err != nil {
//		log.Println(err)
//	}
//
//	err = writer.Flush()
//	if err != nil {
//		return
//	}
//	file.Close()
//	//zaglavlje(newData)
//
//	// citanje duzine
//	file, _ = os.Open("./data/sstable/usertable-data-ic-2-lev1-Data.db")
//	defer file.Close()
//	reader := bufio.NewReader(file)
//	file.Seek(0, 0)
//	bytes1 := make([]byte, 8)
//	_, err = reader.Read(bytes1)
//	fmt.Println(bytes1)
//	if err != nil {
//		log.Println(err)
//	}
//	fileLen := binary.LittleEndian.Uint64(bytes1)
//	fmt.Println("duzina:")
//	fmt.Println(fileLen)
//	currentOffset1 = 8
//
//	// provera da li dalje moze normalno da se cita
//	//file, _ = os.Open("./data/sstable/usertable-data-ic-2-lev1-Data.db")
//	crc1, timestamp1, tombstone1, keyLen1, valueLen1, key1, value1, currentOffset1 = ReadData(file, 8)
//	fmt.Println("--------------------------------")
//	fmt.Println(crc1)
//	fmt.Println(timestamp1)
//	fmt.Println(tombstone1)
//	fmt.Println(keyLen1)
//	fmt.Println(valueLen1)
//	fmt.Println(key1)
//	fmt.Println(value1)
//	fmt.Println(currentOffset1)

//os.Remove("./data/sstable/usertable-data-ic-2-lev1-Data.db")
//}
