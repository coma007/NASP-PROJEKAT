package main

import (
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
	// novi indeks i summary fajl pravimo preko data fajla
	//newIndex, _ := os.Create(dir + fIFile + sIFile + "lev" + strLevel + "-Index.db")
	//newSummary, _ := os.Create(dir + fSFile + sSFile + "lev" + strLevel + "-Summary.db")
	
	keys := make([]string, 0)
	offset := make([]uint, 0)
	currentOffset := uint(0)

	fDataFile, err := os.Open(dir + fDFile) // otvoren prvi data fajl
	if err != nil {
		log.Println("Fajl ne moze da se otvori, naziv fajla = ", fDFile)
	}
	defer fDataFile.Close()

	sDataFile, _ := os.Open(dir + sDFile) // otvoren drugi data fajl
	defer sDataFile.Close()

	f1, _ := fDataFile.Stat()
	size1 := uint(f1.Size())
	f2, _ := sDataFile.Stat()
	size2 := uint(f2.Size())
	CreateBloomFilter(size1+size2, 2)

	// TODO redosledno ciranje datoteka

	// brisanje starih sstabela
	os.Remove(dir + fDFile)
	os.Remove(dir + fIFile)
	os.Remove(dir + fSFile)
	os.Remove(dir + sDFile)
	os.Remove(dir + sIFile)
	os.Remove(dir + sSFile)
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
