package main

import (
	"fmt"
	"io/ioutil"
)

type LSM struct {
	maxLevel int
	maxSize  int
	helper   []int  // pomocna struktura - trenutno stanje stabla na osnovu popunjenosti
}

func CreateLsm(maxL, maxS int) *LSM {
	return &LSM{
		maxLevel: maxL,
		maxSize: maxS,
		helper: make([]int, maxL, maxL),
	}
}

func (lsm LSM) IsCompactionNeeded(level int) bool{
	if level == 1 {
		lsm.helper[1]++    // cim dodamo novu sstabelu prvi nivo se povecava za 1
	}
	return lsm.helper[level] == lsm.maxSize
}

func (lsm LSM) DoCompaction(level int) {
	if level >= lsm.maxLevel {     // nema izvrsavanja kompakcija na poslednjem nivou
		return
	}

	if !lsm.IsCompactionNeeded(level) {
		return
	}

	lsm.DoCompaction(level+1)
}

func ReadFiles(dir string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println("greska!")
		return
	}
	fmt.Println(files)
	return

}


func main() {
	var lsm = CreateLsm(4, 4)
	lsm.IsCompactionNeeded(2)
	ReadFiles("structures")
	fmt.Println("sdsfdsf")
}