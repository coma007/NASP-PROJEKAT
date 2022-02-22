package structures

import (
	"bytes"
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type CountMinSketch struct {
	M     uint          // Velicina Set-a
	K     uint          // Broj hash funkcija
	E     float64       // Preciznost
	D     float64 		// Tacnost
	Set   [][]int      // Set sa bitovima
	hashs []hash.Hash32 // hash funkcije
	TimeConst uint
}

func CreateCMS(p float64, d float64) *CountMinSketch {
	m := findM(p)
	k := findK(d)
	hashs, tc := createHashFunctions(k)
	set := make([][]int, k, k)
	for i, _ := range set {
		set[i] = make([]int, m, m)
	}
	bf := CountMinSketch{m, k, p, d, set, hashs, tc}
	//fmt.Printf("Created Count Min Skatch with M = %d, K = %d\n", M, k)
	return &bf
}

func (cms *CountMinSketch) Add(elem string) {
	for i, hashF := range cms.hashs {
		j := hashIt(hashF, elem, cms.M)
		cms.Set[i][j] += 1
	}
	//fmt.Printf("Element %s added !\n", elem)
}

func (cms *CountMinSketch) Query(elem string) int {
	values := make([]int, cms.K, cms.K)
	for i, hashF := range cms.hashs {
		j:= hashIt(hashF, elem, cms.M)
		values[i] = cms.Set[i][j]
	}

	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

func hashIt(hashF hash.Hash32, elem string, m uint) uint32 {
	_, err := hashF.Write([]byte(elem))
	if err != nil {
		panic(err)
	}
	i := hashF.Sum32() % uint32(m)
	hashF.Reset()
	return i
}

func findM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func findK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func createHashFunctions(k uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h, ts
}

func (cms *CountMinSketch) SerializeCMS() []byte {

	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	encoder.Encode(cms)
	return buff.Bytes()
}

func DeserializeCMS(data []byte) *CountMinSketch {

	buff := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buff)
	cms := new(CountMinSketch)

	for {
		err := decoder.Decode(cms)
		if err != nil {
			break
		}
	}
	cms.hashs = CopyHashFunctions(cms.K, cms.TimeConst)
	return cms
}
