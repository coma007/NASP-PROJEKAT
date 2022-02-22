package structures

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"math"
)

type HyperLogLog struct {
	M   uint64
	P   uint8
	Reg []uint8
}

func (hll *HyperLogLog) emptyCount() uint8 {
	sum := uint8(0)
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func CreateHLL(p uint8) *HyperLogLog {
	m := int(math.Pow(2, float64(p)))
	return &HyperLogLog{uint64(m),p, make([]uint8, m, m)}
}

func (hll *HyperLogLog) Add(word string) {
	bin := ToBinary(GetMD5Hash(word))
	key := 0
	p := hll.P
	for i := 0; i < int(p); i++ {
		key +=  (int(bin[i]) - '0') * int(math.Pow(2, float64(int(p) - i)))
	}
	val := 0
	for i := len(bin) - 1; i > 0; i-- {
		if bin[i] == '0' {
			val++
		} else {
			break
		}
	}
	hll.Reg[key] = uint8(val)

}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)),-1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) SerializeHLL() []byte {

	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	encoder.Encode(&hll)
	//fmt.Println(buff.Bytes())
	//return []byte{1}
	return buff.Bytes()
}

func DeserializeHLL(data []byte) *HyperLogLog {

	buff := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buff)
	hll := new(HyperLogLog)

	for {
		err := decoder.Decode(&hll)
		if err != nil {
			break
		}
	}
	return hll
}
