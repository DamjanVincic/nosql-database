package hyperLogLog

import (
	"encoding/binary"
	"errors"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
	"math"
	"math/bits"
)

// min and max number of bits that represent index of registry in which result is stored
// TBD should be read from config file?
const (
	HllMinPrecision = 4
	HllMaxPrecision = 16
)

type HyperLogLog struct {
	m            uint64  //number of registries
	p            uint8   //precision (number of bits for registry index)
	reg          []uint8 //registries
	hashFunction hash.HashWithSeed
}

func NewHyperLogLog(bucketBits uint8) (*HyperLogLog, error) {
	if bucketBits < HllMinPrecision || bucketBits > HllMaxPrecision {
		return nil, errors.New("Hll precision must be between" + string(rune(HllMinPrecision)) + " and " + string(rune(HllMinPrecision)))
	}
	size := uint64(math.Pow(2, float64(bucketBits)))
	return &HyperLogLog{p: bucketBits,
		m:            size,
		reg:          make([]uint8, size),
		hashFunction: hash.CreateHashFunctions(1)[0],
	}, nil
}

func (hyperLogLog *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hyperLogLog.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hyperLogLog.m))
	estimation := alpha * math.Pow(float64(hyperLogLog.m), 2.0) / sum
	emptyRegs := hyperLogLog.emptyCount()
	if estimation <= 2.5*float64(hyperLogLog.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hyperLogLog.m) * math.Log(float64(hyperLogLog.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hyperLogLog *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hyperLogLog.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hyperLogLog *HyperLogLog) Add(value []byte) error {
	hashValue, err := hyperLogLog.hashFunction.Hash(value)
	if err != nil {
		return err
	}

	//get index of registry in which to put the result
	bucketIndex := hashValue >> (64 - hyperLogLog.p)

	count := uint8(bits.TrailingZeros64(hashValue) + 1)
	//if current result is greater than the last one, put it in the registry
	if count > hyperLogLog.reg[bucketIndex] {
		hyperLogLog.reg[bucketIndex] = count
	}

	return nil
}

func (hyperLogLog *HyperLogLog) Serialize() []byte {
	serializedHll := make([]byte, 0)
	// Temporary storage for 64-bit integers
	tempByte := make([]byte, 8)

	binary.BigEndian.PutUint64(tempByte, hyperLogLog.m)

	serializedHll = append(serializedHll, tempByte...)
	serializedHll = append(serializedHll, hyperLogLog.p)

	//append data from each registry to byte slice
	for _, item := range hyperLogLog.reg {
		serializedHll = append(serializedHll, item)
	}

	// Append hash function seed
	serializedHll = append(serializedHll, hash.Serialize([]hash.HashWithSeed{hyperLogLog.hashFunction})...)

	return serializedHll
}

func Deserialize(serializedHll []byte) *HyperLogLog {
	//get m from first 8 bytes of serialized hll
	m := binary.BigEndian.Uint64(serializedHll[:8])
	//get p from 9th byte of serialized hll
	p := serializedHll[8]
	var i uint64
	reg := make([]uint8, m)

	//read registry data from remaining bytes
	for i = 0; i < m; i++ {
		reg[i] = serializedHll[9+i]
	}
	// Deserialize hash function
	hashFunction := hash.Deserialize(serializedHll[len(serializedHll)-4:])[0]

	return &HyperLogLog{p: p,
		m:            m,
		reg:          reg,
		hashFunction: hashFunction,
	}
}
