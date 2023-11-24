package main

import (
	"encoding/binary"
	"log"
	"math"
	"os"
)

type HyperLogLog struct {
	m   uint64
	p   uint8
	reg []uint8
}

func NewHyperLogLog(bucketBits uint8) HyperLogLog {
	size := uint64(math.Pow(2, float64(bucketBits)))
	return HyperLogLog{p: bucketBits, m: size, reg: make([]uint8, size)}
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

func (hyperLogLog *HyperLogLog) add(value []byte) {
	hashValue := Hash(value)
	bucketIndex := firstKbits(hashValue, uint64(hyperLogLog.p))
	count := uint8(trailingZeroBits(hashValue)) + 1
	if count > hyperLogLog.reg[bucketIndex] {
		hyperLogLog.reg[bucketIndex] = count
	}
}

func (hyperLogLog *HyperLogLog) serialize() []byte {
	serializedHll := make([]byte, 0)
	// Temporary storage for 64-bit integers
	tempByte := make([]byte, 8)

	binary.BigEndian.PutUint64(tempByte, hyperLogLog.m)

	serializedHll = append(serializedHll, tempByte...)
	serializedHll = append(serializedHll, hyperLogLog.p)

	for _, item := range hyperLogLog.reg {
		serializedHll = append(serializedHll, item)
	}
	return serializedHll
}

func deserialize(serializedHll []byte) HyperLogLog {
	m := binary.BigEndian.Uint64(serializedHll[:8])
	p := serializedHll[8]
	var i uint64
	reg := make([]uint8, m)
	for i = 0; i < m; i++ {
		reg[i] = serializedHll[9+i]
	}
	return HyperLogLog{p: p, m: m, reg: reg}
}

func (hyperLogLog *HyperLogLog) writeToFile(destination string) {
	bytes := hyperLogLog.serialize()
	err := os.WriteFile(destination, bytes, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func loadFromFile(destination string) HyperLogLog {
	serializedHll, err := os.ReadFile(destination)
	if err != nil {
		log.Fatal(err)
	}
	return deserialize(serializedHll)
}
