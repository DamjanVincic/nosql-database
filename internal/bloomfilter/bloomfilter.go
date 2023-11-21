package bloomfilter

import (
	"github.com/DamjanVincic/key-value-engine/internal/hash"
	"math"
)

type BloomFilter struct {
	m             uint
	k             uint
	byteArray     []byte
	hashFunctions []hash.HashWithSeed
}

func calculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(-float64(expectedElements) * math.Log(falsePositiveRate) / math.Pow(math.Log(2), 2))
}

func calculateK(expectedElements int, m uint) uint {
	return uint(float64(m) / float64(expectedElements) * math.Log(2))
}

func CreateBloomFilter(expectedElements int, falsePositiveRate float64) BloomFilter {
	m := calculateM(expectedElements, falsePositiveRate)
	k := calculateK(expectedElements, m)

	bf := BloomFilter{
		m:             m,
		k:             k,
		byteArray:     make([]byte, m),
		hashFunctions: hash.CreateHashFunctions(k),
	}
	return bf
}

func (bf BloomFilter) AddElement(element []byte) {
	for _, fn := range bf.hashFunctions {
		idx := fn.Hash(element) % uint32(bf.m)
		bf.byteArray[idx] = 1
	}
}
