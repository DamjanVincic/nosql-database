package bloomfilter

import (
	"encoding/binary"
	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
	"math"
)

type BloomFilter struct {
	m             uint32
	k             uint32
	byteArray     []byte
	hashFunctions []hash.HashWithSeed
}

func calculateM(expectedElements int, falsePositiveRate float64) uint32 {
	return uint32(-float64(expectedElements) * math.Log(falsePositiveRate) / math.Pow(math.Log(2), 2))
}

func calculateK(expectedElements int, m uint32) uint32 {
	return uint32(float64(m) / float64(expectedElements) * math.Log(2))
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
	// Hash all elements and set the corresponding bits in the byte array to 1
	for _, fn := range bf.hashFunctions {
		idx := fn.Hash(element) % bf.m
		bf.byteArray[idx] = 1
	}
}

func (bf BloomFilter) ContainsElement(element []byte) bool {
	// Hash the element and check if all bits at the corresponding indexes in the byte array are 1
	for _, fn := range bf.hashFunctions {
		idx := fn.Hash(element) % bf.m
		if bf.byteArray[idx] == 0 {
			return false
		}
	}
	return true
}

func (bf BloomFilter) Serialize() []byte {
	bytes := make([]byte, 0)

	// Temporary storage for 32-bit integers
	tempByte := make([]byte, 4)

	binary.BigEndian.PutUint32(tempByte, bf.m)
	// Append the number of bytes in the byte array and then the byte array itself
	bytes = append(bytes, tempByte...)
	bytes = append(bytes, bf.byteArray...)

	binary.BigEndian.PutUint32(tempByte, bf.k)
	// Append the number of hash functions and then the hash functions themselves
	bytes = append(bytes, tempByte...)
	bytes = append(bytes, hash.Serialize(bf.hashFunctions)...)
	return bytes
}

func Deserialize(bytes []byte) BloomFilter {
	// First 4 bytes are the number of bytes in the byte array
	m := binary.BigEndian.Uint32(bytes[:4])

	// Next m bytes are the byte array
	byteArray := bytes[4 : 4+m]

	// Next 4 bytes are the number of hash functions
	k := binary.BigEndian.Uint32(bytes[4+m : 8+m])

	// Next k*4 bytes are the hash functions seeds
	hashFunctions := hash.Deserialize(bytes[8+m : 8+m+4*k])

	return BloomFilter{
		m:             m,
		k:             k,
		byteArray:     byteArray,
		hashFunctions: hashFunctions,
	}
}
