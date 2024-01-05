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

func (bf *BloomFilter) AddElement(element []byte) error {
	// Hash all elements and set the corresponding bits in the byte array to 1
	for _, fn := range bf.hashFunctions {
		hash_, err := fn.Hash(element)
		if err != nil {
			return err
		}

		idx := hash_ % uint64(bf.m)
		bf.byteArray[idx] = 1
	}

	return nil
}

func (bf *BloomFilter) ContainsElement(element []byte) (bool, error) {
	// Hash the element and check if all bits at the corresponding indexes in the byte array are 1
	for _, fn := range bf.hashFunctions {
		hash_, err := fn.Hash(element)
		if err != nil {
			return false, err
		}

		idx := hash_ % uint64(bf.m)
		if bf.byteArray[idx] == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (bf *BloomFilter) Serialize() []byte {
	// Calculate the number of bytes needed to store the byte array and the hash functions
	bytes := make([]byte, 8+bf.m+4*bf.k)

	// Append the number of bytes in the byte array
	binary.BigEndian.PutUint32(bytes[:4], bf.m)

	// Append the number of hash functions
	binary.BigEndian.PutUint32(bytes[4:8], bf.k)

	// Copy the byte array after the first 8 bytes
	copy(bytes[8:8+bf.m], bf.byteArray)

	// Copy the hash functions after the byte array
	copy(bytes[8+bf.m:], hash.Serialize(bf.hashFunctions))

	return bytes
}

func Deserialize(bytes []byte) BloomFilter {
	// First 4 bytes are the number of bytes in the byte array
	m := binary.BigEndian.Uint32(bytes[:4])

	// Next 4 bytes are the number of hash functions
	k := binary.BigEndian.Uint32(bytes[4:8])

	// Next m bytes is the byte array
	byteArray := bytes[8 : 8+m]

	// Next k*4 bytes are the hash functions seeds
	hashFunctions := hash.Deserialize(bytes[8+m:])

	return BloomFilter{
		m:             m,
		k:             k,
		byteArray:     byteArray,
		hashFunctions: hashFunctions,
	}
}
