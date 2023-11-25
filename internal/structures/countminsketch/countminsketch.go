package countminsketch

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/DamjanVincic/key-value-engine/internal/structures/hash"
)

type CountMinSketch struct {
	m, k          uint32
	matrix        [][]uint32
	hashFunctions []hash.HashWithSeed
}

func calculateM(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func calculateK(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}

func NewCMS(errorRate float64, probability float64) CountMinSketch {
	m := calculateM(errorRate)
	k := calculateK(probability)
	matrix := make([][]uint32, k)
	for i := range matrix {
		matrix[i] = make([]uint32, m)
	}
	return CountMinSketch{m: m, k: k, matrix: matrix, hashFunctions: hash.CreateHashFunctions(k)}
}

func (cms *CountMinSketch) Add(element []byte) {
	// Hash the element and increase the value of the corresponding matrix cell by 1
	for key, seed := range cms.hashFunctions {
		value := seed.Hash(element)
		cms.matrix[key][value%cms.m] += 1
	}
}

func (cms *CountMinSketch) GetFrequency(element []byte) uint32 {
	// Hash the element and return the value of corresponding matrix cell
	var count = make([]uint32, cms.k)
	for key, seed := range cms.hashFunctions {
		value := seed.Hash(element)
		count[key] = cms.matrix[key][value%cms.m]
	}
	return slices.Min(count)
}

func (cms *CountMinSketch) Serialize() []byte {
	// Calculate the number of bytes needed to store the byte array and the hash functions
	//4 + 4 for m and k, for matrix dimensions(m*k)*4 (because type is uint32), and 4*k for hash functions
	bytes := make([]byte, 8+cms.k*cms.m*4+4*cms.k)

	// Append the number of bytes in the byte array
	binary.BigEndian.PutUint32(bytes[:4], cms.m)

	// Append the number of hash functions
	binary.BigEndian.PutUint32(bytes[4:8], cms.k)

	// Append the matrix after the first 8 bytes
	for i := range cms.matrix {
		for j := range cms.matrix[i] {
			binary.BigEndian.PutUint32(bytes[8+(i*int(cms.m)+j)*4:12+(i*int(cms.m)+j)*4], cms.matrix[i][j])
		}
	}

	// Copy the hash functions after the byte array
	copy(bytes[8+cms.k*cms.m*4:], hash.Serialize(cms.hashFunctions))

	return bytes
}

func Deserialize(bytes []byte) CountMinSketch {
	// First 4 bytes are the number of bytes in the byte array
	m := binary.BigEndian.Uint32(bytes[:4])

	// Next 4 bytes are the number of hash functions
	k := binary.BigEndian.Uint32(bytes[4:8])

	// Next m*k*4 bytes is the byte array
	matrix := make([][]uint32, k)
	for i := range matrix {
		matrix[i] = make([]uint32, m)
		for j := range matrix[i] {
			matrix[i][j] = binary.BigEndian.Uint32(bytes[8+(i*int(m)+j)*4 : 12+(i*int(m)+j)*4])
		}
	}
	// Next k*4 bytes are the hash functions seeds
	hashFunctions := hash.Deserialize(bytes[8+k*m*4:])

	return CountMinSketch{m: m, k: k, matrix: matrix, hashFunctions: hashFunctions}
}
