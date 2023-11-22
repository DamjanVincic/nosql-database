package hash

import (
	"encoding/binary"
	"hash/fnv"
	"time"
)

type HashWithSeed struct {
	seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint32 {
	fn := fnv.New32()
	_, err := fn.Write(append(data, h.seed...))
	if err != nil {
		panic(err)
	}
	return fn.Sum32()
}

func CreateHashFunctions(k uint32) []HashWithSeed {
	functions := make([]HashWithSeed, k)
	currentTime := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, currentTime+i)
		hfn := HashWithSeed{seed: seed}
		functions[i] = hfn
	}
	return functions
}

func Serialize(functions []HashWithSeed) []byte {
	bytes := make([]byte, 0)
	for _, fn := range functions {
		bytes = append(bytes, fn.seed...)
	}
	return bytes
}

func Deserialize(bytes []byte) []HashWithSeed {
	functions := make([]HashWithSeed, 0)
	for i := 0; i < len(bytes); i += 4 {
		functions = append(functions, HashWithSeed{seed: bytes[i : i+4]})
	}
	return functions
}
