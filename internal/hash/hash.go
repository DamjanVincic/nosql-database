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

func CreateHashFunctions(k uint) []HashWithSeed {
	functions := make([]HashWithSeed, k)
	currentTime := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(currentTime+i))
		hfn := HashWithSeed{seed: seed}
		functions[i] = hfn
	}
	return functions
}
