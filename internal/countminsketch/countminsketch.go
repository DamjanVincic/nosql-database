package main

import (
	"encoding/gob"
	"os"
	"slices"
)

type CountMinSketch struct {
	M, K          uint
	Matrix        [][]uint
	HashFunctions []HashWithSeed
}

func newCMS(errorRate float64, probability float64) CountMinSketch {
	M := uint(CalculateM(errorRate))
	K := uint(CalculateK(probability))
	Matrix := make([][]uint, K)
	for i := range Matrix {
		Matrix[i] = make([]uint, M)
		for j := range Matrix[i] {
			Matrix[i][j] = 0
		}
	}
	return CountMinSketch{M: M, K: K, Matrix: Matrix, HashFunctions: CreateHashFunctions(K)}
}

func (cms *CountMinSketch) Add(element []byte) {
	for key, seed := range cms.HashFunctions {
		value := seed.Hash(element)
		cms.Matrix[key][value%uint64(cms.M)] += 1
	}
}

func (cms *CountMinSketch) GetFrequency(element []byte) uint {
	var count = make([]uint, cms.K)
	for key, seed := range cms.HashFunctions {
		value := seed.Hash(element)
		count[key] = cms.Matrix[key][value%uint64(cms.M)]
	}

	return slices.Min(count)
}

func Serialize(cms *CountMinSketch, filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms)
	if err != nil {
		panic(err.Error())
	}
	return nil
}

func Deserialize(filename string) (*CountMinSketch, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var cms = new(CountMinSketch)
	file.Seek(0, 0)
	for {
		err = decoder.Decode(cms)
		if err != nil {
			break
		}
	}
	return cms, nil
}
