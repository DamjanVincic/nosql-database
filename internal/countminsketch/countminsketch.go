package main

import "slices"

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
