package main

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
