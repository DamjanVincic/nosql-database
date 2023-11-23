package main

type CountMinSketch struct {
	M, K          uint
	Matrix        [][]uint
	HashFunctions []HashWithSeed
}
