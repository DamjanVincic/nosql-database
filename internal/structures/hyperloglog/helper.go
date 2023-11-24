package main

import (
	"crypto/md5"
	"encoding/binary"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

func Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(data)
	return binary.BigEndian.Uint64(fn.Sum(nil))
}
