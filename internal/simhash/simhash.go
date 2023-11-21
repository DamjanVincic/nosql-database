package simhash

import (
	"github.com/bbalet/stopwords"
	"hash/fnv"
	"strings"
)

func GetFingerprint(text string) uint64 {
	// Remove stop words and trim spaces
	text = strings.TrimSpace(stopwords.CleanString(text, "en", false))

	var sum [64]int // Each element is a sum of the bits at corresponding position
	// Go through all words and calculate the sum of the hashes
	// We're considering that every word has a weight of 1, they will add up together as if we were to calculate their weight first
	for _, word := range strings.Split(text, " ") {
		h := fnv.New64()
		_, err := h.Write([]byte(word))
		if err != nil {
			panic(err)
		}

		// If a bit is 0 we decrement the sum, if it's 1 we increment the sum
		hash := h.Sum64()
		for i := uint8(0); i < 64; i++ {
			if (hash>>i)&1 == 1 { // if hash & (1 << i) == 1 { } doesn't work?
				sum[i]++
			} else {
				sum[i]--
			}
		}
	}

	// If a sum is positive we set the corresponding bit to 1, otherwise we leave it at 0
	var fingerprint uint64
	for i := uint8(0); i < 64; i++ {
		if sum[i] > 0 {
			fingerprint |= 1 << i
		}
	}

	return fingerprint
}

func GetHammingDistance(fingerprint1, fingerprint2 uint64) uint8 {
	// XOR the two fingerprints and count the number of 1s
	xor := fingerprint1 ^ fingerprint2
	var distance uint8
	for i := uint8(0); i < 64; i++ {
		if (xor>>i)&1 == 1 {
			distance++
		}
	}
	return distance
}
