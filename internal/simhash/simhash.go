package simhash

import (
	"crypto/md5"
	"fmt"
	"github.com/bbalet/stopwords"
	"strings"
)

func getWeightsForTokens(tokens []string) map[string]int {
	weights := make(map[string]int)
	for _, token := range tokens {
		weights[token]++
	}
	return weights
}

func getHashAsString(data string) string {
	hash := md5.Sum([]byte(data))
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

func GetFingerprint(text string) string {
	text = stopwords.CleanString(text, "en", false)
	text = strings.TrimSpace(text)
	tokens := strings.Split(text, " ")
	weights := getWeightsForTokens(tokens)
	hashes := make(map[string]string)
	for _, token := range tokens {
		hashes[token] = getHashAsString(token)
	}
	var fingerprint string
	for i := 0; i < len(hashes[tokens[0]]); i++ {
		sum := 0
		for token, hash := range hashes {
			if hash[i] == '0' {
				sum += -1 * weights[token]
			} else {
				sum += weights[token]
			}
		}
		if sum > 0 {
			fingerprint += "1"
		} else {
			fingerprint += "0"
		}
	}
	return fingerprint
}

func GetHammingDistance(fingerprint1 string, fingerprint2 string) string {
	if len(fingerprint1) != len(fingerprint2) {
		panic("Fingerprints must be of the same length")
	}
	var distance string
	for i := 0; i < len(fingerprint1); i++ {
		if fingerprint1[i] != fingerprint2[i] {
			distance += "1"
		} else {
			distance += "0"
		}
	}
	return distance
}
