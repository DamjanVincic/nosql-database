package simhash

import (
	"crypto/md5"
	"fmt"
	"slices"
	"strings"
)

func isStoppingWord(token string) bool {
	stoppingWords := []string{"i", "a", "about", "an", "are", "as", "at", "be", "by", "com", "de", "en", "for", "from", "how", "in", "is", "it", "la", "of", "on", "or", "that", "this", "to", "was", "what", "when", "where", "who", "will", "with", "and", "the", "www"}
	return slices.Contains(stoppingWords, token)
}

func removeStoppingWords(tokens []string) []string {
	result := make([]string, 0)
	for _, token := range tokens {
		if !isStoppingWord(token) {
			result = append(result, token)
		}
	}
	return result
}

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
	tokens := strings.Split(text, " ")
	tokens = removeStoppingWords(tokens)
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
