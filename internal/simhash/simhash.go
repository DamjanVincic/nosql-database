package simhash

import (
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

func GetFingerprint(text string) string {
	tokens := strings.Split(text, " ")
	tokens = removeStoppingWords(tokens)
	weights := getWeightsForTokens(tokens)
	return ""
}
