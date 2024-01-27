package tokenbucket

import (
	"encoding/binary"
	"time"
)

const (
	refillPeriod = 60 //number of seconds between each token refill
	maxTokens    = 5  //max number of tokens in each refillPeriod
)

type TokenBucket struct {
	lastRefillTime uint64 //unix time of last token refill
	tokenCount     uint64 //number of remaining tokens
}

func NewTokenBucket() *TokenBucket {
	return &TokenBucket{lastRefillTime: uint64(time.Now().Unix()), tokenCount: maxTokens}
}

// refills tokens if needed, tries to remove one token, if possible returns true if not returns false
func (tokenBucket *TokenBucket) UseToken() bool {
	if uint64(time.Now().Unix())-tokenBucket.lastRefillTime > tokenBucket.lastRefillTime {
		tokenBucket.tokenCount = maxTokens
	}
	if tokenBucket.tokenCount == 0 {
		return false
	}
	tokenBucket.tokenCount--
	return true
}

func (tokenBucket *TokenBucket) Serialize() []byte {
	serializedTokenBucket := make([]byte, 0)
	// Temporary storage for 64-bit integers
	tempByte := make([]byte, 8)

	//convert tokenCount to byte array and append it to serializedTokenBucket
	binary.BigEndian.PutUint64(tempByte, tokenBucket.tokenCount)
	serializedTokenBucket = append(serializedTokenBucket, tempByte...)

	//convert lastRefillTime to byte array and append it to serializedTokenBucket
	binary.BigEndian.PutUint64(tempByte, tokenBucket.lastRefillTime)
	serializedTokenBucket = append(serializedTokenBucket, tempByte...)

	return serializedTokenBucket
}

func Deserialize(serializedTokenBucket []byte) *TokenBucket {
	//get tokenCount from first 8 bytes of serializedTokenBucket
	tokenCount := binary.BigEndian.Uint64(serializedTokenBucket[:8])
	//get lastRefillTime from second 8 bytes of serializedTokenBucket
	lastRefillTime := binary.BigEndian.Uint64(serializedTokenBucket[8:])
	return &TokenBucket{tokenCount: tokenCount, lastRefillTime: lastRefillTime}
}
