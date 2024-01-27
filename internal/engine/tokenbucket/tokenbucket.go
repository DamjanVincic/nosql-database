package tokenbucket

import (
	"encoding/binary"
	"time"
)

const (
	refillPeriod = 60 //number of seconds between each token refill
	maxTokens    = 3  //max number of tokens in each refillPeriod
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
	if uint64(time.Now().Unix())-tokenBucket.lastRefillTime > refillPeriod {
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

	tempByte := make([]byte, binary.MaxVarintLen64) // Temporary storage for 64-bit integers
	var tempBytesWritten int

	//convert tokenCount to byte array and append it to serializedTokenBucket
	tempBytesWritten = binary.PutUvarint(tempByte, tokenBucket.tokenCount)
	serializedTokenBucket = append(serializedTokenBucket, tempByte[:tempBytesWritten]...)

	//convert lastRefillTime to byte array and append it to serializedTokenBucket
	tempBytesWritten = binary.PutUvarint(tempByte, tokenBucket.lastRefillTime)
	serializedTokenBucket = append(serializedTokenBucket, tempByte[:tempBytesWritten]...)

	return serializedTokenBucket
}

func Deserialize(serializedTokenBucket []byte) *TokenBucket {
	//get tokenCount from first 8 bytes of serializedTokenBucket
	tokenCount, bytesRead := binary.Uvarint(serializedTokenBucket)
	//get lastRefillTime from second 8 bytes of serializedTokenBucket
	lastRefillTime, _ := binary.Uvarint(serializedTokenBucket[bytesRead:])
	return &TokenBucket{tokenCount: tokenCount, lastRefillTime: lastRefillTime}
}
