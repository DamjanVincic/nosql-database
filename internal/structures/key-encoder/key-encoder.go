package key_encoder

import (
	"encoding/binary"
	"errors"
)

type KeyEncoder struct {
	Keys map[string]uint64
}

func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{Keys: make(map[string]uint64)}
}

// returns encoded value of given key, if it doesn't already exists it is created
func (keyEncoder *KeyEncoder) GetEncoded(key string) uint64 {
	encoded, ok := keyEncoder.Keys[key] //if key already exists, just return its encoded value
	if ok {
		return encoded
	}
	encoded = uint64(len(keyEncoder.Keys) + 1)
	keyEncoder.Keys[key] = encoded
	return encoded
}

// returns key for given encoded value
func (keyEncoder *KeyEncoder) GetKey(encoded uint64) (key string, err error) {
	key = ""
	err = nil
	for k, v := range keyEncoder.Keys {
		if v == encoded {
			key = k
			return
		}
	}
	err = errors.New("couldn't find key with given encoded value in keyEncoder")
	return
}

func (keyEncoder *KeyEncoder) Serialize() []byte {
	bytes := make([]byte, 0)
	tempBytes := make([]byte, binary.MaxVarintLen64) //temp storage for 64-bit integers

	var tempBytesLength int
	var keyLength uint64

	for key, encoded := range keyEncoder.Keys {
		//put encoded key value into tempBytes and get number of bytes written
		tempBytesLength = binary.PutUvarint(tempBytes, encoded)
		//append encoded key value to bytes
		bytes = append(bytes, tempBytes[:tempBytesLength]...)

		keyLength = uint64(len([]byte(key)))
		//put keyLength into tempBytes and get number of bytes written
		tempBytesLength = binary.PutUvarint(tempBytes, keyLength)
		//append key length to bytes
		bytes = append(bytes, tempBytes[:tempBytesLength]...)
		//append key to bytes
		bytes = append(bytes, []byte(key)...)
	}
	return bytes
}

func Deserialize(serializedKeyEncoder []byte) (keyEncoder *KeyEncoder, err error) {
	keyEncoder = NewKeyEncoder()
	err = nil

	var tempEncoded uint64 //temp variable for storing encoded key value
	var tempKeySize uint64 //temp variable for storing keySize
	var tempKey string     //temp variable used for storing key
	var tempBytesRead int  //temp variable for number of bytes read for each 64-bit integer
	bytesRead := 0         //total number of bytes read

	for {
		tempEncoded, tempBytesRead = binary.Uvarint(serializedKeyEncoder[bytesRead:])
		if tempBytesRead == 0 {
			return
		}
		bytesRead += tempBytesRead
		tempKeySize, tempBytesRead = binary.Uvarint(serializedKeyEncoder[bytesRead:])
		if tempBytesRead == 0 {
			err = errors.New("missing value for key size")
			return
		}
		bytesRead += tempBytesRead
		tempKey = string(serializedKeyEncoder[bytesRead : bytesRead+int(tempKeySize)])
		bytesRead += int(tempKeySize)
		keyEncoder.Keys[tempKey] = tempEncoded
	}
}
