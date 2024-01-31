package key_encoder

import "errors"

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
	encoded = uint64(len(keyEncoder.Keys))
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
