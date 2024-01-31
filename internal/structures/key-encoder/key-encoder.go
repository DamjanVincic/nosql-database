package key_encoder

type KeyEncoder struct {
	Keys map[string]uint64
}

func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{Keys: make(map[string]uint64)}
}

// returns encoded value of given key, if it doesn't already exists it is created
func (keyEncoder *KeyEncoder) GetKey(key string) uint64 {
	encoded, ok := keyEncoder.Keys[key] //if key already exists, just return its encoded value
	if ok {
		return encoded
	}
	encoded = uint64(len(keyEncoder.Keys))
	keyEncoder.Keys[key] = encoded
	return encoded
}
