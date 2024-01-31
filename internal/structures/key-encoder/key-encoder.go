package key_encoder

type KeyEncoder struct {
	Keys map[string]uint64
}

func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{Keys: make(map[string]uint64)}
}
