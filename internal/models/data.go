package models

// Data struct that wraps bytes, tombstone and timestamp
// to be used in structures that require those fields
type Data struct {
	Value     []byte
	Tombstone bool
	Timestamp uint64
}
