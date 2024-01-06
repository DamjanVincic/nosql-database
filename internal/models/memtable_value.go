package models

type MemtableValue struct {
	Value     []byte
	Tombstone bool
	Timestamp uint64
}
