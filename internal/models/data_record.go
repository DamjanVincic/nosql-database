package models

// DataRecord struct for SSTable that wraps Data, KeySize and ValueSize
type DataRecord struct {
	Data      Data
	KeySize   uint64
	ValueSize uint64
}
