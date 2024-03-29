package hashmap

import (
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"sort"
)

type HashMap struct {
	data map[string]*models.Data
}

func CreateHashMap() *HashMap {
	return &HashMap{data: make(map[string]*models.Data)}
}

// Get Error is returned because the interface requires it
func (hashMap *HashMap) Get(key string) *models.Data {
	value, ok := hashMap.data[key]
	if !ok {
		return nil
	}
	return value
}

// Put Error is returned because the interface requires it
func (hashMap *HashMap) Put(key string, value []byte, tombstone bool, timestamp uint64) {
	hashMap.data[key] = &models.Data{Key: key, Value: value, Tombstone: tombstone, Timestamp: timestamp}
}

// Delete Error is returned because the interface requires it
func (hashMap *HashMap) Delete(key string) {
	delete(hashMap.data, key)
}

// GetSorted returns all values sorted
func (hashMap *HashMap) GetSorted() []*models.Data {
	keys := make([]string, 0)
	for key := range hashMap.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]*models.Data, 0)
	for _, key := range keys {
		entries = append(entries, hashMap.data[key])
	}

	return entries
}

func (hashMap *HashMap) Size() int {
	return len(hashMap.data)
}
