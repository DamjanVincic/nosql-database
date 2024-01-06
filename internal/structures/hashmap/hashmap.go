package hashmap

import "github.com/DamjanVincic/key-value-engine/internal/models"

type HashMap struct {
	data map[string]*models.MemtableValue
}

func CreateHashMap() *HashMap {
	return &HashMap{data: make(map[string]*models.MemtableValue)}
}

// Get Error is returned because the interface requires it
func (hashMap *HashMap) Get(key string) (*interface{}, error) {
	var value interface{}
	value, ok := hashMap.data[key]
	if !ok {
		return nil, nil
	}
	return &value, nil
}

// Put Error is returned because the interface requires it
func (hashMap *HashMap) Put(key string, value []byte, tombstone bool, timestamp uint64) error {
	hashMap.data[key] = &models.MemtableValue{Value: value, Tombstone: tombstone, Timestamp: timestamp}
	return nil
}

// Delete Error is returned because the interface requires it
func (hashMap *HashMap) Delete(key string) error {
	delete(hashMap.data, key)
	return nil
}
