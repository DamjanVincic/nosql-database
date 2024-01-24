package cache

import (
	"container/list"
	"github.com/DamjanVincic/key-value-engine/internal/models"
)

const (
	capacity = 5
)

type Cache struct {
	cacheList *list.List
	cacheMap  map[string]*list.Element
}

func NewCache() *Cache {
	return &Cache{cacheList: list.New(), cacheMap: make(map[string]*list.Element)}
}

func (cache *Cache) Put(entry *models.MemEntry) {
	elem, ok := cache.cacheMap[entry.Key]
	if !ok {
		cache.cacheList.PushFront(entry)
		if cache.cacheList.Len() > capacity {
			elem = cache.cacheList.Back()
			delete(cache.cacheMap, elem.Value.(*models.MemEntry).Key)
			cache.cacheList.Remove(elem)
		}
	} else {
		cache.cacheList.MoveToFront(elem)
	}
}

func (cache *Cache) Get(key string) *models.MemEntry {
	elem, ok := cache.cacheMap[key]
	if !ok {
		return nil
	}
	cache.cacheList.MoveToFront(elem)
	value := elem.Value.(*models.MemEntry)
	return value
}
