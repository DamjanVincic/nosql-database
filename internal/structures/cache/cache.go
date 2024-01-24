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

func (cache *Cache) Put(key string) {

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
