package cache

import (
	"container/list"
	"github.com/DamjanVincic/key-value-engine/internal/models"
)

const (
	capacity = 3
)

type Cache struct {
	cacheList *list.List               //doubly linked list containing cached MemEntries
	cacheMap  map[string]*list.Element //hashMap used for accessing elements in cacheList by key
}

func NewCache() *Cache {
	return &Cache{cacheList: list.New(), cacheMap: make(map[string]*list.Element)}
}

// adds MemEntry to cache and makes it most recently used
func (cache *Cache) Put(entry *models.Data) {
	elem, ok := cache.cacheMap[entry.Key] //find element in cacheList
	if !ok {                              //if entry isn't already in cache, it needs to be added
		if cache.cacheList.Len() >= capacity { //if cache is full, least recently used entry is deleted
			elem = cache.cacheList.Back()
			delete(cache.cacheMap, elem.Value.(*models.Data).Key)
			cache.cacheList.Remove(elem)
		}

		cache.cacheList.PushFront(entry)
		cache.cacheMap[entry.Key] = cache.cacheList.Front()

	} else {
		cache.cacheList.MoveToFront(elem) //make new entry most recently used
		elem.Value = entry                //update value
	}
}

// returns entry form cache and makes it most recently used
func (cache *Cache) Get(key string) *models.Data {
	elem, ok := cache.cacheMap[key] //find element in cacheList
	if !ok {
		return nil
	}
	cache.cacheList.MoveToFront(elem)
	value := elem.Value.(*models.Data)
	return value
}

func (cache *Cache) Delete(key string) {
	elem, ok := cache.cacheMap[key] //find element in cacheList
	if !ok {
		return
	}
	delete(cache.cacheMap, elem.Value.(*models.Data).Key)
	cache.cacheList.Remove(elem)
}

// update data that is flushed from memtable
func (cache *Cache) Update(data []*models.Data) {
	for _, record := range data {
		elem, ok := cache.cacheMap[record.Key]
		if ok {
			elem.Value = record
		}
	}
}
