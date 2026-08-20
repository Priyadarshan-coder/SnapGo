package cache

import (
	"container/list"
	"sync"
	"time"
)

// item stores both key and value so we know what to delete from the map
// when the linked list tells us to evict the tail.
type item struct {
	key       string
	value     string
	expiresAt int64
}

type Cache struct {
	mu        sync.RWMutex
	capacity  int
	items     map[string]*list.Element // Maps strings to node pointers in the linked list
	evictList *list.List               // The doubly linked list managing the LRU order
}

// New initializes the LRU Cache with a maximum capacity
func New(capacity int, cleanupInterval time.Duration) *Cache {
	c := &Cache{
		capacity:  capacity,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}

	go c.startSweeper(cleanupInterval)
	return c
}

// Set adds a value to the front of the list, and evicts the tail if we are over capacity
func (c *Cache) Set(key string, value string, ttl int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Unix() + int64(ttl)
	}

	// 1. If it already exists, update it and move it to the front (Most Recently Used)
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*item).value = value
		ent.Value.(*item).expiresAt = expiration
		return
	}

	// 2. Add new item to the front of the list
	entry := &item{key: key, value: value, expiresAt: expiration}
	element := c.evictList.PushFront(entry)
	c.items[key] = element

	// 3. The LRU Magic: If we exceed capacity, delete the oldest item at the back
	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}
}

// Get fetches a value and moves it to the front of the list
func (c *Cache) Get(key string) (string, bool) {
	c.mu.Lock() // Requires a full lock because we are modifying the linked list order
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		entry := ent.Value.(*item)

		// TTL Check
		if entry.expiresAt > 0 && time.Now().Unix() > entry.expiresAt {
			c.removeElement(ent)
			return "", false
		}

		// LRU Magic: Because it was just accessed, move it to the front!
		c.evictList.MoveToFront(ent)
		return entry.value, true
	}

	return "", false
}

// removeOldest drops the least recently used item (the tail of the list)
func (c *Cache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is a helper to delete from both the list and the map
func (c *Cache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*item)
	delete(c.items, kv.key)
}

// startSweeper (remains mostly the same, just utilizing removeElement)
func (c *Cache) startSweeper(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now().Unix()
		for _, ent := range c.items {
			if entry := ent.Value.(*item); entry.expiresAt > 0 && now > entry.expiresAt {
				c.removeElement(ent)
			}
		}
		c.mu.Unlock()
	}
}
