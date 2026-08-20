package cache

import (
	"container/list"
	"sync"
	"time"
)

// Define how often a key is allowed to trigger a write-lock for LRU promotion
const promotionWindow int64 = 5

type item struct {
	key          string
	value        string
	expiresAt    int64
	lastPromoted int64
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

	now := time.Now().Unix()
	var expiration int64
	if ttl > 0 {
		expiration = now + int64(ttl)
	}

	// 1. If it already exists, update it and move it to the front (Most Recently Used)
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*item).value = value
		ent.Value.(*item).expiresAt = expiration
		ent.Value.(*item).lastPromoted = now
		return
	}

	// 2. Add new item to the front of the list
	entry := &item{key: key, value: value, expiresAt: expiration, lastPromoted: now}
	element := c.evictList.PushFront(entry)
	c.items[key] = element

	// 3. The LRU Magic: If we exceed capacity, delete the oldest item at the back
	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}
}

// Get fetches a value using coarse-grained LRU bumping
func (c *Cache) Get(key string) (string, bool) {
	now := time.Now().Unix()
	c.mu.RLock()
	ent, ok := c.items[key]

	if !ok {
		c.mu.RUnlock()
		return "", false
	}

	entry := ent.Value.(*item)

	// TTL Check (Passive Expiration)
	if entry.expiresAt > 0 && now > entry.expiresAt {
		c.mu.RUnlock()
		c.deleteExpired(key) // Clean up safely without holding the read lock
		return "", false
	}

	// The Coarse-Grained Check: If promoted recently, DO NOT write-lock!
	if now-entry.lastPromoted < promotionWindow {
		val := entry.value
		c.mu.RUnlock()
		return val, true
	}

	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-Check 1: Does it still exist? (Someone else might have deleted it)
	if ent, ok = c.items[key]; !ok {
		return "", false
	}

	entry = ent.Value.(*item)

	// Double-Check 2: Did someone else just promote it while we were waiting?
	if now-entry.lastPromoted >= promotionWindow {
		c.evictList.MoveToFront(ent)
		entry.lastPromoted = time.Now().Unix() // Reset the 5-second timer
	}

	return entry.value, true
}

// deleteExpired safely cleans up an expired key using a write lock
func (c *Cache) deleteExpired(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check it hasn't been overwritten before deleting
	if ent, ok := c.items[key]; ok {
		if entry := ent.Value.(*item); entry.expiresAt > 0 && time.Now().Unix() > entry.expiresAt {
			c.removeElement(ent)
		}
	}
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

// startSweeper runs a background cleanup loop for expired keys
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
